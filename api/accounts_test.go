package api

import (
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateAccount(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	tests := []struct {
		name         string
		payload      model2.CreateAccount
		expectedCode int
		wantErr      bool
	}{
		{
			name: "Missing required fields",
			payload: model2.CreateAccount{
				BankName: "Test Bank",
				Number:   gofakeit.AchAccount(),
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Both BalanceId and LedgerId provided",
			payload: model2.CreateAccount{
				BankName:  "Test Bank",
				Number:    gofakeit.AchAccount(),
				BalanceId: "bln_123",
				LedgerId:  "ldg_123",
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBytes, _ := request.ToJsonReq(&tt.payload)
			var response map[string]interface{}
			testRequest := TestRequest{
				Payload:  payloadBytes,
				Response: &response,
				Method:   "POST",
				Route:    "/accounts",
				Auth:     "",
				Router:   router,
			}

			resp, _ := SetUpTestRequest(testRequest)
			assert.Equal(t, tt.expectedCode, resp.Code)
		})
	}
}

func TestGetAccount(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	newBalance, err := b.CreateBalance(context.Background(), model.Balance{
		LedgerID: newLedger.LedgerID,
		Currency: gofakeit.CurrencyShort(),
	})
	if err != nil {
		t.Fatalf("Failed to create balance: %v", err)
	}

	newIdentity, err := b.CreateIdentity(model.Identity{
		FirstName:    gofakeit.FirstName(),
		LastName:     gofakeit.LastName(),
		EmailAddress: gofakeit.Email(),
		Category:     "individual",
	})
	if err != nil {
		t.Fatalf("Failed to create identity: %v", err)
	}

	newAccount, err := b.CreateAccount(model.Account{
		BankName:   "Test Bank",
		Number:     gofakeit.AchAccount(),
		LedgerID:   newLedger.LedgerID,
		BalanceID:  newBalance.BalanceID,
		IdentityID: newIdentity.IdentityID,
	})
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}

	var response model.Account
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    fmt.Sprintf("/accounts/%s", newAccount.AccountID),
		Auth:     "",
		Router:   router,
	}

	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, newAccount.AccountID, response.AccountID)
}

func TestGetAccountWithIncludes(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	ctx := context.Background()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)
	newBalance, err := b.CreateBalance(ctx, model.Balance{LedgerID: newLedger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	orgName := "Acme " + gofakeit.UUID()
	identity, err := b.CreateIdentity(model.Identity{
		IdentityType:     "organization",
		OrganizationName: orgName,
		Category:         "business",
		EmailAddress:     gofakeit.Email(),
	})
	require.NoError(t, err)

	newAccount, err := b.CreateAccount(model.Account{
		BankName:   "Test Bank",
		Number:     gofakeit.AchAccount(),
		LedgerID:   newLedger.LedgerID,
		BalanceID:  newBalance.BalanceID,
		IdentityID: identity.IdentityID,
	})
	require.NoError(t, err)

	t.Run("include=balance scans balance amounts and account timestamp", func(t *testing.T) {
		var response model.Account
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/accounts/%s?include=balance", newAccount.AccountID),
			Router:   router,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.Code)
		require.NotNil(t, response.Balance, "balance must be populated")
		assert.Equal(t, newBalance.BalanceID, response.Balance.BalanceID)
		require.NotNil(t, response.Balance.Balance, "balance amount must scan into big.Int")
		assert.Equal(t, "0", response.Balance.Balance.String())
		assert.False(t, response.CreatedAt.IsZero(), "account created_at must be scanned (regression: was written to balance)")
	})

	t.Run("include=identity returns the joined identity", func(t *testing.T) {
		var response model.Account
		resp, err := SetUpTestRequest(TestRequest{
			Response: &response,
			Method:   "GET",
			Route:    fmt.Sprintf("/accounts/%s?include=identity", newAccount.AccountID),
			Router:   router,
		})
		require.NoError(t, err)
		assert.Equal(t, http.StatusOK, resp.Code)
		require.NotNil(t, response.Identity)
		assert.Equal(t, identity.IdentityID, response.Identity.IdentityID)
		assert.Equal(t, orgName, response.Identity.OrganizationName)
	})
}

func TestGetAllAccounts(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	var response []model.Account
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    "/accounts",
		Auth:     "",
		Router:   router,
	}

	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusOK, resp.Code)
}

func TestGenerateMockAccount(t *testing.T) {
	router, _, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	var response map[string]string
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    "/mocked-account",
		Auth:     "",
		Router:   router,
	}

	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, "Blnk Bank", response["bank_name"])
	assert.NotEmpty(t, response["account_number"])
}

func TestFilterAccounts(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	newBalance := createTestBalanceWithLedger(t, b, gofakeit.CurrencyShort(), nil)
	newIdentity := createTestIdentity(t, b)

	// UUID as bank name guarantees a unique filter match
	uniqueBankName := gofakeit.UUID()
	newAccount, err := b.CreateAccount(model.Account{
		BankName:   uniqueBankName,
		Number:     gofakeit.AchAccount(),
		LedgerID:   newBalance.LedgerID,
		BalanceID:  newBalance.BalanceID,
		IdentityID: newIdentity.IdentityID,
	})
	if err != nil {
		t.Fatalf("Failed to create account: %v", err)
	}

	t.Run("Filter by bank_name eq", func(t *testing.T) {
		body := fmt.Sprintf(`{"filters": [{"field": "bank_name", "operator": "eq", "value": "%s"}]}`, uniqueBankName)
		var response []model.Account
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/accounts/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		if assert.Equal(t, 1, len(response)) {
			assert.Equal(t, newAccount.AccountID, response[0].AccountID)
		}
	})

	t.Run("Include count", func(t *testing.T) {
		body := fmt.Sprintf(`{"filters": [{"field": "bank_name", "operator": "eq", "value": "%s"}], "include_count": true}`, uniqueBankName)
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/accounts/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusOK, resp.Code)
		assert.Contains(t, response, "data")
		assert.Equal(t, float64(1), response["total_count"])
	})

	t.Run("Malformed JSON body", func(t *testing.T) {
		req := httptest.NewRequest("POST", "/accounts/filter", bytes.NewReader([]byte("{bad")))
		req.Header.Set("Content-Type", "application/json")
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})

	t.Run("Invalid filter operator", func(t *testing.T) {
		body := `{"filters": [{"field": "bank_name", "operator": "badop", "value": "x"}]}`
		var response map[string]interface{}
		resp, err := SetUpTestRequest(TestRequest{
			Payload:  bytes.NewReader([]byte(body)),
			Response: &response,
			Method:   "POST",
			Route:    "/accounts/filter",
			Router:   router,
		})
		if err != nil {
			t.Fatal(err)
		}
		assert.Equal(t, http.StatusBadRequest, resp.Code)
	})
}
