package api

import (
	"context"
	"fmt"
	"net/http"
	"testing"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/internal/request"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
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
