package api

import (
	"net/http"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	model2 "github.com/jerry-enebeli/blnk/api/model"
	"github.com/jerry-enebeli/blnk/internal/request"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/stretchr/testify/assert"
)

func TestRecordCreditTransaction(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}

	newBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Error(err)
		return
	}
	validPayload := model2.RecordTransaction{
		Amount:        10000,
		Reference:     gofakeit.UUID(),
		Drcr:          "Credit",
		PaymentMethod: "Book",
		Description:   "test",
		Currency:      "NGN",
		BalanceId:     newBalance.BalanceID,
	}
	payloadBytes, _ := request.ToJsonReq(&validPayload)
	var response model.Transaction
	testRequest := TestRequest{
		Payload:  payloadBytes,
		Response: &response,
		Method:   "POST",
		Route:    "/transactions",
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, http.StatusCreated, resp.Code)
	assert.Equal(t, response.LedgerID, newLedger.LedgerID)
	assert.Equal(t, response.Currency, validPayload.Currency)
	assert.NotEmpty(t, response.BalanceID)
	assert.Equal(t, response.BalanceID, newBalance.BalanceID)
	assert.Equal(t, response.Status, "QUEUED")
}

func TestRecordDebitTransaction(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}

	newBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Error(err)
		return
	}
	validPayload := model2.RecordTransaction{
		Amount:        10000,
		Reference:     gofakeit.UUID(),
		Drcr:          "Debit",
		PaymentMethod: "Book",
		Description:   "test",
		Currency:      "NGN",
		BalanceId:     newBalance.BalanceID,
	}
	payloadBytes, _ := request.ToJsonReq(&validPayload)
	var response model.Transaction
	testRequest := TestRequest{
		Payload:  payloadBytes,
		Response: &response,
		Method:   "POST",
		Route:    "/transactions",
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, http.StatusCreated, resp.Code)
	assert.Equal(t, response.LedgerID, newLedger.LedgerID)
	assert.Equal(t, response.Currency, validPayload.Currency)
	assert.NotEmpty(t, response.BalanceID)
	assert.Equal(t, response.BalanceID, newBalance.BalanceID)
	assert.Equal(t, response.Status, "QUEUED")
}
