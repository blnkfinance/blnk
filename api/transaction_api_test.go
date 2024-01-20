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
		Route:    "/transaction",
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	updatedBalance, err := b.GetBalanceByID(newBalance.BalanceID, nil)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusCreated, resp.Code)
	assert.Equal(t, response.LedgerID, newLedger.LedgerID)
	assert.Equal(t, response.Currency, validPayload.Currency)
	assert.NotEmpty(t, response.BalanceID)
	assert.Equal(t, response.BalanceID, newBalance.BalanceID)
	assert.Equal(t, updatedBalance.Balance, int64(10000))
	assert.Equal(t, updatedBalance.CreditBalance, int64(10000))
	assert.Equal(t, response.Status, "APPLIED")
	assert.Equal(t, updatedBalance.DebitBalance, int64(0))
}

func TestRecordDebitTransaction(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}

	newBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
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
		Route:    "/transaction",
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	updatedBalance, err := b.GetBalanceByID(newBalance.BalanceID, nil)
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusCreated, resp.Code)
	assert.Equal(t, response.LedgerID, newLedger.LedgerID)
	assert.Equal(t, response.Currency, validPayload.Currency)
	assert.NotEmpty(t, response.BalanceID)
	assert.Equal(t, response.BalanceID, newBalance.BalanceID)
	assert.Equal(t, response.Status, "APPLIED")
	assert.Equal(t, updatedBalance.Balance, int64(-10000))
	assert.Equal(t, updatedBalance.CreditBalance, int64(0))
	assert.Equal(t, updatedBalance.DebitBalance, int64(10000))
}

func TestQueueDebitTransaction(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}

	newBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
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
		Route:    "/transaction-queue",
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
