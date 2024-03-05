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

func TestRecordTransaction(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}

	newSourceBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Error(err)
		return
	}

	newDestinationBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Error(err)
		return
	}
	validPayload := model2.RecordTransaction{
		Amount:      10000,
		Reference:   gofakeit.UUID(),
		Description: "test",
		Currency:    "NGN",
		Destination: newDestinationBalance.BalanceID,
		Source:      newSourceBalance.BalanceID,
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
	assert.Equal(t, response.Currency, validPayload.Currency)
	assert.Equal(t, response.Status, "QUEUED")
}

func TestRecordTransactionWithExitingRef(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}

	newSourceBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Error(err)
		return
	}

	newDestinationBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Error(err)
		return
	}
	validPayload := model2.RecordTransaction{
		Amount:      10000,
		Reference:   gofakeit.UUID(),
		Description: "test",
		Currency:    "NGN",
		Destination: newDestinationBalance.BalanceID,
		Source:      newSourceBalance.BalanceID,
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
	assert.Equal(t, response.Currency, validPayload.Currency)
	assert.Equal(t, response.Status, "QUEUED")
}
