package api

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	model2 "github.com/jerry-enebeli/blnk/api/model"
	"github.com/jerry-enebeli/blnk/internal/request"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/stretchr/testify/assert"
)

func TestCreateBalance(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}
	validPayload := model2.CreateBalance{
		LedgerId: newLedger.LedgerID,
		Currency: gofakeit.Currency().Short,
	}
	payloadBytes, _ := request.ToJsonReq(&validPayload)
	var response model.Balance
	testRequest := TestRequest{
		Payload:  payloadBytes,
		Response: &response,
		Method:   "POST",
		Route:    "/balances",
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
}

func TestGetBalance(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}
	newBalance, err := b.CreateBalance(model.Balance{LedgerID: newLedger.LedgerID, Currency: gofakeit.CurrencyShort()})
	if err != nil {
		return
	}
	var response model.Balance
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    fmt.Sprintf("/balances/%s", newBalance.BalanceID),
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(response)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, response.BalanceID, newBalance.BalanceID)
	assert.Equal(t, response.LedgerID, newLedger.LedgerID)
	assert.Equal(t, response.Currency, newBalance.Currency)
}
