/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0
*/
package api

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"

	model2 "github.com/blnkfinance/blnk/api/model"
)

// newBulkTestRouter builds a minimal gin engine wired to the bulk inflight
// handlers, without going through setupRouter() (which requires Postgres +
// Redis). The handlers themselves bail at request validation for the cases
// covered here, so no real service backing is needed.
func newBulkTestRouter() *gin.Engine {
	gin.SetMode(gin.TestMode)
	r := gin.New()
	a := Api{} // blnk service intentionally nil; validation paths must not touch it.
	r.POST("/transactions/bulk", a.CreateBulkTransactions)
	r.POST("/transactions/inflight/bulk/void", a.BulkVoidInflight)
	r.POST("/transactions/inflight/bulk/commit", a.BulkCommitInflight)
	return r
}

func doJSON(r *gin.Engine, method, path string, body interface{}) *httptest.ResponseRecorder {
	var buf bytes.Buffer
	if body != nil {
		_ = json.NewEncoder(&buf).Encode(body)
	}
	req := httptest.NewRequest(method, path, &buf)
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestBulkVoidInflight_RejectsEmptyList(t *testing.T) {
	r := newBulkTestRouter()
	w := doJSON(r, "POST", "/transactions/inflight/bulk/void",
		model2.BulkInflightVoidRequest{TransactionIDs: []string{}})
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "cannot be empty")
}

func TestBulkVoidInflight_RejectsOversizedList(t *testing.T) {
	r := newBulkTestRouter()
	ids := make([]string, model2.MaxBulkInflightItems+1)
	for i := range ids {
		ids[i] = fmt.Sprintf("txn_%d", i)
	}
	w := doJSON(r, "POST", "/transactions/inflight/bulk/void",
		model2.BulkInflightVoidRequest{TransactionIDs: ids})
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "too many transaction_ids")
}

func TestBulkVoidInflight_RejectsMalformedJSON(t *testing.T) {
	r := newBulkTestRouter()
	req := httptest.NewRequest("POST", "/transactions/inflight/bulk/void",
		strings.NewReader(`{"transaction_ids": "not-an-array"}`))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	assert.Equal(t, http.StatusBadRequest, w.Code)
}

func TestBulkCommitInflight_RejectsEmptyList(t *testing.T) {
	r := newBulkTestRouter()
	w := doJSON(r, "POST", "/transactions/inflight/bulk/commit",
		model2.BulkInflightCommitRequest{Transactions: nil})
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "cannot be empty")
}

func TestBulkCommitInflight_RejectsOversizedList(t *testing.T) {
	r := newBulkTestRouter()
	items := make([]model2.BulkInflightCommitItem, model2.MaxBulkInflightItems+1)
	for i := range items {
		items[i] = model2.BulkInflightCommitItem{TransactionID: fmt.Sprintf("txn_%d", i)}
	}
	w := doJSON(r, "POST", "/transactions/inflight/bulk/commit",
		model2.BulkInflightCommitRequest{Transactions: items})
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "too many transactions")
}

func TestCreateBulkTransactions_RejectsEmptyList(t *testing.T) {
	r := newBulkTestRouter()
	w := doJSON(r, "POST", "/transactions/bulk",
		model2.BulkTransactionRequest{Transactions: []*model2.RecordTransaction{}})
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "transactions cannot be empty")
	assert.Contains(t, w.Body.String(), "TXN_BULK_EMPTY")
}

func TestCreateBulkTransactions_RejectsOversizedList(t *testing.T) {
	r := newBulkTestRouter()
	txns := make([]*model2.RecordTransaction, model2.MaxBulkTransactionItems+1)
	for i := range txns {
		txns[i] = &model2.RecordTransaction{Amount: 1, Reference: fmt.Sprintf("r%d", i)}
	}
	w := doJSON(r, "POST", "/transactions/bulk",
		model2.BulkTransactionRequest{Transactions: txns})
	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "too many transactions")
	assert.Contains(t, w.Body.String(), "TXN_BULK_LIMIT_EXCEEDED")
}

func TestCreateBulkTransactions_ValidatesRecordTransactionItems(t *testing.T) {
	r := newBulkTestRouter()
	w := doJSON(r, "POST", "/transactions/bulk", model2.BulkTransactionRequest{
		Transactions: []*model2.RecordTransaction{
			{
				Amount:      100,
				Precision:   10.5,
				Reference:   "bulk_precision_invalid",
				Description: "invalid fractional precision",
				Currency:    "USD",
				Source:      "source_balance",
				Destination: "destination_balance",
			},
		},
	})

	assert.Equal(t, http.StatusBadRequest, w.Code)
	assert.Contains(t, w.Body.String(), "transactions[0]")
	assert.Contains(t, w.Body.String(), "precision must be an integer value")
}
