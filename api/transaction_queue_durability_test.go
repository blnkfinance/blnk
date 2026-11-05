/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package api

import (
	"encoding/json"
	"net/http"
	"testing"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func queuedTransferBody(reference, source, destination string) map[string]interface{} {
	return map[string]interface{}{
		"amount":      1,
		"precision":   100,
		"currency":    "USD",
		"reference":   reference,
		"description": "queued durability",
		"source":      source,
		"destination": destination,
	}
}

// A 201 QUEUED answer must have a saved row behind it.
func TestQueuedTransactionIsSavedBeforeAnswer(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)
	src, dst := newDryRunFixture(t, b)

	reference := model.GenerateUUIDWithSuffix("durable")
	w := doJSON(router, http.MethodPost, "/transactions", queuedTransferBody(reference, src, dst))
	require.Equal(t, http.StatusCreated, w.Code, w.Body.String())

	var answered model.Transaction
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &answered))
	assert.Equal(t, "QUEUED", answered.Status)

	saved, err := b.GetDataSource().GetTransactionByRef(t.Context(), reference)
	require.NoError(t, err, "the transaction must be saved by the time the API answers")
	assert.Equal(t, answered.TransactionID, saved.TransactionID)
	assert.Equal(t, "QUEUED", saved.Status)
}

func TestQueuedTransactionDuplicateReferenceConflicts(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)
	src, dst := newDryRunFixture(t, b)

	reference := model.GenerateUUIDWithSuffix("dupqueued")
	first := doJSON(router, http.MethodPost, "/transactions", queuedTransferBody(reference, src, dst))
	require.Equal(t, http.StatusCreated, first.Code, first.Body.String())

	second := doJSON(router, http.MethodPost, "/transactions", queuedTransferBody(reference, src, dst))
	assertErrorCode(t, second, http.StatusConflict, apierror.ErrTxnDuplicateReference)
}

func TestQueuedTransactionWithUnknownBalanceIsNotAccepted(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)
	_, dst := newDryRunFixture(t, b)

	reference := model.GenerateUUIDWithSuffix("missingbal")
	w := doJSON(router, http.MethodPost, "/transactions", queuedTransferBody(reference, model.GenerateUUIDWithSuffix("bln"), dst))
	assertErrorCode(t, w, http.StatusNotFound, apierror.ErrBalNotFound)

	_, err = b.GetDataSource().GetTransactionByRef(t.Context(), reference)
	assert.Error(t, err, "a transaction that was not accepted must not be saved")
}
