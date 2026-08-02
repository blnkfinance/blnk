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

package blnk

import (
	"context"
	"errors"
	"math/big"
	"testing"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInflightActionReference(t *testing.T) {
	assert.Equal(t, "txn_1_commit_act_9", inflightActionReference("txn_1", InflightActionCommit, "act_9"))
	assert.Equal(t, "txn_1_void_act_9", inflightActionReference("txn_1", InflightActionVoid, "act_9"))
	// An empty action ID yields no reference, so finalize* falls back to a random one.
	assert.Equal(t, "", inflightActionReference("txn_1", InflightActionCommit, ""))
}

func TestIsTerminalInflightError(t *testing.T) {
	l := &Blnk{}

	terminal := []error{
		errors.New("cannot commit. Transaction already committed"),
		errors.New("transaction has already been voided"),
		errors.New("transaction is not in inflight status"),
		errors.New("cannot commit more than the remaining amount"),
		errors.New(`duplicate key value violates unique constraint "idx_transactions_reference_unique"`),
	}
	for _, err := range terminal {
		assert.Truef(t, l.isTerminalInflightError(err), "expected terminal: %v", err)
	}

	transient := []error{
		errors.New("failed to acquire lock for inflight commit"),
		errors.New("transaction not found"),
		errors.New("connection reset by peer"),
		nil,
	}
	for _, err := range transient {
		assert.Falsef(t, l.isTerminalInflightError(err), "expected transient: %v", err)
	}
}

func TestValidateRequestedAmountErrorFormatting(t *testing.T) {
	l := &Blnk{}
	txn := &model.Transaction{
		Currency:      "USD",
		Amount:        100,
		PreciseAmount: big.NewInt(10000),
		Precision:     100,
	}

	err := l.validateRequestedAmount(txn, big.NewInt(7000), big.NewInt(6000))
	require.Error(t, err)
	assert.Equal(t, "cannot commit more than the remaining amount. Available: USD60.00, Requested: USD70.00", err.Error())
	assert.NotContains(t, err.Error(), "%!")

	err = l.validateRequestedAmount(txn, big.NewInt(11000), big.NewInt(10000))
	require.Error(t, err)
	assert.Equal(t, "cannot commit more than the original transaction amount. Original: USD100.00, Requested: USD110.00", err.Error())
	assert.NotContains(t, err.Error(), "%!")
}

func TestShouldExpandInflightParent(t *testing.T) {
	assert.True(t, shouldExpandInflightParent(errors.New("transaction not found")))
	assert.True(t, shouldExpandInflightParent(errors.New("transaction is not in inflight status")))
	assert.True(t, shouldExpandInflightParent(errors.New("NOT_FOUND: Transaction with ID 'txn_x' not found")))
	assert.False(t, shouldExpandInflightParent(errors.New("failed to acquire lock for inflight commit")))
	assert.False(t, shouldExpandInflightParent(nil))
}

func TestClearTerminalInflightMetadata(t *testing.T) {
	terminal := &model.Transaction{
		Status:   StatusApplied,
		Inflight: true,
		MetaData: map[string]interface{}{
			"inflight": true,
			"source":   "checkout",
		},
	}

	clearTerminalInflightMetadata(terminal)

	assert.False(t, terminal.Inflight)
	assert.NotContains(t, terminal.MetaData, "inflight")
	assert.Equal(t, "checkout", terminal.MetaData["source"])

	pending := &model.Transaction{Status: StatusInflight, Inflight: true, MetaData: map[string]interface{}{"inflight": true}}
	clearTerminalInflightMetadata(pending)

	assert.True(t, pending.Inflight)
	assert.Contains(t, pending.MetaData, "inflight")

	clearTerminalInflightMetadata(nil)
}

func TestBuildTransactionExecutionWorkClearsTerminalInflightMetadata(t *testing.T) {
	l := &Blnk{}
	source := &model.Balance{BalanceID: "source"}
	destination := &model.Balance{BalanceID: "destination"}

	for _, status := range []string{StatusCommit, StatusVoid} {
		t.Run(status, func(t *testing.T) {
			expectedStatus := StatusVoid
			if status == StatusCommit {
				expectedStatus = StatusApplied
			}

			transaction := &model.Transaction{
				Status:            status,
				PreciseAmount:     big.NewInt(100),
				ParentTransaction: "txn_parent",
				MetaData: map[string]interface{}{
					"inflight": true,
					"source":   "checkout",
				},
			}

			work, discarded := l.buildTransactionExecutionWork(context.Background(), transaction, source, destination)

			require.False(t, discarded)
			assert.Equal(t, expectedStatus, work.transaction.Status)
			assert.Nil(t, work.outbox)
			assert.False(t, work.transaction.Inflight)
			assert.NotContains(t, work.transaction.MetaData, "inflight")
			assert.Equal(t, "checkout", work.transaction.MetaData["source"])
		})
	}
}
