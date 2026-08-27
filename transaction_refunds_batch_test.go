package blnk

import (
	"errors"
	"fmt"
	"testing"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestReconcileRefundBatchResults(t *testing.T) {
	applied := []*model.Transaction{{TransactionID: "txn_refund_b"}}

	t.Run("no error", func(t *testing.T) {
		got, err := reconcileRefundBatchResults(applied, nil)
		require.NoError(t, err)
		assert.Equal(t, applied, got)
	})

	t.Run("applied with skippable already-refunded error", func(t *testing.T) {
		batchErr := &errTransactionAlreadyRefunded{transactionID: "txn_leg_a"}
		got, err := reconcileRefundBatchResults(applied, batchErr)
		require.NoError(t, err)
		assert.Equal(t, applied, got)
	})

	t.Run("applied with joined already-refunded errors", func(t *testing.T) {
		batchErr := errors.Join(
			&errTransactionAlreadyRefunded{transactionID: "txn_leg_a"},
			&errTransactionAlreadyRefunded{transactionID: "txn_leg_c"},
		)
		got, err := reconcileRefundBatchResults(applied, batchErr)
		require.NoError(t, err)
		assert.Equal(t, applied, got)
	})

	t.Run("nothing applied all already refunded", func(t *testing.T) {
		batchErr := errors.Join(
			&errTransactionAlreadyRefunded{transactionID: "txn_leg_a"},
			&errTransactionAlreadyRefunded{transactionID: "txn_leg_b"},
		)
		got, err := reconcileRefundBatchResults(nil, batchErr)
		require.Error(t, err)
		assert.Nil(t, got)
		assert.True(t, allRefundBatchErrorsAreAlreadyRefunded(err))
	})

	t.Run("applied with fatal error", func(t *testing.T) {
		fatal := fmt.Errorf("failed to queue refund transaction")
		got, err := reconcileRefundBatchResults(applied, fatal)
		require.Error(t, err)
		assert.Equal(t, applied, got)
		assert.Equal(t, fatal, err)
	})
}
