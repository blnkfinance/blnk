package blnk

import (
	"errors"
	"fmt"
	"strings"
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

	t.Run("applied with mixed already-refunded and fatal errors", func(t *testing.T) {
		batchErr := errors.Join(
			&errTransactionAlreadyRefunded{transactionID: "txn_leg_a"},
			fmt.Errorf("failed to queue refund transaction"),
		)
		got, err := reconcileRefundBatchResults(applied, batchErr)
		require.Error(t, err)
		assert.Equal(t, applied, got)
		assert.False(t, isTransactionAlreadyRefundedError(err))
		assert.NotContains(t, err.Error(), "has already been refunded")
		assert.Contains(t, err.Error(), "failed to queue refund transaction")
	})

	t.Run("mixed errors without apply keeps skippable-only conflict", func(t *testing.T) {
		batchErr := errors.Join(
			&errTransactionAlreadyRefunded{transactionID: "txn_leg_a"},
			fmt.Errorf("failed to queue refund transaction"),
		)
		got, err := reconcileRefundBatchResults(nil, batchErr)
		require.Error(t, err)
		assert.Nil(t, got)
		assert.Contains(t, err.Error(), "failed to queue refund transaction")
		assert.NotContains(t, err.Error(), "has already been refunded")
	})
}

func TestNonSkippableRefundBatchErrors(t *testing.T) {
	t.Run("joined mixed strips skippable phrase", func(t *testing.T) {
		err := nonSkippableRefundBatchErrors(errors.Join(
			&errTransactionAlreadyRefunded{transactionID: "txn_leg_a"},
			fmt.Errorf("failed to queue refund transaction"),
		))
		require.Error(t, err)
		assert.False(t, strings.Contains(err.Error(), "has already been refunded"))
	})
}
