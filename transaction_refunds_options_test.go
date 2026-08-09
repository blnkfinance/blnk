package blnk

import (
	"testing"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func originalForRefund() *model.Transaction {
	return &model.Transaction{
		TransactionID: "txn_original",
		Reference:     "ref_original",
		Source:        "bln_source",
		Destination:   "bln_destination",
		Amount:        100,
		Precision:     100,
		Currency:      "USD",
		Description:   "customer deposit",
		Status:        StatusApplied,
		MetaData: map[string]interface{}{
			"type":     "deposit",
			"inflight": true,
			"atomic":   true,
		},
	}
}

// TestPrepareRefundInheritsWithoutOptions pins the historical behaviour: with
// no overrides the reversal still copies the original's description and
// metadata exactly.
func TestPrepareRefundInheritsWithoutOptions(t *testing.T) {
	original := originalForRefund()

	refund := prepareRefundTransaction(original, RefundOptions{})

	assert.Equal(t, "customer deposit", refund.Description)
	assert.Equal(t, "deposit", refund.MetaData["type"])
	assert.Equal(t, "bln_destination", refund.Source, "a refund reverses the movement")
	assert.Equal(t, "bln_source", refund.Destination)
}

// TestPrepareRefundAppliesDescription covers the reported gap: a reversal
// should be describable as its own movement.
func TestPrepareRefundAppliesDescription(t *testing.T) {
	original := originalForRefund()

	refund := prepareRefundTransaction(original, RefundOptions{Description: "refund for ticket #4821"})

	assert.Equal(t, "refund for ticket #4821", refund.Description)
	assert.Equal(t, "customer deposit", original.Description, "the original must be left alone")
}

// TestPrepareRefundMergesMetaData covers the other half: caller keys win, and
// keys the ledger sets on the original survive.
func TestPrepareRefundMergesMetaData(t *testing.T) {
	original := originalForRefund()

	refund := prepareRefundTransaction(original, RefundOptions{
		MetaData: map[string]interface{}{"type": "refund", "reason": "goodwill"},
	})

	assert.Equal(t, "refund", refund.MetaData["type"], "caller keys win on conflict")
	assert.Equal(t, "goodwill", refund.MetaData["reason"], "new keys are added")
	assert.Equal(t, true, refund.MetaData["inflight"], "ledger-set keys are preserved")
	assert.Equal(t, true, refund.MetaData["atomic"])
}

// TestPrepareRefundDoesNotMutateOriginalMetaData is the aliasing guard. The
// reversal is built with a struct copy, which shares the original's metadata
// map — writing into it directly would rewrite the transaction being refunded.
func TestPrepareRefundDoesNotMutateOriginalMetaData(t *testing.T) {
	original := originalForRefund()

	refund := prepareRefundTransaction(original, RefundOptions{
		MetaData: map[string]interface{}{"type": "refund"},
	})

	assert.Equal(t, "deposit", original.MetaData["type"], "the original's metadata must not be rewritten")
	assert.Equal(t, "refund", refund.MetaData["type"])

	// The two must not share a map at all.
	refund.MetaData["added_later"] = true
	_, leaked := original.MetaData["added_later"]
	assert.False(t, leaked, "the reversal must not share the original's metadata map")
}

// TestPrepareRefundCanOverrideInheritedInflight covers a practical use of
// merge semantics: setTransactionMetadata only ever sets the inflight marker,
// never clears it, so a reversal of an inflight transaction inherits it even
// though the reversal itself is not inflight. An override lets a caller
// correct that on their own row.
func TestPrepareRefundCanOverrideInheritedInflight(t *testing.T) {
	original := originalForRefund()
	require.Equal(t, true, original.MetaData["inflight"])

	refund := prepareRefundTransaction(original, RefundOptions{
		MetaData: map[string]interface{}{"inflight": false},
	})

	assert.Equal(t, false, refund.MetaData["inflight"])
}

// TestPrepareRefundEmptyOverridesAreIgnored checks the overrides are opt-in:
// an empty description or metadata map must not blank out what was inherited.
func TestPrepareRefundEmptyOverridesAreIgnored(t *testing.T) {
	original := originalForRefund()

	refund := prepareRefundTransaction(original, RefundOptions{
		Description: "",
		MetaData:    map[string]interface{}{},
	})

	assert.Equal(t, "customer deposit", refund.Description)
	assert.Equal(t, "deposit", refund.MetaData["type"])
}

// TestPrepareRefundKeepsSkipQueueBehaviour guards the existing option while
// the struct is introduced around it.
func TestPrepareRefundKeepsSkipQueueBehaviour(t *testing.T) {
	original := originalForRefund()

	assert.True(t, prepareRefundTransaction(original, RefundOptions{SkipQueue: true}).SkipQueue)
	assert.False(t, prepareRefundTransaction(original, RefundOptions{SkipQueue: false}).SkipQueue)
}

// TestPrepareRefundVoidOriginalStaysInflight guards the existing status
// handling: reversing a voided transaction must still process as an inflight
// reversal, whatever the caller sets on metadata.
func TestPrepareRefundVoidOriginalStaysInflight(t *testing.T) {
	original := originalForRefund()
	original.Status = StatusVoid

	refund := prepareRefundTransaction(original, RefundOptions{
		MetaData: map[string]interface{}{"type": "refund"},
	})

	assert.True(t, refund.Inflight)
	assert.Empty(t, refund.Status, "status is reset so the queue can assign it")
}
