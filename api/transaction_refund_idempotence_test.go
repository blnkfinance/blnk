package api

import (
	"net/http"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/model"
)

// balancePair reads both balances as plain strings for comparison.

// A repeated refund must not move money.
//
// The refund of X is itself a child of X, so a second refund of X used to
// select both X and the refund of X. The refund had not itself been refunded,
// so it passed validation and was reversed -- re-applying the original payment
// -- while X failed as already refunded. The aggregated error surfaced as 409
// after the money had already moved, which reads to a caller like a safe
// idempotency rejection.
func TestRefundTwiceDoesNotReverseTheRefund(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)
	src, dst := newDryRunFixture(t, b)

	txn, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference: model.GenerateUUIDWithSuffix("refidem"), Source: src, Destination: dst,
		Amount: 1, Precision: 100, Currency: "USD", SkipQueue: true,
	})
	require.NoError(t, err)

	refund := func() int {
		w := doJSON(router, http.MethodPost, "/refund-transaction/"+txn.TransactionID,
			map[string]interface{}{"skip_queue": true})
		return w.Code
	}

	require.Equal(t, http.StatusCreated, refund(), "the first refund must succeed")

	srcAfterFirst, err := b.GetBalanceByID(t.Context(), src, nil, false)
	require.NoError(t, err)
	dstAfterFirst, err := b.GetBalanceByID(t.Context(), dst, nil, false)
	require.NoError(t, err)

	// Every further attempt must be inert, not just the second.
	for attempt := 2; attempt <= 3; attempt++ {
		assert.Equal(t, http.StatusConflict, refund(), "refund attempt %d should conflict", attempt)

		srcNow, err := b.GetBalanceByID(t.Context(), src, nil, false)
		require.NoError(t, err)
		dstNow, err := b.GetBalanceByID(t.Context(), dst, nil, false)
		require.NoError(t, err)

		assert.Equal(t, srcAfterFirst.Balance.String(), srcNow.Balance.String(),
			"refund attempt %d moved the source balance while reporting failure", attempt)
		assert.Equal(t, dstAfterFirst.Balance.String(), dstNow.Balance.String(),
			"refund attempt %d moved the destination balance while reporting failure", attempt)
	}
}

// Split legs are children of the parent too, so excluding refunds by parentage
// would have made them unrefundable. Excluding by the refund reference must
// leave a fan-out refund working.
func TestRefundStillRefundsSplitLegs(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)
	src, dstA := newDryRunFixture(t, b)
	_, dstB := newDryRunFixture(t, b)

	txn, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference: model.GenerateUUIDWithSuffix("splitref"), Source: src,
		Amount: 2, Precision: 100, Currency: "USD", SkipQueue: true,
		Destinations: []model.Distribution{
			{Identifier: dstA, Distribution: "50%"},
			{Identifier: dstB, Distribution: "50%"},
		},
	})
	require.NoError(t, err)

	beforeA, err := b.GetBalanceByID(t.Context(), dstA, nil, false)
	require.NoError(t, err)
	beforeB, err := b.GetBalanceByID(t.Context(), dstB, nil, false)
	require.NoError(t, err)
	require.NotEqual(t, "0", beforeA.Balance.String(), "leg A should have been credited")
	require.NotEqual(t, "0", beforeB.Balance.String(), "leg B should have been credited")

	w := doJSON(router, http.MethodPost, "/refund-transaction/"+txn.TransactionID,
		map[string]interface{}{"skip_queue": true})
	require.Equal(t, http.StatusCreated, w.Code, "a fan-out refund must still work: %s", w.Body.String())

	afterA, err := b.GetBalanceByID(t.Context(), dstA, nil, false)
	require.NoError(t, err)
	afterB, err := b.GetBalanceByID(t.Context(), dstB, nil, false)
	require.NoError(t, err)

	assert.NotEqual(t, beforeA.Balance.String(), afterA.Balance.String(), "leg A should have been refunded")
	assert.NotEqual(t, beforeB.Balance.String(), afterB.Balance.String(), "leg B should have been refunded")
}
