package api

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
)

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

// Default (queued) refund path: worker applies the reversal, then repeats are inert.
func TestRefundQueuedThenRepeatDoesNotMoveBalances(t *testing.T) {
	router, b, err := setupRouter()
	require.NoError(t, err)

	cnf, err := config.Fetch()
	require.NoError(t, err)

	queueName := fmt.Sprintf("%s_%d", cnf.Queue.TransactionQueue, 1)
	cleanup := StartTestAsynqWorker(t, cnf, b, queueName)
	defer cleanup()

	src, dst := newDryRunFixture(t, b)

	txn, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference: model.GenerateUUIDWithSuffix("refqueue"), Source: src, Destination: dst,
		Amount: 1, Precision: 100, Currency: "USD", SkipQueue: true,
	})
	require.NoError(t, err)

	w := doJSON(router, http.MethodPost, "/refund-transaction/"+txn.TransactionID, map[string]interface{}{})
	require.Equal(t, http.StatusCreated, w.Code, "first queued refund must succeed: %s", w.Body.String())

	appliedRef := model.RefundReference(txn.TransactionID) + "_q"
	_, err = pollForTransactionStatus(context.Background(), b.GetDataSource(), appliedRef, "APPLIED", 200*time.Millisecond, 10*time.Second)
	require.NoError(t, err, "worker must apply the queued refund")

	srcAfterApply := balanceString(t, b, src)
	dstAfterApply := balanceString(t, b, dst)

	refund := func() int {
		return doJSON(router, http.MethodPost, "/refund-transaction/"+txn.TransactionID, map[string]interface{}{}).Code
	}

	for attempt := 2; attempt <= 3; attempt++ {
		assert.Equal(t, http.StatusConflict, refund(), "refund attempt %d should conflict", attempt)
		assert.Equal(t, srcAfterApply, balanceString(t, b, src),
			"refund attempt %d moved the source balance while reporting failure", attempt)
		assert.Equal(t, dstAfterApply, balanceString(t, b, dst),
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

	srcPre := balanceString(t, b, src)
	dstAPre := balanceString(t, b, dstA)
	dstBPre := balanceString(t, b, dstB)

	txn, err := b.QueueTransaction(t.Context(), &model.Transaction{
		Reference: model.GenerateUUIDWithSuffix("splitref"), Source: src,
		Amount: 2, Precision: 100, Currency: "USD", SkipQueue: true,
		Destinations: []model.Distribution{
			{Identifier: dstA, Distribution: "50%"},
			{Identifier: dstB, Distribution: "50%"},
		},
	})
	require.NoError(t, err)

	srcPostPay := balanceString(t, b, src)
	dstAPostPay := balanceString(t, b, dstA)
	dstBPostPay := balanceString(t, b, dstB)
	require.NotEqual(t, dstAPre, dstAPostPay, "leg A should have been credited")
	require.NotEqual(t, dstBPre, dstBPostPay, "leg B should have been credited")
	require.NotEqual(t, srcPre, srcPostPay, "source should have been debited")

	w := doJSON(router, http.MethodPost, "/refund-transaction/"+txn.TransactionID,
		map[string]interface{}{"skip_queue": true})
	require.Equal(t, http.StatusCreated, w.Code, "a fan-out refund must still work: %s", w.Body.String())

	assert.Equal(t, srcPre, balanceString(t, b, src), "source should be restored after refund")
	assert.Equal(t, dstAPre, balanceString(t, b, dstA), "leg A should be restored after refund")
	assert.Equal(t, dstBPre, balanceString(t, b, dstB), "leg B should be restored after refund")
}

func balanceString(t *testing.T, b interface {
	GetBalanceByID(ctx context.Context, id string, include []string, withQueued bool) (*model.Balance, error)
}, balanceID string) string {
	t.Helper()
	bal, err := b.GetBalanceByID(t.Context(), balanceID, nil, false)
	require.NoError(t, err)
	return bal.Balance.String()
}
