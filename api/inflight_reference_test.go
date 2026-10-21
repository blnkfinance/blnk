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
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type inflightReferenceFixture struct {
	router *gin.Engine
	b      *blnk.Blnk
	src    string
	dst    string
	holdID string
}

// newInflightReferenceFixture creates a 100.00 hold (precise 10000) from src to dst.
func newInflightReferenceFixture(t *testing.T) inflightReferenceFixture {
	t.Helper()
	router, b, err := setupRouter()
	require.NoError(t, err)

	ctx := context.Background()
	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err)
	src, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)
	dst, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	require.NoError(t, err)

	holdID := createInflightForBulk(t, router, src.BalanceID, dst.BalanceID, "USD", 100)
	return inflightReferenceFixture{router: router, b: b, src: src.BalanceID, dst: dst.BalanceID, holdID: holdID}
}

func (f inflightReferenceFixture) update(body map[string]interface{}) *httptest.ResponseRecorder {
	return doJSON(f.router, "PUT", "/transactions/inflight/"+f.holdID, body)
}

func (f inflightReferenceFixture) assertBalances(t *testing.T, wantSrc, wantDst, msg string) {
	t.Helper()
	ctx := context.Background()
	s, err := f.b.GetBalanceByID(ctx, f.src, nil, false)
	require.NoError(t, err)
	d, err := f.b.GetBalanceByID(ctx, f.dst, nil, false)
	require.NoError(t, err)
	assert.Equal(t, wantSrc, s.Balance.String(), "source "+msg)
	assert.Equal(t, wantDst, d.Balance.String(), "destination "+msg)
}

func commitBody(amount int64, reference string, skipQueue bool) map[string]interface{} {
	return map[string]interface{}{
		"status":         "commit",
		"precise_amount": amount,
		"reference":      reference,
		"skip_queue":     skipQueue,
	}
}

func decodeResponse(t *testing.T, rec *httptest.ResponseRecorder) map[string]interface{} {
	t.Helper()
	var resp map[string]interface{}
	require.NoError(t, json.Unmarshal(rec.Body.Bytes(), &resp), "body: %s", rec.Body.String())
	return resp
}

func errorCode(resp map[string]interface{}) interface{} {
	detail, _ := resp["error_detail"].(map[string]interface{})
	return detail["code"]
}

func TestInflightCommitReference_RepeatIsReplayed(t *testing.T) {
	f := newInflightReferenceFixture(t)
	ref := "capture_" + gofakeit.UUID()

	first := f.update(commitBody(3000, ref, true))
	require.Equal(t, http.StatusOK, first.Code, "first commit: %s", first.Body.String())
	firstResp := decodeResponse(t, first)

	second := f.update(commitBody(3000, ref, true))
	require.Equal(t, http.StatusOK, second.Code, "repeated commit must replay, got: %s", second.Body.String())
	secondResp := decodeResponse(t, second)

	assert.Equal(t, firstResp["transaction_id"], secondResp["transaction_id"], "replay must return the original commit")
	f.assertBalances(t, "-3000", "3000", "after a repeated commit must reflect one commit")
}

func TestInflightCommitReference_ReuseWithDifferentAmountConflicts(t *testing.T) {
	f := newInflightReferenceFixture(t)
	ref := "capture_" + gofakeit.UUID()

	first := f.update(commitBody(3000, ref, true))
	require.Equal(t, http.StatusOK, first.Code, "first commit: %s", first.Body.String())

	second := f.update(commitBody(2000, ref, true))
	require.Equal(t, http.StatusConflict, second.Code, "same reference with a different amount: %s", second.Body.String())
	assert.Equal(t, "TXN_DUPLICATE_REFERENCE", errorCode(decodeResponse(t, second)))
	f.assertBalances(t, "-3000", "3000", "after a conflicting reuse must be unchanged")
}

func TestInflightCommitReference_DistinctReferencesBothApply(t *testing.T) {
	f := newInflightReferenceFixture(t)

	first := f.update(commitBody(3000, "capture_"+gofakeit.UUID(), true))
	require.Equal(t, http.StatusOK, first.Code, "first commit: %s", first.Body.String())
	second := f.update(commitBody(2000, "capture_"+gofakeit.UUID(), true))
	require.Equal(t, http.StatusOK, second.Code, "second commit: %s", second.Body.String())

	assert.NotEqual(t, decodeResponse(t, first)["transaction_id"], decodeResponse(t, second)["transaction_id"])
	f.assertBalances(t, "-5000", "5000", "after two distinct captures")
}

func TestInflightCommitReference_QueuedRepeatAnsweredSynchronously(t *testing.T) {
	f := newInflightReferenceFixture(t)
	ref := "capture_" + gofakeit.UUID()

	first := f.update(commitBody(3000, ref, true))
	require.Equal(t, http.StatusOK, first.Code, "first commit: %s", first.Body.String())
	firstResp := decodeResponse(t, first)

	queued := f.update(commitBody(3000, ref, false))
	require.Equal(t, http.StatusOK, queued.Code, "queued repeat: %s", queued.Body.String())
	queuedResp := decodeResponse(t, queued)
	assert.NotEqual(t, true, queuedResp["queued"], "a known repeat must not be queued")
	assert.Equal(t, firstResp["transaction_id"], queuedResp["transaction_id"], "queued repeat must replay the original commit")

	// Nothing was enqueued for the repeat, so a new queued capture is accepted rather than 409.
	next := f.update(commitBody(1000, "capture_"+gofakeit.UUID(), false))
	require.Equal(t, http.StatusOK, next.Code, "new queued capture: %s", next.Body.String())
	assert.Equal(t, true, decodeResponse(t, next)["queued"])
}

func TestInflightCommitReference_QueuedReuseWithDifferentAmountConflicts(t *testing.T) {
	f := newInflightReferenceFixture(t)
	ref := "capture_" + gofakeit.UUID()

	first := f.update(commitBody(3000, ref, true))
	require.Equal(t, http.StatusOK, first.Code, "first commit: %s", first.Body.String())

	queued := f.update(commitBody(2000, ref, false))
	require.Equal(t, http.StatusConflict, queued.Code, "queued reuse with a different amount: %s", queued.Body.String())
	assert.Equal(t, "TXN_DUPLICATE_REFERENCE", errorCode(decodeResponse(t, queued)))
}

func TestInflightCommitReference_ConcurrentRepeatAppliesOnce(t *testing.T) {
	f := newInflightReferenceFixture(t)
	ref := "capture_" + gofakeit.UUID()

	// Each caller retries while another request holds the hold's lock, so every
	// attempt eventually gets a real answer.
	const attempts = 5
	recs := make([]*httptest.ResponseRecorder, attempts)
	var wg sync.WaitGroup
	for i := range recs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for try := 0; try < 100; try++ {
				recs[i] = f.update(commitBody(3000, ref, true))
				if !strings.Contains(recs[i].Body.String(), "failed to acquire lock") {
					return
				}
				time.Sleep(20 * time.Millisecond)
			}
		}(i)
	}
	wg.Wait()

	ids := map[interface{}]bool{}
	for _, rec := range recs {
		if rec.Code == http.StatusOK {
			ids[decodeResponse(t, rec)["transaction_id"]] = true
		}
	}
	assert.Len(t, ids, 1, "every successful response must be the same commit")
	f.assertBalances(t, "-3000", "3000", "after concurrent repeats must reflect one commit")
}

func TestInflightVoidReference_RepeatIsReplayed(t *testing.T) {
	f := newInflightReferenceFixture(t)
	ref := "void_" + gofakeit.UUID()
	body := map[string]interface{}{"status": "void", "reference": ref, "skip_queue": true}

	first := f.update(body)
	require.Equal(t, http.StatusOK, first.Code, "first void: %s", first.Body.String())
	second := f.update(body)
	require.Equal(t, http.StatusOK, second.Code, "repeated void must replay, got: %s", second.Body.String())

	assert.Equal(t, decodeResponse(t, first)["transaction_id"], decodeResponse(t, second)["transaction_id"])
	f.assertBalances(t, "0", "0", "after a repeated void")
}
