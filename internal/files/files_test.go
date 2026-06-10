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

package files

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// collector is a thread-safe StoreFunc target.
type collector struct {
	mu   sync.Mutex
	txns []model.ExternalTransaction
}

func (c *collector) store(_ context.Context, _ string, txn model.ExternalTransaction) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.txns = append(c.txns, txn)
	return nil
}

func TestProcessJSON_StreamsAndStampsSource(t *testing.T) {
	body := `[
		{"id":"e1","amount":10.5,"reference":"r1","currency":"USD"},
		{"id":"e2","amount":20,"reference":"r2","currency":"USD"},
		{"id":"e3","amount":30,"reference":"r3","currency":"EUR"}
	]`
	c := &collector{}
	n, err := ProcessJSON(context.Background(), "up_1", "bank_a", strings.NewReader(body), c.store)

	require.NoError(t, err)
	assert.Equal(t, 3, n)
	require.Len(t, c.txns, 3)
	for _, txn := range c.txns {
		assert.Equal(t, "bank_a", txn.Source, "every record must be stamped with the source")
	}
	assert.Equal(t, "e1", c.txns[0].ID)
	assert.Equal(t, 10.5, c.txns[0].Amount)
}

func TestProcessJSON_EmptyArray(t *testing.T) {
	c := &collector{}
	n, err := ProcessJSON(context.Background(), "up_1", "src", strings.NewReader(`[]`), c.store)
	require.NoError(t, err)
	assert.Equal(t, 0, n)
	assert.Empty(t, c.txns)
}

func TestProcessJSON_NonArrayRejected(t *testing.T) {
	c := &collector{}
	_, err := ProcessJSON(context.Background(), "up_1", "src", strings.NewReader(`{"id":"e1"}`), c.store)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "expected a JSON array")
}

func TestProcessJSON_MalformedElementReturnsPartialCount(t *testing.T) {
	// Second element is malformed: the first must have been stored, then the
	// stream aborts with an error and the partial count.
	body := `[{"id":"e1","amount":1},{"id":"e2","amount":}]`
	c := &collector{}
	n, err := ProcessJSON(context.Background(), "up_1", "src", strings.NewReader(body), c.store)
	require.Error(t, err)
	assert.Equal(t, 1, n, "the valid leading element is counted before the failure")
	require.Len(t, c.txns, 1)
	assert.Equal(t, "e1", c.txns[0].ID)
}

func TestProcessJSON_StoreErrorPropagates(t *testing.T) {
	body := `[{"id":"e1"},{"id":"e2"}]`
	called := 0
	store := func(_ context.Context, _ string, _ model.ExternalTransaction) error {
		called++
		return fmt.Errorf("store failed")
	}
	n, err := ProcessJSON(context.Background(), "up_1", "src", strings.NewReader(body), store)
	require.Error(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, 1, called, "must stop at the first store error, not process the rest")
}

func TestProcessJSON_ContextCancellation(t *testing.T) {
	// Build an array larger than one context-check interval so cancellation
	// is observed mid-stream.
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < ContextCheckInterval*3; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":"e%d","amount":1}`, i)
	}
	b.WriteByte(']')

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // already canceled

	c := &collector{}
	n, err := ProcessJSON(ctx, "up_1", "src", strings.NewReader(b.String()), c.store)
	require.ErrorIs(t, err, context.Canceled)
	assert.Less(t, n, ContextCheckInterval*3, "cancellation must stop the stream before the end")
}

// TestProcessJSON_LargeInputStreams proves the streaming path handles a large
// array without materializing it; the previous implementation decoded the
// whole slice into memory first.
func TestProcessJSON_LargeInputStreams(t *testing.T) {
	const count = 50000
	var b strings.Builder
	b.WriteByte('[')
	for i := 0; i < count; i++ {
		if i > 0 {
			b.WriteByte(',')
		}
		fmt.Fprintf(&b, `{"id":"e%d","amount":1,"currency":"USD"}`, i)
	}
	b.WriteByte(']')

	// A counting store keeps the test's own memory flat.
	stored := 0
	store := func(_ context.Context, _ string, _ model.ExternalTransaction) error {
		stored++
		return nil
	}
	n, err := ProcessJSON(context.Background(), "up_1", "src", strings.NewReader(b.String()), store)
	require.NoError(t, err)
	assert.Equal(t, count, n)
	assert.Equal(t, count, stored)
}
