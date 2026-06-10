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

package search

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/typesense/typesense-go/typesense"
)

// Fault-injection tests: typesense-go accepts any server URL, so these point
// the client at an httptest.Server scripted to fail in ways a healthy real
// Typesense cannot produce (5xx, timeouts, garbage responses).

// newFaultClient builds a TypesenseClient against the fake server with a
// short timeout so failure tests stay fast. Constructed directly (instead of
// NewTypesenseClient) to override the production 5s connection timeout.
func newFaultClient(serverURL string, timeout time.Duration) *TypesenseClient {
	return &TypesenseClient{Client: typesense.NewClient(
		typesense.WithServer(serverURL),
		typesense.WithAPIKey("test-key"),
		typesense.WithConnectionTimeout(timeout),
	)}
}

func TestCreateCollection_ServerErrorIsNotSwallowed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"message": "disk full"}`))
	}))
	defer srv.Close()

	client := newFaultClient(srv.URL, time.Second)
	resp, err := client.CreateCollection(context.Background(), getLedgerSchema())

	// A 500 must propagate — only "already exists" errors are suppressed.
	require.Error(t, err)
	assert.Nil(t, resp)
	assert.NotContains(t, err.Error(), "already exists")
}

func TestHandleNotification_UpsertServerErrorPropagates(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"message": "service unavailable"}`))
	}))
	defer srv.Close()

	client := newFaultClient(srv.URL, time.Second)
	err := client.HandleNotification(context.Background(), CollectionLedgers, map[string]interface{}{
		"ledger_id":  "ldg_fault",
		"name":       "fault ledger",
		"created_at": time.Now(),
	})

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to upsert document")
}

func TestUpsert_SlowServerTimesOut(t *testing.T) {
	release := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()
	defer close(release)

	client := newFaultClient(srv.URL, 150*time.Millisecond)

	start := time.Now()
	err := client.HandleNotification(context.Background(), CollectionLedgers, map[string]interface{}{
		"ledger_id":  "ldg_slow",
		"name":       "slow ledger",
		"created_at": time.Now(),
	})

	require.Error(t, err, "a server slower than the client timeout must produce an error")
	assert.Less(t, time.Since(start), 5*time.Second, "timeout should respect the configured budget, not hang")
}

func TestMigrateTypeSenseSchema_MalformedResponse(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{not valid json`))
	}))
	defer srv.Close()

	client := newFaultClient(srv.URL, time.Second)
	err := client.MigrateTypeSenseSchema(context.Background(), CollectionLedgers)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to retrieve current schema")
}

func TestHandleBatchNotification_FailingDependencyStopsBatch(t *testing.T) {
	// Server fails every document write: the batch must abort on the first
	// dependency and never reach the primary item.
	var requests int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&requests, 1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte(`{"message": "down"}`))
	}))
	defer srv.Close()

	client := newFaultClient(srv.URL, time.Second)
	batch := NewIndexBatch("b1")
	batch.AddDependency(CollectionBalances, "bln_1", map[string]interface{}{"balance_id": "bln_1", "created_at": time.Now()})
	batch.SetPrimary(CollectionTransactions, "txn_1", map[string]interface{}{"transaction_id": "txn_1", "created_at": time.Now()})

	err := client.HandleBatchNotification(context.Background(), batch)

	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to index dependency", "batch must fail on the dependency, before the primary")
	assert.Contains(t, err.Error(), "bln_1")
}
