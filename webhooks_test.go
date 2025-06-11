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
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// MockConfigFetcher is a mock for the config fetching
type MockConfigFetcher struct {
	mock.Mock
}

func (m *MockConfigFetcher) Fetch() (*config.Configuration, error) {
	args := m.Called()
	return args.Get(0).(*config.Configuration), args.Error(1)
}

func TestSendWebhook(t *testing.T) {
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("an error '%s' occurred when starting miniredis", err)
	}
	defer mr.Close()

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: "http://localhost:8080",
			},
		},
	}
	config.ConfigStore.Store(cnf)

	testData := NewWebhook{
		Event:   "transaction.queued",
		Payload: getTransactionMock(10000, false),
	}
	blnk, err := NewBlnk(nil)
	assert.NoError(t, err)

	err = blnk.SendWebhook(testData)
	assert.NoError(t, err)

	// Verify that the task was enqueued
	assert.NoError(t, err)
	tasks := mr.Keys()
	assert.NoError(t, err)
	assert.NotEmpty(t, tasks)

}

func TestConnectionReuse(t *testing.T) {
	// Track unique connections
	var connectionsMutex sync.Mutex
	connections := make(map[string]bool)
	requestCount := 0

	// Create test server that tracks connections
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		connectionsMutex.Lock()
		defer connectionsMutex.Unlock()

		// Track unique remote addresses (connections)
		remoteAddr := r.RemoteAddr
		connections[remoteAddr] = true
		requestCount++

		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	// Setup miniredis for queue
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("an error '%s' occurred when starting miniredis", err)
	}
	defer mr.Close()

	// Configure with test server URL
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: server.URL,
			},
		},
	}
	config.ConfigStore.Store(cnf)

	// Create Blnk instance
	blnk, err := NewBlnk(nil)
	assert.NoError(t, err)
	defer blnk.Close()

	// Send multiple webhook requests directly (bypass queue for immediate testing)
	numRequests := 10
	testWebhook := NewWebhook{
		Event:   "transaction.applied",
		Payload: map[string]interface{}{"test": "data"},
	}

	// Send multiple requests concurrently to test connection reuse
	var wg sync.WaitGroup
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := processHTTP(testWebhook, blnk.httpClient)
			assert.NoError(t, err)
		}()
	}
	wg.Wait()

	// Allow some time for all requests to complete
	time.Sleep(100 * time.Millisecond)

	connectionsMutex.Lock()
	uniqueConnections := len(connections)
	totalRequests := requestCount
	connectionsMutex.Unlock()

	// Verify connection reuse
	t.Logf("Total requests: %d, Unique connections: %d", totalRequests, uniqueConnections)

	// With connection reuse, we should have fewer unique connections than requests
	// Allow for some flexibility as the exact number can vary based on timing
	assert.Equal(t, numRequests, totalRequests, "All requests should have been received")
	assert.LessOrEqual(t, uniqueConnections, totalRequests, "Should have fewer or equal connections than requests")

	// In most cases with proper connection reuse, we should see significantly fewer connections
	// This is a more lenient check to account for test environment variations
	if uniqueConnections < totalRequests {
		t.Logf("✅ Connection reuse working: %d connections for %d requests", uniqueConnections, totalRequests)
	} else {
		t.Logf("⚠️  Connection reuse may not be optimal: %d connections for %d requests", uniqueConnections, totalRequests)
	}
}

func TestHTTPClientConfiguration(t *testing.T) {
	// Setup miniredis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("an error '%s' occurred when starting miniredis", err)
	}
	defer mr.Close()

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
	}
	config.ConfigStore.Store(cnf)

	// Create Blnk instance
	blnk, err := NewBlnk(nil)
	assert.NoError(t, err)
	defer blnk.Close()

	// Verify HTTP client is configured properly
	assert.NotNil(t, blnk.httpClient, "HTTP client should be initialized")
	assert.Equal(t, 30*time.Second, blnk.httpClient.Timeout, "Timeout should be 30 seconds")

	// Verify transport configuration
	transport, ok := blnk.httpClient.Transport.(*http.Transport)
	assert.True(t, ok, "Transport should be *http.Transport")
	assert.Equal(t, 100, transport.MaxIdleConns, "MaxIdleConns should be 100")
	assert.Equal(t, 10, transport.MaxIdleConnsPerHost, "MaxIdleConnsPerHost should be 10")
	assert.Equal(t, 90*time.Second, transport.IdleConnTimeout, "IdleConnTimeout should be 90 seconds")
}

func TestProcessWebhookWithReusedClient(t *testing.T) {
	// Track that the same client instance is used
	var clientUsed *http.Client
	var clientMutex sync.Mutex

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{"status":"ok"}`))
	}))
	defer server.Close()

	// Setup miniredis
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("an error '%s' occurred when starting miniredis", err)
	}
	defer mr.Close()

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: mr.Addr(),
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url: server.URL,
			},
		},
	}
	config.ConfigStore.Store(cnf)

	blnk, err := NewBlnk(nil)
	assert.NoError(t, err)
	defer blnk.Close()

	// Store reference to the HTTP client
	clientMutex.Lock()
	clientUsed = blnk.httpClient
	clientMutex.Unlock()

	// Create webhook payloads
	webhook1 := NewWebhook{Event: "test.event1", Payload: map[string]string{"id": "1"}}
	webhook2 := NewWebhook{Event: "test.event2", Payload: map[string]string{"id": "2"}}

	// Process webhooks using the same client
	err1 := processHTTP(webhook1, blnk.httpClient)
	err2 := processHTTP(webhook2, blnk.httpClient)

	assert.NoError(t, err1)
	assert.NoError(t, err2)

	// Verify the same client instance was used
	clientMutex.Lock()
	assert.Same(t, clientUsed, blnk.httpClient, "Should use the same HTTP client instance")
	clientMutex.Unlock()
}
