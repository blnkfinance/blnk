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
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/blnkfinance/blnk/config"
	"github.com/hibiken/asynq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// receivedWebhook captures everything the webhook receiver saw for one request.
type receivedWebhook struct {
	body    []byte
	headers http.Header
	method  string
}

// newWebhookReceiver starts an httptest server responding with status and
// returns an accessor for the captured requests.
func newWebhookReceiver(status int) (*httptest.Server, func() []receivedWebhook) {
	var mu sync.Mutex
	var received []receivedWebhook

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		received = append(received, receivedWebhook{
			body:    body,
			headers: r.Header.Clone(),
			method:  r.Method,
		})
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(`{"received":true}`))
	}))

	get := func() []receivedWebhook {
		mu.Lock()
		defer mu.Unlock()
		out := make([]receivedWebhook, len(received))
		copy(out, received)
		return out
	}
	return server, get
}

// storeWebhookTestConfig stores a configuration pointing the webhook
// notification at url, with a unique webhook queue per test to avoid
// cross-test interference on the shared Redis instance.
func storeWebhookTestConfig(url, secret string, headers map[string]string) (*config.Configuration, string) {
	queueName := fmt.Sprintf("webhook_q_%d", time.Now().UnixNano())
	cnf := &config.Configuration{
		Redis: config.RedisConfig{Dns: "localhost:6379"},
		Server: config.ServerConfig{
			SecretKey: secret,
		},
		Queue: config.QueueConfig{
			WebhookQueue:   queueName,
			NumberOfQueues: 1,
		},
		Notification: config.Notification{
			Webhook: config.WebhookConfig{
				Url:     url,
				Headers: headers,
			},
		},
	}
	config.ConfigStore.Store(cnf)
	return cnf, queueName
}

func TestProcessWebhook_DeliversSignedPayload(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()

	const secret = "webhook-signing-secret"
	storeWebhookTestConfig(server.URL, secret, map[string]string{
		"X-Custom-Tenant": "tenant-42",
	})

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	event := NewWebhook{
		Event: "transaction.applied",
		Payload: map[string]interface{}{
			"transaction_id": "txn_process_test",
			"amount":         150.25,
		},
	}
	taskPayload, err := json.Marshal(event)
	require.NoError(t, err)

	before := time.Now().Unix()
	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", taskPayload))
	after := time.Now().Unix()
	require.NoError(t, err)

	reqs := received()
	require.Len(t, reqs, 1, "receiver must get exactly one delivery")
	req := reqs[0]

	// Method and content type.
	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "application/json", req.headers.Get("Content-Type"))

	// Custom configured header is forwarded.
	assert.Equal(t, "tenant-42", req.headers.Get("X-Custom-Tenant"))

	// Event envelope: {"event": ..., "data": ...}.
	var envelope map[string]interface{}
	require.NoError(t, json.Unmarshal(req.body, &envelope))
	assert.Equal(t, "transaction.applied", envelope["event"])
	data, ok := envelope["data"].(map[string]interface{})
	require.True(t, ok, "data field must be the event payload object")
	assert.Equal(t, "txn_process_test", data["transaction_id"])
	assert.Equal(t, 150.25, data["amount"])

	// Timestamp header must be a unix timestamp from the delivery window.
	tsHeader := req.headers.Get("X-Blnk-Timestamp")
	require.NotEmpty(t, tsHeader, "X-Blnk-Timestamp must be set")
	ts, err := strconv.ParseInt(tsHeader, 10, 64)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, ts, before)
	assert.LessOrEqual(t, ts, after)

	// Signature must be HMAC-SHA256(secret, timestamp + "." + body) hex-encoded,
	// recomputable by the receiver from what it was given.
	sigHeader := req.headers.Get("X-Blnk-Signature")
	require.NotEmpty(t, sigHeader, "X-Blnk-Signature must be set")
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(tsHeader + "." + string(req.body)))
	expectedSig := hex.EncodeToString(mac.Sum(nil))
	assert.Equal(t, expectedSig, sigHeader, "signature must verify against timestamp.body with the shared secret")
}

func TestProcessWebhook_MalformedTaskPayloadReturnsError(t *testing.T) {
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()
	storeWebhookTestConfig(server.URL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", []byte(`{"event": "broken`)))
	assert.Error(t, err, "corrupt task payload must surface an error so asynq can retry/dead-letter")
	assert.Empty(t, received(), "no HTTP call should be made for an unparseable payload")
}

func TestProcessWebhook_NoURLConfiguredIsNoOp(t *testing.T) {
	storeWebhookTestConfig("", "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	payload, err := json.Marshal(NewWebhook{Event: "transaction.applied", Payload: map[string]string{"k": "v"}})
	require.NoError(t, err)

	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", payload))
	assert.NoError(t, err, "missing webhook URL should be a silent no-op, not a task failure")
}

func TestProcessWebhook_ConnectionRefusedReturnsError(t *testing.T) {
	server, _ := newWebhookReceiver(http.StatusOK)
	deadURL := server.URL
	server.Close() // receiver is down

	storeWebhookTestConfig(deadURL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	payload, err := json.Marshal(NewWebhook{Event: "transaction.applied", Payload: map[string]string{"k": "v"}})
	require.NoError(t, err)

	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", payload))
	assert.Error(t, err, "transport-level failure must propagate so asynq retries the delivery")
}

func TestProcessWebhook_Non2xxTriggersRetry(t *testing.T) {
	// Regression: processHTTP used to swallow non-2xx responses (nil error),
	// so asynq marked the task succeeded and the webhook was permanently
	// lost on receiver-side failures. It must now return an error so asynq's
	// retry machinery redelivers.
	server, received := newWebhookReceiver(http.StatusServiceUnavailable)
	defer server.Close()
	storeWebhookTestConfig(server.URL, "secret", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	payload, err := json.Marshal(NewWebhook{Event: "transaction.applied", Payload: map[string]string{"k": "v"}})
	require.NoError(t, err)

	err = b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", payload))
	assert.Error(t, err, "non-2xx from receiver must fail the task so asynq retries delivery")
	assert.Len(t, received(), 1, "exactly one attempt per ProcessWebhook call; retries are asynq's job")
}

func TestSendWebhook_EnqueuesTaskWithPayloadOnConfiguredQueue(t *testing.T) {
	cnf, queueName := storeWebhookTestConfig("http://localhost:1/never-called", "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_, _ = inspector.DeleteAllPendingTasks(queueName)
		_ = inspector.DeleteQueue(queueName, true)
	})

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	event := NewWebhook{
		Event: "transaction.inflight",
		Payload: map[string]interface{}{
			"transaction_id": gofakeit.UUID(),
			"status":         "INFLIGHT",
		},
	}
	require.NoError(t, b.SendWebhook(event))

	tasks, err := inspector.ListPendingTasks(queueName)
	require.NoError(t, err)
	require.Len(t, tasks, 1, "exactly one task should land on the webhook queue")

	task := tasks[0]
	assert.Equal(t, cnf.Queue.WebhookQueue, task.Type, "task type must be the webhook queue name for worker routing")
	assert.Equal(t, queueName, task.Queue)

	var roundTripped NewWebhook
	require.NoError(t, json.Unmarshal(task.Payload, &roundTripped))
	assert.Equal(t, "transaction.inflight", roundTripped.Event)
	data, ok := roundTripped.Payload.(map[string]interface{})
	require.True(t, ok)
	assert.Equal(t, "INFLIGHT", data["status"])
}

func TestSendWebhook_NoURLConfiguredSkipsEnqueue(t *testing.T) {
	_, queueName := storeWebhookTestConfig("", "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_ = inspector.DeleteQueue(queueName, true)
	})

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	require.NoError(t, b.SendWebhook(NewWebhook{Event: "transaction.applied", Payload: map[string]string{"k": "v"}}))

	tasks, err := inspector.ListPendingTasks(queueName)
	if err == nil {
		assert.Empty(t, tasks, "nothing should be enqueued when no webhook URL is configured")
	}
	// err != nil means the queue does not exist at all, which is equally correct.
}

func TestSendWebhook_UnmarshalablePayloadReturnsError(t *testing.T) {
	_, queueName := storeWebhookTestConfig("http://localhost:1/never-called", "secret", nil)

	inspector := asynq.NewInspector(asynq.RedisClientOpt{Addr: "localhost:6379"})
	t.Cleanup(func() {
		_ = inspector.DeleteQueue(queueName, true)
	})

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	// channels cannot be JSON-marshaled
	err = b.SendWebhook(NewWebhook{Event: "transaction.applied", Payload: make(chan int)})
	assert.Error(t, err)

	tasks, listErr := inspector.ListPendingTasks(queueName)
	if listErr == nil {
		assert.Empty(t, tasks, "nothing should be enqueued when payload serialization fails")
	}
}

func TestProcessWebhook_NoSignatureHeadersWithEmptySecret(t *testing.T) {
	// Regression: an empty Server.SecretKey used to produce an HMAC over the
	// empty key — computable (and forgeable) by anyone. The delivery is now
	// sent unsigned so receivers cannot mistake a forgeable header for
	// authentication.
	server, received := newWebhookReceiver(http.StatusOK)
	defer server.Close()
	storeWebhookTestConfig(server.URL, "", nil)

	b, err := NewBlnk(nil)
	require.NoError(t, err)
	defer func() { _ = b.Close() }()

	payload, err := json.Marshal(NewWebhook{Event: "transaction.void", Payload: map[string]string{"id": "1"}})
	require.NoError(t, err)
	require.NoError(t, b.ProcessWebhook(context.Background(), asynq.NewTask("webhook_delivery", payload)))

	reqs := received()
	require.Len(t, reqs, 1)
	assert.Empty(t, reqs[0].headers.Get("X-Blnk-Signature"), "no signature header may be sent without a configured secret")
	assert.Empty(t, reqs[0].headers.Get("X-Blnk-Timestamp"), "no timestamp header may be sent without a configured secret")
}
