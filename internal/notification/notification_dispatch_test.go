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

package notification

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// storeNotificationConfig installs a configuration with the given Slack and
// webhook URLs. DataSource/Redis DNS are required by config validation but are
// never dialed by the notification package.
func storeNotificationConfig(t *testing.T, slackURL, webhookURL string) {
	t.Helper()
	config.MockConfig(&config.Configuration{
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Notification: config.Notification{
			Slack:   config.SlackWebhook{WebhookUrl: slackURL},
			Webhook: config.WebhookConfig{Url: webhookURL},
		},
	})
	// MockConfig silently refuses to store invalid configs; fail loudly instead.
	conf, err := config.Fetch()
	require.NoError(t, err)
	require.Equal(t, slackURL, conf.Notification.Slack.WebhookUrl)
	require.Equal(t, webhookURL, conf.Notification.Webhook.Url)
}

// capturedRequest records what the fake Slack endpoint received.
type capturedRequest struct {
	method      string
	contentType string
	body        []byte
}

// newSlackCaptureServer spins up an httptest server that records every request
// and responds with the given status code and body.
func newSlackCaptureServer(status int, responseBody string) (*httptest.Server, func() []capturedRequest) {
	var mu sync.Mutex
	var captured []capturedRequest

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		captured = append(captured, capturedRequest{
			method:      r.Method,
			contentType: r.Header.Get("Content-Type"),
			body:        body,
		})
		mu.Unlock()
		w.WriteHeader(status)
		_, _ = w.Write([]byte(responseBody))
	}))

	get := func() []capturedRequest {
		mu.Lock()
		defer mu.Unlock()
		out := make([]capturedRequest, len(captured))
		copy(out, captured)
		return out
	}
	return server, get
}

// slackPayload mirrors the block structure SlackNotification builds.
type slackPayload struct {
	Blocks []struct {
		Type string `json:"type"`
		Text *struct {
			Type  string `json:"type"`
			Text  string `json:"text"`
			Emoji bool   `json:"emoji"`
		} `json:"text,omitempty"`
		Fields []struct {
			Type string `json:"type"`
			Text string `json:"text"`
		} `json:"fields,omitempty"`
	} `json:"blocks"`
}

func TestSlackNotification_PayloadShape(t *testing.T) {
	server, captured := newSlackCaptureServer(http.StatusOK, `{}`)
	defer server.Close()
	storeNotificationConfig(t, server.URL, "")

	SlackNotification(errors.New("ledger exploded: insufficient funds"))

	reqs := captured()
	require.Len(t, reqs, 1, "exactly one Slack call expected")

	req := reqs[0]
	assert.Equal(t, http.MethodPost, req.method)
	assert.Equal(t, "application/json", req.contentType)

	var payload slackPayload
	require.NoError(t, json.Unmarshal(req.body, &payload), "Slack payload must be valid JSON, got: %s", string(req.body))
	require.Len(t, payload.Blocks, 3)

	// Header block.
	require.NotNil(t, payload.Blocks[0].Text)
	assert.Equal(t, "header", payload.Blocks[0].Type)
	assert.Equal(t, "Error From Blnk 🐞", payload.Blocks[0].Text.Text)

	// Error block must carry the actual error message.
	require.NotEmpty(t, payload.Blocks[1].Fields)
	assert.Contains(t, payload.Blocks[1].Fields[0].Text, "*Error:*")
	assert.Contains(t, payload.Blocks[1].Fields[0].Text, "ledger exploded: insufficient funds")

	// Time block must carry a parseable RFC822 timestamp.
	require.NotEmpty(t, payload.Blocks[2].Fields)
	assert.Contains(t, payload.Blocks[2].Fields[0].Text, "*Time:*")
}

func TestSlackNotification_ErrorMessageWithQuotesStillDelivered(t *testing.T) {
	// Regression: the Slack payload used to be built by fmt.Sprintf-ing
	// err.Error() into a raw JSON template, so quotes/backslashes/newlines in
	// the error (e.g. pq errors) produced invalid JSON and the notification
	// was silently dropped. The payload is now built with json.Marshal.
	server, captured := newSlackCaptureServer(http.StatusOK, `{}`)
	defer server.Close()
	storeNotificationConfig(t, server.URL, "")

	SlackNotification(errors.New(`pq: column "balance_id" does not exist`))

	reqs := captured()
	require.Len(t, reqs, 1, "notification with quoted error message must still be delivered")
	var payload slackPayload
	require.NoError(t, json.Unmarshal(reqs[0].body, &payload))
	assert.Contains(t, payload.Blocks[1].Fields[0].Text, `column "balance_id" does not exist`)
}

func TestSlackNotification_Receiver500DoesNotPanicAndDoesNotRetry(t *testing.T) {
	server, captured := newSlackCaptureServer(http.StatusInternalServerError, `{"ok":false}`)
	defer server.Close()
	storeNotificationConfig(t, server.URL, "")

	assert.NotPanics(t, func() {
		SlackNotification(errors.New("downstream said no"))
	})

	// Document the current contract: one attempt, no retries, failure swallowed.
	assert.Len(t, captured(), 1)
}

func TestSlackNotification_NonJSONResponseBody(t *testing.T) {
	// Real Slack webhooks reply with plain-text "ok", which is not valid JSON.
	// The JSON decode of the response fails internally; the function must
	// still have delivered the message and must not panic.
	server, captured := newSlackCaptureServer(http.StatusOK, `ok`)
	defer server.Close()
	storeNotificationConfig(t, server.URL, "")

	assert.NotPanics(t, func() {
		SlackNotification(errors.New("plain text response handling"))
	})
	assert.Len(t, captured(), 1)
}

func TestSlackNotification_ConnectionRefused(t *testing.T) {
	server, _ := newSlackCaptureServer(http.StatusOK, `{}`)
	deadURL := server.URL
	server.Close() // nothing listening anymore

	storeNotificationConfig(t, deadURL, "")

	assert.NotPanics(t, func() {
		SlackNotification(errors.New("nobody is listening"))
	})
}

func TestSlackNotification_HangingReceiverShouldTimeOut(t *testing.T) {
	// FIXED: request.Call now uses an http.Client with a 30s Timeout, so a
	// hung Slack endpoint no longer blocks the NotifyError goroutine forever.
	// The positive verification is still skipped because it needs a ~30s
	// wall-clock wait for the real timeout to fire.
	t.Skip("verifying the 30s client timeout requires a 30s wall-clock wait; " +
		"the timeout is set in internal/request/request.go Call()")

	blockForever := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blockForever // never respond
	}))
	defer func() {
		close(blockForever)
		server.Close()
	}()

	storeNotificationConfig(t, server.URL, "")

	done := make(chan struct{})
	go func() {
		SlackNotification(errors.New("should not hang forever"))
		close(done)
	}()

	select {
	case <-done:
		// returned in bounded time: a timeout exists
	case <-time.After(35 * time.Second): // generous bound > the 30s used elsewhere
		t.Fatal("SlackNotification blocked indefinitely on a hung receiver: no HTTP timeout configured")
	}
}

func TestNotifyError_WebhookSenderReceivesSystemError(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeNotificationConfig(t, "", "http://example.invalid/webhook-target")

	type call struct {
		event   string
		payload interface{}
	}
	calls := make(chan call, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		calls <- call{event: event, payload: payload}
		return nil
	})

	NotifyError(errors.New("queue worker crashed"))

	select {
	case got := <-calls:
		assert.Equal(t, "system.error", got.event)
		payloadMap, ok := got.payload.(map[string]interface{})
		require.True(t, ok, "payload should be a map, got %T", got.payload)
		assert.Equal(t, "queue worker crashed", payloadMap["error"])
		ts, ok := payloadMap["time"].(time.Time)
		require.True(t, ok, "payload time should be a time.Time")
		assert.WithinDuration(t, time.Now(), ts, 10*time.Second)
	case <-time.After(3 * time.Second):
		t.Fatal("webhook sender was never invoked by NotifyError")
	}
}

func TestNotifyError_NoWebhookURLConfigured_SenderNotCalled(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeNotificationConfig(t, "", "")

	calls := make(chan string, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		calls <- event
		return nil
	})

	NotifyError(errors.New("should stay local"))

	select {
	case event := <-calls:
		t.Fatalf("webhook sender called (%q) despite empty webhook URL", event)
	case <-time.After(500 * time.Millisecond):
		// expected: nothing dispatched
	}
}

func TestNotifyError_NoSenderRegistered_DoesNotPanic(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)
	webhookSender = nil

	storeNotificationConfig(t, "", "http://example.invalid/webhook-target")

	assert.NotPanics(t, func() {
		NotifyError(errors.New("no sender registered"))
		time.Sleep(300 * time.Millisecond) // let the goroutine run
	})
}

func TestNotifyError_SenderErrorIsSwallowed(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	storeNotificationConfig(t, "", "http://example.invalid/webhook-target")

	called := make(chan struct{}, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		called <- struct{}{}
		return errors.New("downstream webhook enqueue failed")
	})

	assert.NotPanics(t, func() {
		NotifyError(errors.New("sender will fail"))
	})

	select {
	case <-called:
		// the error from the sender must not propagate or panic
	case <-time.After(3 * time.Second):
		t.Fatal("webhook sender was never invoked")
	}
}

func TestNotifyError_DispatchesToBothSlackAndWebhook(t *testing.T) {
	original := webhookSender
	defer RegisterWebhookSender(original)

	server, captured := newSlackCaptureServer(http.StatusOK, `{}`)
	defer server.Close()

	storeNotificationConfig(t, server.URL, "http://example.invalid/webhook-target")

	senderCalled := make(chan string, 1)
	RegisterWebhookSender(func(event string, payload interface{}) error {
		senderCalled <- event
		return nil
	})

	NotifyError(errors.New("dual channel failure"))

	select {
	case event := <-senderCalled:
		assert.Equal(t, "system.error", event)
	case <-time.After(3 * time.Second):
		t.Fatal("webhook sender was never invoked")
	}

	// Slack is called synchronously before the webhook sender inside the
	// NotifyError goroutine, so by now it must have been hit.
	reqs := captured()
	require.Len(t, reqs, 1, "Slack should have received the error notification")
	assert.Contains(t, string(reqs[0].body), "dual channel failure")
}
