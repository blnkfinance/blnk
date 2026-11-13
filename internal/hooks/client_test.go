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

package hooks

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/blnkfinance/blnk/config"
	"github.com/redis/go-redis/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestExecuteHookSignature(t *testing.T) {
	execute := func(t *testing.T, secret string) (http.Header, []byte) {
		t.Helper()

		var headers http.Header
		var body []byte
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			headers = r.Header.Clone()
			body, _ = io.ReadAll(r.Body)
			w.WriteHeader(http.StatusOK)
		}))
		t.Cleanup(server.Close)

		redisServer := miniredis.RunT(t)
		client := redis.NewClient(&redis.Options{Addr: redisServer.Addr()})
		t.Cleanup(func() { _ = client.Close() })

		cfg := &config.Configuration{}
		cfg.Server.SecretKey = secret
		manager := &redisHookManager{client: client, config: cfg}

		hook := &Hook{ID: "hook-1", Name: "Hook", URL: server.URL, Type: PostTransaction, Timeout: 5}
		payload := HookPayload{TransactionID: "txn-1", HookType: PostTransaction, Timestamp: time.Now()}
		require.NoError(t, manager.ExecuteHook(context.Background(), hook, payload))

		return headers, body
	}

	t.Run("signs with the configured secret", func(t *testing.T) {
		headers, body := execute(t, "test-secret")

		timestamp := headers.Get("X-Blnk-Timestamp")
		require.NotEmpty(t, timestamp)
		mac := hmac.New(sha256.New, []byte("test-secret"))
		mac.Write([]byte(timestamp + "." + string(body)))
		assert.Equal(t, hex.EncodeToString(mac.Sum(nil)), headers.Get("X-Blnk-Signature"))
	})

	t.Run("sends no signature without a secret", func(t *testing.T) {
		headers, _ := execute(t, "")

		assert.Empty(t, headers.Get("X-Blnk-Signature"), "an HMAC with an empty key is forgeable")
		assert.Empty(t, headers.Get("X-Blnk-Timestamp"))
		assert.Equal(t, "hook-1", headers.Get("X-Hook-ID"))
	})
}
