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

// These tests are in package request, not request_test, so they can reach
// callClient. That is deliberate: the bound has to stay fixed and unreachable
// from other packages — no exported setter — but it still has to be provable
// that it exists and that it fires. Substituting the client from inside the
// package is the narrowest way to get both, and it cannot affect any caller.
package request

import (
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

// TestCallClientTimeoutIsFixed guards the bound itself. A client with no
// Timeout waits forever on a receiver that accepts the connection and then
// never answers, which is how a single unresponsive webhook endpoint can pin
// a goroutine for the life of the process.
func TestCallClientTimeoutIsFixed(t *testing.T) {
	require.Equal(t, 30*time.Second, callClient.Timeout,
		"outbound calls must stay bounded by a fixed timeout")
}

// TestCallReturnsWhenTimeoutFires proves the bound is actually applied to the
// request — a Timeout set on a client that Call does not use would satisfy the
// test above and still hang here.
//
// The real 30s default cannot be asserted directly without a 30s wall-clock
// wait, so the same client is swapped for a short-timeout one for the length
// of this test. The substitution is package-local and restored on return;
// tests in this package do not run in parallel with each other.
func TestCallReturnsWhenTimeoutFires(t *testing.T) {
	blockForever := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-blockForever // accept, then never answer
	}))
	defer func() {
		close(blockForever)
		server.Close()
	}()

	original := callClient
	callClient = &http.Client{Timeout: 200 * time.Millisecond}
	defer func() { callClient = original }()

	req, err := http.NewRequest(http.MethodGet, server.URL, nil)
	require.NoError(t, err)

	done := make(chan error, 1)
	go func() {
		var response map[string]interface{}
		_, callErr := Call(req, &response)
		done <- callErr
	}()

	select {
	case callErr := <-done:
		require.Error(t, callErr, "a hung receiver must surface as a timeout error")
		var netErr net.Error
		require.True(t, errors.As(callErr, &netErr) && netErr.Timeout(),
			"expected a timeout error, got: %v", callErr)
	case <-time.After(10 * time.Second):
		t.Fatal("Call blocked on a hung receiver: the client timeout is not being applied")
	}
}
