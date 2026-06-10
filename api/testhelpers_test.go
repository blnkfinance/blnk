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
	"io"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/require"
)

// setupRouterWithConfig builds a router like setupRouter but lets the caller
// adjust the mock configuration (e.g. TokenizationSecret, BackupDir, TypeSense)
// before the Blnk instance is created.
func setupRouterWithConfig(t *testing.T, mutate func(*config.Configuration)) (*gin.Engine, *blnk.Blnk, *config.Configuration) {
	t.Helper()

	cfg := &config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_api_md_async",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	}
	if mutate != nil {
		mutate(cfg)
	}
	config.MockConfig(cfg)

	cnf, err := config.Fetch()
	require.NoError(t, err, "failed to fetch config")

	db, err := database.NewDataSource(cnf)
	require.NoError(t, err, "failed to create datasource")

	newBlnk, err := blnk.NewBlnk(db)
	require.NoError(t, err, "failed to create blnk instance")

	return NewAPI(newBlnk).Router(), newBlnk, cnf
}

// setupAuthedRouter builds a router whose requests carry the given API-key
// principal context, mirroring what the auth middleware sets in secure mode.
func setupAuthedRouter(t *testing.T, isMaster bool, principal *model.APIKey) (*gin.Engine, *blnk.Blnk) {
	t.Helper()

	config.MockConfig(&config.Configuration{
		Queue: config.QueueConfig{
			TransactionQueue: "transaction_queue_test_api_md_async",
			NumberOfQueues:   1,
		},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	})
	cnf, err := config.Fetch()
	require.NoError(t, err)

	db, err := database.NewDataSource(cnf)
	require.NoError(t, err)

	newBlnk, err := blnk.NewBlnk(db)
	require.NoError(t, err)

	apiInstance := NewAPI(newBlnk)
	apiInstance.router.Use(func(c *gin.Context) {
		c.Set("isMasterKey", isMaster)
		if principal != nil {
			c.Set("apiKey", principal)
		}
		c.Next()
	})

	return apiInstance.Router(), newBlnk
}

type balanceFixtureOpts struct {
	indicator        string
	trackFundLineage bool
}

// createTestBalanceWithLedger creates a ledger and a balance through the
// service layer. Indicator and lineage tracking are only settable here, not
// via the public create-balance API model.
func createTestBalanceWithLedger(t *testing.T, b *blnk.Blnk, currency string, opts *balanceFixtureOpts) *model.Balance {
	t.Helper()

	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	require.NoError(t, err, "failed to create ledger")

	balance := model.Balance{LedgerID: ledger.LedgerID, Currency: currency}
	if opts != nil {
		balance.Indicator = opts.indicator
		if opts.trackFundLineage {
			identity, err := b.CreateIdentity(model.Identity{
				IdentityType: "individual",
				FirstName:    gofakeit.FirstName(),
				LastName:     gofakeit.LastName(),
				EmailAddress: gofakeit.Email(),
			})
			require.NoError(t, err, "failed to create identity for lineage balance")
			balance.IdentityID = identity.IdentityID
			balance.TrackFundLineage = true
		}
	}

	created, err := b.CreateBalance(context.Background(), balance)
	require.NoError(t, err, "failed to create balance")
	return &created
}

// createTestIdentity creates an identity through the service layer with
// realistic tokenizable fields.
func createTestIdentity(t *testing.T, b *blnk.Blnk) *model.Identity {
	t.Helper()

	identity, err := b.CreateIdentity(model.Identity{
		IdentityType: "individual",
		FirstName:    gofakeit.FirstName(),
		LastName:     gofakeit.LastName(),
		EmailAddress: gofakeit.Email(),
		PhoneNumber:  gofakeit.Phone(),
		Street:       gofakeit.Street(),
		PostCode:     gofakeit.Zip(),
	})
	require.NoError(t, err, "failed to create identity")
	return &identity
}

// backdateTransaction rewrites a transaction's created_at so recovery tests
// can treat it as stuck without waiting out the real threshold. The
// prevent_transaction_update trigger forbids this, so it is bypassed for this
// session via session_replication_role (requires superuser, which the test
// database user is).
func backdateTransaction(t *testing.T, cnf *config.Configuration, transactionID string, age time.Duration) {
	t.Helper()

	ds, err := database.GetDBConnection(cnf)
	require.NoError(t, err, "failed to get db connection")

	ctx := context.Background()
	conn, err := ds.Conn.Conn(ctx)
	require.NoError(t, err, "failed to acquire db connection")
	defer func() { _ = conn.Close() }()

	_, err = conn.ExecContext(ctx, "SET session_replication_role = replica")
	require.NoError(t, err, "failed to disable triggers for backdate")

	// The recovery cutoff is computed as a naive UTC timestamp
	// (GetStuckQueuedTransactions uses time.Now().UTC()), so anchor the
	// backdated value to UTC regardless of the server timezone.
	_, err = conn.ExecContext(ctx,
		"UPDATE blnk.transactions SET created_at = (NOW() AT TIME ZONE 'UTC') - $1::interval WHERE transaction_id = $2",
		age.String(), transactionID,
	)

	_, resetErr := conn.ExecContext(ctx, "SET session_replication_role = DEFAULT")
	require.NoError(t, err, "failed to backdate transaction")
	require.NoError(t, resetErr, "failed to re-enable triggers after backdate")
}

// skipWithoutTypesense gates tests that need a live Typesense instance.
func skipWithoutTypesense(t *testing.T) string {
	t.Helper()

	dns := os.Getenv("BLNK_TYPESENSE_DNS")
	if dns == "" {
		t.Skip("BLNK_TYPESENSE_DNS not set; skipping Typesense-dependent test")
	}
	return dns
}

// jsonReader wraps a JSON string literal for use as a request payload.
func jsonReader(s string) io.Reader {
	return strings.NewReader(s)
}

// resetReindexManager clears the package-level reindex singleton so progress
// tests are deterministic regardless of test order.
func resetReindexManager() {
	globalReindexManager.mu.Lock()
	globalReindexManager.service = nil
	globalReindexManager.mu.Unlock()
}

// assertErrorCode asserts the standard dual error payload: the response has
// the given status, error_detail carries the expected catalog code, and the
// legacy flat field is still present.
func assertErrorCode(t *testing.T, w *httptest.ResponseRecorder, status int, code apierror.ErrorCode) {
	t.Helper()

	require.Equal(t, status, w.Code)

	var body struct {
		Error       interface{}       `json:"error"`
		Errors      interface{}       `json:"errors"`
		ErrorDetail apierror.APIError `json:"error_detail"`
	}
	require.NoError(t, json.Unmarshal(w.Body.Bytes(), &body))
	require.Equal(t, code, body.ErrorDetail.Code)
	require.NotEmpty(t, body.ErrorDetail.Message)
	require.True(t, body.Error != nil || body.Errors != nil, "legacy error field must still be present")
}
