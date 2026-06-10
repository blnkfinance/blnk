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

package main

import (
	"strconv"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/require"
)

// realInfraConfig installs (via config.MockConfig) and returns a configuration
// pointing at the local test services, following the same convention as the
// api and root package suites: Postgres on :5432, Redis on :6379, and
// Typesense on :8108 are hard test dependencies.
func realInfraConfig(t *testing.T) *config.Configuration {
	t.Helper()
	cfg := &config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		Queue: config.QueueConfig{
			TransactionQueue:    "transaction_queue_cmd_test",
			NumberOfQueues:      1,
			WebhookQueue:        "webhook_queue_cmd_test",
			IndexQueue:          "index_queue_cmd_test",
			InflightExpiryQueue: "inflight_expiry_cmd_test",
			InflightCommitQueue: "inflight_commit_cmd_test",
			MonitoringPort:      "5117",
		},
		Transaction: config.TransactionConfig{
			BatchSize:    100,
			MaxQueueSize: 1000,
			MaxWorkers:   2,
			// MockConfig runs validateAndAddDefaults, which interprets a
			// non-zero LockDuration as SECONDS and multiplies by time.Second.
			LockDuration: 30,
		},
	}
	config.MockConfig(cfg)
	fetched, err := config.Fetch()
	require.NoError(t, err)
	return fetched
}

// newCmdTestInstance builds a blnkInstance backed by the real local Postgres
// and Redis, mirroring how preRun wires the production instance.
func newCmdTestInstance(t *testing.T) *blnkInstance {
	t.Helper()
	cfg := realInfraConfig(t)

	newBlnk, err := setupBlnk(cfg)
	require.NoError(t, err, "Postgres and Redis must be running for the cmd test suite")

	return &blnkInstance{blnk: newBlnk, cnf: cfg}
}

// createBalancePair creates two USD balances in the general ledger.
func createBalancePair(t *testing.T, b *blnkInstance) (*model.Balance, *model.Balance) {
	t.Helper()
	ds := b.blnk.GetDataSource()
	src, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
	require.NoError(t, err)
	dst, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: "general_ledger_id"})
	require.NoError(t, err)
	return &src, &dst
}

// queuedTransaction builds a QUEUED-status transaction payload exactly like
// the queue producer would put on the wire for processTransaction.
func queuedTransaction(src, dst string, amount float64, allowOverdraft bool) *model.Transaction {
	txn := &model.Transaction{
		TransactionID:  model.GenerateUUIDWithSuffix("txn"),
		Reference:      "ref_" + gofakeit.UUID(),
		Source:         src,
		Destination:    dst,
		Amount:         amount,
		Precision:      100,
		Currency:       "USD",
		AllowOverdraft: allowOverdraft,
		Status:         "QUEUED",
		CreatedAt:      time.Now(),
	}
	model.ApplyPrecision(txn)
	// The queue producer ships AmountString on the wire; the rejection path
	// persists it verbatim into the numeric amount column.
	txn.AmountString = strconv.FormatFloat(amount, 'f', -1, 64)
	return txn
}
