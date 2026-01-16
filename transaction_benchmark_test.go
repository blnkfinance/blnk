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
	"encoding/json"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/mock"
)

// setupBenchmarkConfig creates and stores a test configuration
func setupBenchmarkConfig() *config.Configuration {
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		Queue: config.QueueConfig{
			WebhookQueue:        "webhook_queue",
			TransactionQueue:    "transaction_queue",
			IndexQueue:          "index_queue",
			InflightExpiryQueue: "inflight_expiry_queue",
			NumberOfQueues:      1,
		},
		Server: config.ServerConfig{SecretKey: "benchmark-secret-key"},
		Transaction: config.TransactionConfig{
			BatchSize:        100000,
			MaxQueueSize:     1000,
			MaxWorkers:       10,
			LockDuration:     30 * time.Second,
			IndexQueuePrefix: "transactions",
		},
	}
	config.ConfigStore.Store(cnf)
	return cnf
}

// createTestBalance creates a test balance with the given ID and amount
func createTestBalance(id string, balance int64) *model.Balance {
	return &model.Balance{
		BalanceID:             id,
		Balance:               big.NewInt(balance),
		CreditBalance:         big.NewInt(balance),
		DebitBalance:          big.NewInt(0),
		InflightBalance:       big.NewInt(0),
		InflightCreditBalance: big.NewInt(0),
		InflightDebitBalance:  big.NewInt(0),
		Currency:              "USD",
		CurrencyMultiplier:    1,
		LedgerID:              "ledger-001",
		CreatedAt:             time.Now(),
		Version:               1,
	}
}

// createTestTransaction creates a test transaction
func createTestTransaction(source, destination, reference string, amount float64) *model.Transaction {
	return &model.Transaction{
		Reference:      reference,
		Source:         source,
		Destination:    destination,
		Amount:         amount,
		Precision:      100,
		Currency:       "USD",
		Rate:           1,
		AllowOverdraft: false,
	}
}

// setupMockDataSource creates a mock data source with preset responses for benchmarks
func setupMockDataSource() *mocks.MockDataSource {
	mockDS := new(mocks.MockDataSource)

	sourceBalance := createTestBalance("source-balance-001", 100000)
	destBalance := createTestBalance("dest-balance-001", 0)

	mockDS.On("TransactionExistsByRef", mock.Anything, mock.Anything).Return(false, nil)
	mockDS.On("GetBalanceByIDLite", "source-balance-001").Return(sourceBalance, nil)
	mockDS.On("GetBalanceByIDLite", "dest-balance-001").Return(destBalance, nil)
	mockDS.On("GetBalanceByID", mock.Anything, mock.Anything, mock.Anything).Return(sourceBalance, nil)
	mockDS.On("RecordTransactionWithBalances", mock.Anything, mock.Anything, mock.Anything, mock.Anything).
		Return(&model.Transaction{TransactionID: "txn-001", Status: "APPLIED"}, nil)
	mockDS.On("RecordTransaction", mock.Anything, mock.Anything).
		Return(&model.Transaction{TransactionID: "txn-001", Status: "QUEUED"}, nil)
	mockDS.On("GetBalanceMonitors", mock.Anything).Return([]model.BalanceMonitor{}, nil)

	return mockDS
}

// BenchmarkConfigFetch measures the overhead of calling config.Fetch() repeatedly
// This represents the OLD approach before our optimization
func BenchmarkConfigFetch(b *testing.B) {
	setupBenchmarkConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cfg, err := config.Fetch()
		if err != nil {
			b.Fatal(err)
		}
		_ = cfg.Transaction.LockDuration
	}
}

// BenchmarkConfigCached measures direct config access
// This represents the NEW approach after our optimization
func BenchmarkConfigCached(b *testing.B) {
	cfg := setupBenchmarkConfig()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = cfg.Transaction.LockDuration
	}
}

// BenchmarkTransactionMarshal measures JSON serialization overhead
func BenchmarkTransactionMarshal(b *testing.B) {
	txn := createTestTransaction("source-001", "dest-001", "ref-001", 100.00)
	txn.TransactionID = "txn-benchmark-001"
	txn.CreatedAt = time.Now()
	txn.MetaData = map[string]interface{}{
		"order_id":    "order-12345",
		"customer_id": "cust-67890",
		"description": "Payment for services",
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := json.Marshal(txn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkTransactionUnmarshal measures JSON deserialization overhead
func BenchmarkTransactionUnmarshal(b *testing.B) {
	txn := createTestTransaction("source-001", "dest-001", "ref-001", 100.00)
	txn.TransactionID = "txn-benchmark-001"
	txn.CreatedAt = time.Now()
	txn.MetaData = map[string]interface{}{
		"order_id":    "order-12345",
		"customer_id": "cust-67890",
		"description": "Payment for services",
	}

	data, _ := json.Marshal(txn)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var result model.Transaction
		err := json.Unmarshal(data, &result)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkApplyTransactionToBalances measures in-memory balance calculation
func BenchmarkApplyTransactionToBalances(b *testing.B) {
	setupBenchmarkConfig()

	txn := createTestTransaction("source-001", "dest-001", "ref-001", 100.00)
	txn.PreciseAmount = big.NewInt(10000)
	txn.Precision = 100

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		sourceBalance := createTestBalance("source-001", 100000)
		destBalance := createTestBalance("dest-001", 0)

		err := model.UpdateBalances(txn, sourceBalance, destBalance)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkApplyTransactionToBalancesParallel measures parallel balance calculations
func BenchmarkApplyTransactionToBalancesParallel(b *testing.B) {
	setupBenchmarkConfig()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			txn := createTestTransaction("source-001", "dest-001", "ref-001", 100.00)
			txn.PreciseAmount = big.NewInt(10000)
			txn.Precision = 100

			sourceBalance := createTestBalance("source-001", 100000)
			destBalance := createTestBalance("dest-001", 0)

			err := model.UpdateBalances(txn, sourceBalance, destBalance)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkValidateTxn measures transaction reference validation
func BenchmarkValidateTxn(b *testing.B) {
	setupBenchmarkConfig()
	mockDS := setupMockDataSource()

	blnkInstance := &Blnk{
		datasource: mockDS,
		config:     setupBenchmarkConfig(),
	}

	ctx := context.Background()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := createTestTransaction("source-001", "dest-001", fmt.Sprintf("ref-%d", i), 100.00)
		err := blnkInstance.validateTxn(ctx, txn)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkHashBalanceID measures the hash function used for queue assignment
func BenchmarkHashBalanceID(b *testing.B) {
	balanceID := "bln_abc123def456ghi789"

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hashBalanceID(balanceID)
	}
}

// BenchmarkHashBalanceIDVaried measures hashing with different balance IDs
func BenchmarkHashBalanceIDVaried(b *testing.B) {
	balanceIDs := make([]string, 1000)
	for i := 0; i < 1000; i++ {
		balanceIDs[i] = fmt.Sprintf("bln_%d", i)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = hashBalanceID(balanceIDs[i%1000])
	}
}

// BenchmarkTransactionHashTxn measures transaction hash generation
func BenchmarkTransactionHashTxn(b *testing.B) {
	txn := createTestTransaction("source-001", "dest-001", "ref-001", 100.00)
	txn.TransactionID = "txn-benchmark-001"
	txn.CreatedAt = time.Now()
	txn.PreciseAmount = big.NewInt(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = txn.HashTxn()
	}
}

// BenchmarkApplyPrecision measures precision application to transactions
func BenchmarkApplyPrecision(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := createTestTransaction("source-001", "dest-001", "ref-001", 100.50)
		txn.Precision = 100
		_ = model.ApplyPrecision(txn)
	}
}

// BenchmarkSetTransactionMetadata measures transaction metadata setup
func BenchmarkSetTransactionMetadata(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		txn := createTestTransaction("source-001", "dest-001", fmt.Sprintf("ref-%d", i), 100.00)
		setTransactionMetadata(txn)
	}
}

// BenchmarkBigIntOperations measures big.Int arithmetic used in balance calculations
func BenchmarkBigIntOperations(b *testing.B) {
	amount := big.NewInt(10000)
	balance := big.NewInt(100000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result := new(big.Int)
		result.Sub(balance, amount)
	}
}

// BenchmarkBigIntOperationsParallel measures parallel big.Int operations
func BenchmarkBigIntOperationsParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		amount := big.NewInt(10000)
		balance := big.NewInt(100000)

		for pb.Next() {
			result := new(big.Int)
			result.Sub(balance, amount)
		}
	})
}

// BenchmarkGenerateUUID measures UUID generation overhead
func BenchmarkGenerateUUID(b *testing.B) {
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = model.GenerateUUIDWithSuffix("txn")
	}
}

// BenchmarkGenerateUUIDParallel measures parallel UUID generation
func BenchmarkGenerateUUIDParallel(b *testing.B) {
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = model.GenerateUUIDWithSuffix("txn")
		}
	})
}

// BenchmarkIsInflightTransaction measures the inflight status check
func BenchmarkIsInflightTransaction(b *testing.B) {
	txnInflight := &model.Transaction{
		Status:   StatusInflight,
		MetaData: map[string]interface{}{"inflight": true},
	}
	txnQueued := &model.Transaction{
		Status:   StatusQueued,
		MetaData: map[string]interface{}{"inflight": true},
	}
	txnApplied := &model.Transaction{
		Status: StatusApplied,
	}

	transactions := []*model.Transaction{txnInflight, txnQueued, txnApplied}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = IsInflightTransaction(transactions[i%3])
	}
}

// BenchmarkCreateQueueCopy measures queue copy creation for split transactions
func BenchmarkCreateQueueCopy(b *testing.B) {
	txn := createTestTransaction("source-001", "dest-001", "ref-001", 100.00)
	txn.TransactionID = "txn-original-001"
	txn.CreatedAt = time.Now()
	txn.PreciseAmount = big.NewInt(10000)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = createQueueCopy(txn, "ref-original")
	}
}
