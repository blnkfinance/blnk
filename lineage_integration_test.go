package blnk

import (
	"context"
	"fmt"
	"math/big"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pollForLineageMappings polls the database until the expected number of lineage mappings appear
func pollForLineageMappings(ctx context.Context, ds database.IDataSource, balanceID string, expectedCount int, pollInterval, timeout time.Duration) ([]model.LineageMapping, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("timed out waiting for %d lineage mappings for balance %s: %w", expectedCount, balanceID, timeoutCtx.Err())
		case <-ticker.C:
			mappings, err := ds.GetLineageMappings(timeoutCtx, balanceID)
			if err != nil {
				continue
			}
			if len(mappings) >= expectedCount {
				return mappings, nil
			}
		}
	}
}

// pollForBalance polls until the balance reaches the expected value
func pollForBalance(ctx context.Context, ds database.IDataSource, balanceID string, expectedBalance *big.Int, pollInterval, timeout time.Duration) (*model.Balance, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("timed out waiting for balance %s to reach %s: %w", balanceID, expectedBalance.String(), timeoutCtx.Err())
		case <-ticker.C:
			balance, err := ds.GetBalanceByIDLite(balanceID)
			if err != nil {
				continue
			}
			if balance.Balance.Cmp(expectedBalance) == 0 {
				return balance, nil
			}
		}
	}
}

// pollForShadowCreditBalance polls until the shadow balance's CreditBalance reaches the expected value
func pollForShadowCreditBalance(ctx context.Context, ds database.IDataSource, balanceID string, expectedCredit *big.Int, pollInterval, timeout time.Duration) (*model.Balance, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastBalance *model.Balance
	for {
		select {
		case <-timeoutCtx.Done():
			if lastBalance != nil {
				return nil, fmt.Errorf("timed out waiting for shadow balance %s CreditBalance to reach %s (got %s): %w",
					balanceID, expectedCredit.String(), lastBalance.CreditBalance.String(), timeoutCtx.Err())
			}
			return nil, fmt.Errorf("timed out waiting for shadow balance %s CreditBalance to reach %s: %w",
				balanceID, expectedCredit.String(), timeoutCtx.Err())
		case <-ticker.C:
			balance, err := ds.GetBalanceByIDLite(balanceID)
			if err != nil {
				continue
			}
			lastBalance = balance
			if balance.CreditBalance.Cmp(expectedCredit) == 0 {
				return balance, nil
			}
		}
	}
}

// pollForAggregateDebitBalance polls until the aggregate balance's DebitBalance reaches the expected value
func pollForAggregateDebitBalance(ctx context.Context, ds database.IDataSource, balanceID string, expectedDebit *big.Int, pollInterval, timeout time.Duration) (*model.Balance, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	var lastBalance *model.Balance
	for {
		select {
		case <-timeoutCtx.Done():
			if lastBalance != nil {
				return nil, fmt.Errorf("timed out waiting for aggregate balance %s DebitBalance to reach %s (got %s): %w",
					balanceID, expectedDebit.String(), lastBalance.DebitBalance.String(), timeoutCtx.Err())
			}
			return nil, fmt.Errorf("timed out waiting for aggregate balance %s DebitBalance to reach %s: %w",
				balanceID, expectedDebit.String(), timeoutCtx.Err())
		case <-ticker.C:
			balance, err := ds.GetBalanceByIDLite(balanceID)
			if err != nil {
				continue
			}
			lastBalance = balance
			if balance.DebitBalance.Cmp(expectedDebit) == 0 {
				return balance, nil
			}
		}
	}
}

// pollForTransactionFundAllocation polls until the transaction has fund allocation metadata populated
func pollForTransactionFundAllocation(ctx context.Context, blnk *Blnk, transactionID string, expectedCount int, pollInterval, timeout time.Duration) (*TransactionLineage, error) {
	timeoutCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-timeoutCtx.Done():
			return nil, fmt.Errorf("timed out waiting for transaction %s to have %d fund allocations: %w",
				transactionID, expectedCount, timeoutCtx.Err())
		case <-ticker.C:
			lineage, err := blnk.GetTransactionLineage(ctx, transactionID)
			if err != nil {
				continue
			}
			if len(lineage.FundAllocation) >= expectedCount {
				return lineage, nil
			}
		}
	}
}

func TestLineageFullFlow(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping lineage integration test in short mode")
	}

	ctx := context.Background()
	pollInterval := 200 * time.Millisecond
	pollTimeout := 15 * time.Second

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_test",
			IndexQueue:       "index_queue_test",
			TransactionQueue: "transaction_queue_test",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{
			SecretKey: "test-secret",
		},
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	// Start the lineage outbox processor to process lineage entries asynchronously
	processor := NewLineageOutboxProcessor(blnk).WithPollInterval(100 * time.Millisecond)
	processor.Start(ctx)
	defer processor.Stop()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	identity, err := ds.CreateIdentity(model.Identity{
		FirstName: fmt.Sprintf("Alice_%s", suffix),
		LastName:  "Smith",
		Category:  "individual",
	})
	require.NoError(t, err, "Failed to create identity")
	t.Logf("Created identity: %s", identity.IdentityID)

	aliceBalance := model.Balance{
		Currency:           "USD",
		LedgerID:           GeneralLedgerID,
		IdentityID:         identity.IdentityID,
		TrackFundLineage:   true,
		AllocationStrategy: "FIFO",
	}
	createdAlice, err := ds.CreateBalance(aliceBalance)
	require.NoError(t, err, "Failed to create Alice's balance")
	t.Logf("Created Alice's balance: %s (lineage: %v, strategy: %s)",
		createdAlice.BalanceID, createdAlice.TrackFundLineage, createdAlice.AllocationStrategy)

	stripeTxn := &model.Transaction{
		Reference:      fmt.Sprintf("stripe_dep_%s", suffix),
		Source:         "@world",
		Destination:    createdAlice.BalanceID,
		Amount:         50,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		SkipQueue:      true,
		MetaData: map[string]interface{}{
			"BLNK_LINEAGE_PROVIDER": "stripe",
		},
	}

	queuedStripe, err := blnk.QueueTransaction(ctx, stripeTxn)
	require.NoError(t, err, "Failed to queue stripe transaction")
	t.Logf("Stripe deposit transaction: %s", queuedStripe.TransactionID)

	mappings, err := pollForLineageMappings(ctx, ds, createdAlice.BalanceID, 1, pollInterval, pollTimeout)
	require.NoError(t, err, "Failed to poll for stripe lineage mapping")
	assert.Equal(t, 1, len(mappings), "Should have 1 stripe lineage mapping")
	assert.Equal(t, "stripe", mappings[0].Provider, "Stripe lineage mapping provider should be stripe")
	t.Logf("Stripe lineage mapping created: %s", mappings[0].Provider)

	paypalTxn := &model.Transaction{
		Reference:      fmt.Sprintf("paypal_dep_%s", suffix),
		Source:         "@world",
		Destination:    createdAlice.BalanceID,
		Amount:         30,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		SkipQueue:      true,
		MetaData: map[string]interface{}{
			"BLNK_LINEAGE_PROVIDER": "paypal",
		},
	}

	queuedPaypal, err := blnk.QueueTransaction(ctx, paypalTxn)
	require.NoError(t, err, "Failed to queue paypal transaction")
	t.Logf("PayPal deposit transaction: %s", queuedPaypal.TransactionID)

	mappings, err = pollForLineageMappings(ctx, ds, createdAlice.BalanceID, 2, pollInterval, pollTimeout)
	require.NoError(t, err, "Failed to poll for paypal lineage mapping")
	assert.Equal(t, 2, len(mappings), "Should have 2 lineage mappings")
	assert.Equal(t, "paypal", mappings[1].Provider, "Paypal lineage mapping provider should be paypal")
	t.Logf("Lineage mappings count: %d", len(mappings))
	for _, m := range mappings {
		t.Logf("  Provider: %s, ShadowID: %s, AggregateID: %s", m.Provider, m.ShadowBalanceID, m.AggregateBalanceID)
	}

	expectedBalance := big.NewInt(8000)
	aliceAfterDeposits, err := pollForBalance(ctx, ds, createdAlice.BalanceID, expectedBalance, pollInterval, pollTimeout)
	require.NoError(t, err, "Failed to poll for Alice's balance")
	t.Logf("Alice's balance after deposits: %s", aliceAfterDeposits.Balance.String())

	merchantBalance := model.Balance{
		Currency: "USD",
		LedgerID: GeneralLedgerID,
	}
	createdMerchant, err := ds.CreateBalance(merchantBalance)
	require.NoError(t, err, "Failed to create merchant balance")
	t.Logf("Created merchant balance: %s", createdMerchant.BalanceID)

	paymentTxn := &model.Transaction{
		Reference:      fmt.Sprintf("payment_%s", suffix),
		Source:         createdAlice.BalanceID,
		Destination:    createdMerchant.BalanceID,
		Amount:         70,
		Currency:       "USD",
		AllowOverdraft: false,
		Precision:      100,
		SkipQueue:      true,
	}

	queuedPayment, err := blnk.QueueTransaction(ctx, paymentTxn)
	require.NoError(t, err, "Failed to queue payment transaction")
	t.Logf("Payment transaction: %s", queuedPayment.TransactionID)

	expectedAfterPayment := big.NewInt(1000)
	aliceAfterPayment, err := pollForBalance(ctx, ds, createdAlice.BalanceID, expectedAfterPayment, pollInterval, pollTimeout)
	require.NoError(t, err, "Failed to poll for Alice's balance after payment")
	t.Logf("Alice's balance after payment: %s", aliceAfterPayment.Balance.String())

	expectedMerchant := big.NewInt(7000)
	merchantAfterPayment, err := pollForBalance(ctx, ds, createdMerchant.BalanceID, expectedMerchant, pollInterval, pollTimeout)
	require.NoError(t, err, "Failed to poll for merchant balance")
	t.Logf("Merchant's balance: %s", merchantAfterPayment.Balance.String())

	// Wait for lineage debit processing to complete (processed asynchronously via outbox)
	_, err = pollForTransactionFundAllocation(ctx, blnk, queuedPayment.TransactionID, 2, pollInterval, pollTimeout)
	require.NoError(t, err, "Lineage debit processing should complete and populate fund allocation")

	lineage, err := blnk.GetBalanceLineage(ctx, createdAlice.BalanceID)
	require.NoError(t, err, "Failed to get balance lineage")

	assert.Equal(t, 0, lineage.TotalWithLineage.Cmp(big.NewInt(1000)),
		"TotalWithLineage should be 1000 ($10), got %s", lineage.TotalWithLineage.String())

	assert.Len(t, lineage.Providers, 2, "Should have 2 providers (stripe and paypal)")

	for _, p := range lineage.Providers {
		t.Logf("  Provider: %s, Amount: %s, Available: %s", p.Provider, p.Amount.String(), p.Available.String())
		switch p.Provider {
		case "stripe":
			assert.Equal(t, 0, p.Amount.Cmp(big.NewInt(5000)),
				"Stripe amount should be 5000 ($50), got %s", p.Amount.String())
			assert.Equal(t, 0, p.Available.Cmp(big.NewInt(0)),
				"Stripe available should be 0 (all spent), got %s", p.Available.String())
		case "paypal":
			assert.Equal(t, 0, p.Amount.Cmp(big.NewInt(3000)),
				"PayPal amount should be 3000 ($30), got %s", p.Amount.String())
			assert.Equal(t, 0, p.Available.Cmp(big.NewInt(1000)),
				"PayPal available should be 1000 ($10 remaining), got %s", p.Available.String())
		}
	}

	txnLineage, err := blnk.GetTransactionLineage(ctx, queuedPayment.TransactionID)
	require.NoError(t, err, "Failed to get transaction lineage")

	assert.Len(t, txnLineage.FundAllocation, 2, "Should have 2 fund allocations")

	assert.GreaterOrEqual(t, len(txnLineage.ShadowTransactions), 2,
		"Should have at least 2 shadow transactions (release from stripe and paypal)")

	t.Logf("Transaction lineage: FundAllocation=%d entries, ShadowTxns=%d",
		len(txnLineage.FundAllocation), len(txnLineage.ShadowTransactions))
	for _, alloc := range txnLineage.FundAllocation {
		t.Logf("  Allocation: %v", alloc)
	}

	t.Log("=== Lineage full flow test completed ===")
}

func TestLineageToLineageTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping lineage integration test in short mode")
	}

	ctx := context.Background()
	pollInterval := 200 * time.Millisecond
	pollTimeout := 15 * time.Second

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_test",
			IndexQueue:       "index_queue_test",
			TransactionQueue: "transaction_queue_test",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{
			SecretKey: "test-secret",
		},
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	// Start the lineage outbox processor to process lineage entries asynchronously
	processor := NewLineageOutboxProcessor(blnk).WithPollInterval(100 * time.Millisecond)
	processor.Start(ctx)
	defer processor.Stop()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	senderIdentity, err := ds.CreateIdentity(model.Identity{
		FirstName: fmt.Sprintf("Sender_%s", suffix),
		LastName:  "User",
		Category:  "individual",
	})
	require.NoError(t, err)

	receiverIdentity, err := ds.CreateIdentity(model.Identity{
		FirstName: fmt.Sprintf("Receiver_%s", suffix),
		LastName:  "User",
		Category:  "individual",
	})
	require.NoError(t, err)

	senderBalance, err := ds.CreateBalance(model.Balance{
		Currency:           "USD",
		LedgerID:           GeneralLedgerID,
		IdentityID:         senderIdentity.IdentityID,
		TrackFundLineage:   true,
		AllocationStrategy: "FIFO",
	})
	require.NoError(t, err)
	t.Logf("Sender balance: %s", senderBalance.BalanceID)

	receiverBalance, err := ds.CreateBalance(model.Balance{
		Currency:           "USD",
		LedgerID:           GeneralLedgerID,
		IdentityID:         receiverIdentity.IdentityID,
		TrackFundLineage:   true,
		AllocationStrategy: "FIFO",
	})
	require.NoError(t, err)
	t.Logf("Receiver balance: %s", receiverBalance.BalanceID)

	_, err = blnk.QueueTransaction(ctx, &model.Transaction{
		Reference:      fmt.Sprintf("stripe_%s", suffix),
		Source:         "@world",
		Destination:    senderBalance.BalanceID,
		Amount:         100,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		SkipQueue:      true,
		MetaData:       map[string]interface{}{"BLNK_LINEAGE_PROVIDER": "stripe"},
	})
	require.NoError(t, err)

	_, err = pollForLineageMappings(ctx, ds, senderBalance.BalanceID, 1, pollInterval, pollTimeout)
	require.NoError(t, err)

	_, err = blnk.QueueTransaction(ctx, &model.Transaction{
		Reference:      fmt.Sprintf("bank_%s", suffix),
		Source:         "@world",
		Destination:    senderBalance.BalanceID,
		Amount:         50,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		SkipQueue:      true,
		MetaData:       map[string]interface{}{"BLNK_LINEAGE_PROVIDER": "bank"},
	})
	require.NoError(t, err)

	_, err = pollForLineageMappings(ctx, ds, senderBalance.BalanceID, 2, pollInterval, pollTimeout)
	require.NoError(t, err)

	senderAfterDeposit, err := pollForBalance(ctx, ds, senderBalance.BalanceID, big.NewInt(15000), pollInterval, pollTimeout)
	require.NoError(t, err)
	t.Logf("Sender balance: %s", senderAfterDeposit.Balance.String())

	transferTxn, err := blnk.QueueTransaction(ctx, &model.Transaction{
		Reference:      fmt.Sprintf("transfer_%s", suffix),
		Source:         senderBalance.BalanceID,
		Destination:    receiverBalance.BalanceID,
		Amount:         80,
		Currency:       "USD",
		AllowOverdraft: false,
		Precision:      100,
		SkipQueue:      true,
	})
	require.NoError(t, err)
	t.Logf("Transfer transaction: %s", transferTxn.TransactionID)

	senderAfterTransfer, err := pollForBalance(ctx, ds, senderBalance.BalanceID, big.NewInt(7000), pollInterval, pollTimeout)
	require.NoError(t, err)
	t.Logf("Sender balance after transfer: %s", senderAfterTransfer.Balance.String())

	receiverAfterTransfer, err := pollForBalance(ctx, ds, receiverBalance.BalanceID, big.NewInt(8000), pollInterval, pollTimeout)
	require.NoError(t, err)
	t.Logf("Receiver balance after transfer: %s", receiverAfterTransfer.Balance.String())

	// Wait for lineage outbox processing to complete by polling for fund allocation
	_, err = pollForTransactionFundAllocation(ctx, blnk, transferTxn.TransactionID, 1, pollInterval, pollTimeout)
	require.NoError(t, err, "Lineage debit processing should complete and populate fund allocation")

	senderLineage, err := blnk.GetBalanceLineage(ctx, senderBalance.BalanceID)
	require.NoError(t, err)
	t.Logf("Sender lineage after transfer:")
	for _, p := range senderLineage.Providers {
		t.Logf("  %s: Amount=%s, Available=%s", p.Provider, p.Amount.String(), p.Available.String())
	}

	assert.Equal(t, 0, senderLineage.TotalWithLineage.Cmp(big.NewInt(7000)),
		"Sender TotalWithLineage should be 7000 ($70), got %s", senderLineage.TotalWithLineage.String())

	assert.Len(t, senderLineage.Providers, 2, "Sender should have 2 providers (stripe and bank)")

	for _, p := range senderLineage.Providers {
		switch p.Provider {
		case "stripe":
			assert.Equal(t, 0, p.Amount.Cmp(big.NewInt(10000)),
				"Sender stripe amount should be 10000 ($100 deposited), got %s", p.Amount.String())
			assert.Equal(t, 0, p.Available.Cmp(big.NewInt(2000)),
				"Sender stripe available should be 2000 ($20 left after $80 transfer), got %s", p.Available.String())
		case "bank":
			assert.Equal(t, 0, p.Amount.Cmp(big.NewInt(5000)),
				"Sender bank amount should be 5000 ($50 deposited), got %s", p.Amount.String())
			assert.Equal(t, 0, p.Available.Cmp(big.NewInt(5000)),
				"Sender bank available should be 5000 ($50, not touched by FIFO), got %s", p.Available.String())
		}
	}

	receiverLineage, err := blnk.GetBalanceLineage(ctx, receiverBalance.BalanceID)
	require.NoError(t, err)
	t.Logf("Receiver lineage after transfer:")
	for _, p := range receiverLineage.Providers {
		t.Logf("  %s: Amount=%s, Available=%s", p.Provider, p.Amount.String(), p.Available.String())
	}

	assert.Equal(t, 0, receiverLineage.TotalWithLineage.Cmp(big.NewInt(8000)),
		"Receiver TotalWithLineage should be 8000 ($80), got %s", receiverLineage.TotalWithLineage.String())

	assert.Len(t, receiverLineage.Providers, 1, "Receiver should have 1 provider (stripe only from FIFO)")

	if len(receiverLineage.Providers) > 0 {
		stripeProvider := receiverLineage.Providers[0]
		assert.Equal(t, "stripe", stripeProvider.Provider, "Receiver provider should be stripe")
		assert.Equal(t, 0, stripeProvider.Amount.Cmp(big.NewInt(8000)),
			"Receiver stripe amount should be 8000 ($80), got %s", stripeProvider.Amount.String())
		assert.Equal(t, 0, stripeProvider.Available.Cmp(big.NewInt(8000)),
			"Receiver stripe available should be 8000 ($80), got %s", stripeProvider.Available.String())
	}

	txnLineage, err := blnk.GetTransactionLineage(ctx, transferTxn.TransactionID)
	require.NoError(t, err)

	assert.Len(t, txnLineage.FundAllocation, 1, "Should have 1 fund allocation (stripe only)")
	if len(txnLineage.FundAllocation) > 0 {
		alloc := txnLineage.FundAllocation[0]
		assert.Equal(t, "stripe", alloc["provider"], "Fund allocation provider should be stripe")
		switch amt := alloc["amount"].(type) {
		case *big.Int:
			assert.Equal(t, 0, amt.Cmp(big.NewInt(8000)), "Fund allocation amount should be 8000 (precise, $80)")
		case float64:
			assert.Equal(t, float64(8000), amt, "Fund allocation amount should be 8000 (precise, $80)")
		default:
			t.Errorf("Unexpected amount type: %T", alloc["amount"])
		}
	}

	assert.GreaterOrEqual(t, len(txnLineage.ShadowTransactions), 1,
		"Should have at least 1 shadow transaction for the transfer")

	t.Log("=== Lineage to lineage transfer test completed ===")
}

func TestLineageAllocationStrategies(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping lineage integration test in short mode")
	}

	ctx := context.Background()
	pollInterval := 200 * time.Millisecond
	pollTimeout := 15 * time.Second

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_test",
			IndexQueue:       "index_queue_test",
			TransactionQueue: "transaction_queue_test",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{
			SecretKey: "test-secret",
		},
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	// Start the lineage outbox processor to process lineage entries asynchronously
	processor := NewLineageOutboxProcessor(blnk).WithPollInterval(100 * time.Millisecond)
	processor.Start(ctx)
	defer processor.Stop()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	t.Run("LIFO Allocation", func(t *testing.T) {
		identity, err := ds.CreateIdentity(model.Identity{
			FirstName: fmt.Sprintf("LIFO_%s", suffix),
			LastName:  "User",
			Category:  "individual",
		})
		require.NoError(t, err)

		balance, err := ds.CreateBalance(model.Balance{
			Currency:           "USD",
			LedgerID:           GeneralLedgerID,
			IdentityID:         identity.IdentityID,
			TrackFundLineage:   true,
			AllocationStrategy: "LIFO",
		})
		require.NoError(t, err)

		_, err = blnk.QueueTransaction(ctx, &model.Transaction{
			Reference:      fmt.Sprintf("lifo_stripe_%s", suffix),
			Source:         "@world",
			Destination:    balance.BalanceID,
			Amount:         40,
			Currency:       "USD",
			AllowOverdraft: true,
			Precision:      100,
			SkipQueue:      true,
			MetaData:       map[string]interface{}{"BLNK_LINEAGE_PROVIDER": "stripe"},
		})
		require.NoError(t, err)

		_, err = pollForLineageMappings(ctx, ds, balance.BalanceID, 1, pollInterval, pollTimeout)
		require.NoError(t, err)

		_, err = blnk.QueueTransaction(ctx, &model.Transaction{
			Reference:      fmt.Sprintf("lifo_bank_%s", suffix),
			Source:         "@world",
			Destination:    balance.BalanceID,
			Amount:         30,
			Currency:       "USD",
			AllowOverdraft: true,
			Precision:      100,
			SkipQueue:      true,
			MetaData:       map[string]interface{}{"BLNK_LINEAGE_PROVIDER": "bank"},
		})
		require.NoError(t, err)

		_, err = pollForLineageMappings(ctx, ds, balance.BalanceID, 2, pollInterval, pollTimeout)
		require.NoError(t, err)

		_, err = pollForBalance(ctx, ds, balance.BalanceID, big.NewInt(7000), pollInterval, pollTimeout)
		require.NoError(t, err)

		merchant, err := ds.CreateBalance(model.Balance{
			Currency: "USD",
			LedgerID: GeneralLedgerID,
		})
		require.NoError(t, err)

		txn, err := blnk.QueueTransaction(ctx, &model.Transaction{
			Reference:      fmt.Sprintf("lifo_spend_%s", suffix),
			Source:         balance.BalanceID,
			Destination:    merchant.BalanceID,
			Amount:         25,
			Currency:       "USD",
			AllowOverdraft: false,
			Precision:      100,
			SkipQueue:      true,
		})
		require.NoError(t, err)

		_, err = pollForBalance(ctx, ds, balance.BalanceID, big.NewInt(4500), pollInterval, pollTimeout)
		require.NoError(t, err)

		// Poll for fund allocation (debit lineage is processed asynchronously)
		lineage, err := pollForTransactionFundAllocation(ctx, blnk, txn.TransactionID, 1, pollInterval, pollTimeout)
		require.NoError(t, err, "Failed to poll for transaction fund allocation")
		t.Logf("LIFO allocation for $25 spend:")
		for _, alloc := range lineage.FundAllocation {
			t.Logf("  %v", alloc)
		}

		assert.Len(t, lineage.FundAllocation, 1, "LIFO should use only 1 provider for $25 (bank has $30)")
		if len(lineage.FundAllocation) > 0 {
			alloc := lineage.FundAllocation[0]
			assert.Equal(t, "bank", alloc["provider"], "LIFO should use bank (newest) first")
			switch amt := alloc["amount"].(type) {
			case *big.Int:
				assert.Equal(t, 0, amt.Cmp(big.NewInt(2500)), "Should allocate 2500 (precise, $25) from bank")
			case float64:
				assert.Equal(t, float64(2500), amt, "Should allocate 2500 (precise, $25) from bank")
			}
		}

		balLineage, err := blnk.GetBalanceLineage(ctx, balance.BalanceID)
		require.NoError(t, err)
		t.Logf("LIFO remaining balance breakdown:")
		for _, p := range balLineage.Providers {
			t.Logf("  Remaining %s: %s", p.Provider, p.Available.String())
		}

		assert.Equal(t, 0, balLineage.TotalWithLineage.Cmp(big.NewInt(4500)),
			"Total should be 4500 ($45), got %s", balLineage.TotalWithLineage.String())
		assert.Len(t, balLineage.Providers, 2, "Should still have 2 providers")

		for _, p := range balLineage.Providers {
			switch p.Provider {
			case "stripe":
				assert.Equal(t, 0, p.Amount.Cmp(big.NewInt(4000)),
					"Stripe amount should be 4000 ($40), got %s", p.Amount.String())
				assert.Equal(t, 0, p.Available.Cmp(big.NewInt(4000)),
					"Stripe available should be 4000 ($40 untouched), got %s", p.Available.String())
			case "bank":
				assert.Equal(t, 0, p.Amount.Cmp(big.NewInt(3000)),
					"Bank amount should be 3000 ($30), got %s", p.Amount.String())
				assert.Equal(t, 0, p.Available.Cmp(big.NewInt(500)),
					"Bank available should be 500 ($5 left), got %s", p.Available.String())
			}
		}
	})

	t.Run("PROPORTIONAL Allocation", func(t *testing.T) {
		identity, err := ds.CreateIdentity(model.Identity{
			FirstName: fmt.Sprintf("PROP_%s", suffix),
			LastName:  "User",
			Category:  "individual",
		})
		require.NoError(t, err)

		balance, err := ds.CreateBalance(model.Balance{
			Currency:           "USD",
			LedgerID:           GeneralLedgerID,
			IdentityID:         identity.IdentityID,
			TrackFundLineage:   true,
			AllocationStrategy: "PROPORTIONAL",
		})
		require.NoError(t, err)

		_, err = blnk.QueueTransaction(ctx, &model.Transaction{
			Reference:      fmt.Sprintf("prop_stripe_%s", suffix),
			Source:         "@world",
			Destination:    balance.BalanceID,
			Amount:         60,
			Currency:       "USD",
			AllowOverdraft: true,
			Precision:      100,
			SkipQueue:      true,
			MetaData:       map[string]interface{}{"BLNK_LINEAGE_PROVIDER": "stripe"},
		})
		require.NoError(t, err)

		_, err = pollForLineageMappings(ctx, ds, balance.BalanceID, 1, pollInterval, pollTimeout)
		require.NoError(t, err)

		_, err = blnk.QueueTransaction(ctx, &model.Transaction{
			Reference:      fmt.Sprintf("prop_paypal_%s", suffix),
			Source:         "@world",
			Destination:    balance.BalanceID,
			Amount:         40,
			Currency:       "USD",
			AllowOverdraft: true,
			Precision:      100,
			SkipQueue:      true,
			MetaData:       map[string]interface{}{"BLNK_LINEAGE_PROVIDER": "paypal"},
		})
		require.NoError(t, err)

		_, err = pollForLineageMappings(ctx, ds, balance.BalanceID, 2, pollInterval, pollTimeout)
		require.NoError(t, err)

		_, err = pollForBalance(ctx, ds, balance.BalanceID, big.NewInt(10000), pollInterval, pollTimeout)
		require.NoError(t, err)

		merchant, err := ds.CreateBalance(model.Balance{
			Currency: "USD",
			LedgerID: GeneralLedgerID,
		})
		require.NoError(t, err)

		txn, err := blnk.QueueTransaction(ctx, &model.Transaction{
			Reference:      fmt.Sprintf("prop_spend_%s", suffix),
			Source:         balance.BalanceID,
			Destination:    merchant.BalanceID,
			Amount:         50,
			Currency:       "USD",
			AllowOverdraft: false,
			Precision:      100,
			SkipQueue:      true,
		})
		require.NoError(t, err)

		_, err = pollForBalance(ctx, ds, balance.BalanceID, big.NewInt(5000), pollInterval, pollTimeout)
		require.NoError(t, err)

		// Poll for fund allocation (debit lineage is processed asynchronously)
		lineage, err := pollForTransactionFundAllocation(ctx, blnk, txn.TransactionID, 2, pollInterval, pollTimeout)
		require.NoError(t, err, "Failed to poll for transaction fund allocation")
		t.Logf("PROPORTIONAL allocation for $50 spend:")
		for _, alloc := range lineage.FundAllocation {
			t.Logf("  %v", alloc)
		}

		assert.Len(t, lineage.FundAllocation, 2, "PROPORTIONAL should use both providers")

		stripeAlloc := int64(0)
		paypalAlloc := int64(0)
		for _, alloc := range lineage.FundAllocation {
			provider := alloc["provider"].(string)
			var amount int64
			switch amt := alloc["amount"].(type) {
			case *big.Int:
				amount = amt.Int64()
			case float64:
				amount = int64(amt)
			}
			switch provider {
			case "stripe":
				stripeAlloc = amount
			case "paypal":
				paypalAlloc = amount
			}
		}
		assert.Equal(t, int64(3000), stripeAlloc, "Stripe should contribute 3000 (precise, $30 = 60%% of $50)")
		assert.Equal(t, int64(2000), paypalAlloc, "Paypal should contribute 2000 (precise, $20 = 40%% of $50)")

		balLineage, err := blnk.GetBalanceLineage(ctx, balance.BalanceID)
		require.NoError(t, err)
		t.Logf("PROPORTIONAL remaining balance breakdown:")
		for _, p := range balLineage.Providers {
			t.Logf("  %s: %s available", p.Provider, p.Available.String())
		}

		assert.Equal(t, 0, balLineage.TotalWithLineage.Cmp(big.NewInt(5000)),
			"Total should be 5000 ($50), got %s", balLineage.TotalWithLineage.String())
		assert.Len(t, balLineage.Providers, 2, "Should have 2 providers")

		for _, p := range balLineage.Providers {
			switch p.Provider {
			case "stripe":
				assert.Equal(t, 0, p.Amount.Cmp(big.NewInt(6000)),
					"Stripe amount should be 6000 ($60), got %s", p.Amount.String())
				assert.Equal(t, 0, p.Available.Cmp(big.NewInt(3000)),
					"Stripe available should be 3000 ($30 left), got %s", p.Available.String())
			case "paypal":
				assert.Equal(t, 0, p.Amount.Cmp(big.NewInt(4000)),
					"Paypal amount should be 4000 ($40), got %s", p.Amount.String())
				assert.Equal(t, 0, p.Available.Cmp(big.NewInt(2000)),
					"Paypal available should be 2000 ($20 left), got %s", p.Available.String())
			}
		}
	})

	t.Log("=== Allocation strategy tests completed ===")
}

func TestLineageInflightHold(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping lineage integration test in short mode")
	}

	ctx := context.Background()

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_test",
			IndexQueue:       "index_queue_test",
			TransactionQueue: "transaction_queue_test",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{
			SecretKey: "test-secret",
		},
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	// Start the lineage outbox processor to process lineage entries asynchronously
	processor := NewLineageOutboxProcessor(blnk).WithPollInterval(100 * time.Millisecond)
	processor.Start(ctx)
	defer processor.Stop()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	identity, err := ds.CreateIdentity(model.Identity{
		FirstName: fmt.Sprintf("Inflight_%s", suffix),
		LastName:  "Test",
		Category:  "individual",
	})
	require.NoError(t, err)

	balance, err := ds.CreateBalance(model.Balance{
		Currency:           "USD",
		LedgerID:           GeneralLedgerID,
		IdentityID:         identity.IdentityID,
		TrackFundLineage:   true,
		AllocationStrategy: "FIFO",
	})
	require.NoError(t, err)
	t.Logf("Created balance: %s", balance.BalanceID)

	inflightTxn, err := blnk.QueueTransaction(ctx, &model.Transaction{
		Reference:      fmt.Sprintf("inflight_hold_%s", suffix),
		Source:         "@world",
		Destination:    balance.BalanceID,
		Amount:         100,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		Inflight:       true,
		SkipQueue:      true,
		MetaData:       map[string]interface{}{"BLNK_LINEAGE_PROVIDER": "stripe"},
	})
	require.NoError(t, err)
	t.Logf("Created inflight transaction: %s (status: %s)", inflightTxn.TransactionID, inflightTxn.Status)

	require.Equal(t, StatusInflight, inflightTxn.Status, "Main transaction should be INFLIGHT")

	// Poll for lineage mapping (processed asynchronously by outbox worker)
	mappings, err := pollForLineageMappings(ctx, ds, balance.BalanceID, 1, 100*time.Millisecond, 10*time.Second)
	require.NoError(t, err, "Failed to poll for lineage mappings")
	require.Len(t, mappings, 1, "Should have 1 lineage mapping")
	assert.Equal(t, "stripe", mappings[0].Provider)
	t.Logf("Lineage mapping created: provider=%s, shadow=%s", mappings[0].Provider, mappings[0].ShadowBalanceID)

	// Shadow transactions should exist immediately
	shadowTxns, err := ds.GetTransactionsByShadowFor(ctx, inflightTxn.TransactionID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(shadowTxns), 1, "Should have at least 1 shadow transaction")

	for _, shadow := range shadowTxns {
		t.Logf("Shadow transaction: %s (status: %s)", shadow.TransactionID, shadow.Status)
		assert.Equal(t, StatusInflight, shadow.Status,
			"Shadow transaction should also be INFLIGHT, got %s", shadow.Status)
	}

	// Main balance should have inflight credit
	mainBalance, err := ds.GetBalanceByIDLite(balance.BalanceID)
	require.NoError(t, err)
	t.Logf("Main balance: Balance=%s, InflightBalance=%s, InflightCreditBalance=%s",
		mainBalance.Balance.String(), mainBalance.InflightBalance.String(), mainBalance.InflightCreditBalance.String())
	assert.Equal(t, 0, mainBalance.Balance.Cmp(big.NewInt(0)),
		"Main balance should be 0 (not committed yet)")
	assert.Equal(t, 0, mainBalance.InflightBalance.Cmp(big.NewInt(10000)),
		"Main inflight balance should be 10000")
	assert.Equal(t, 0, mainBalance.InflightCreditBalance.Cmp(big.NewInt(10000)),
		"Main inflight credit balance should be 10000")

	// Shadow balance should have inflight debit
	shadowBalance, err := ds.GetBalanceByIDLite(mappings[0].ShadowBalanceID)
	require.NoError(t, err)
	t.Logf("Shadow balance: Balance=%s, InflightDebitBalance=%s",
		shadowBalance.Balance.String(), shadowBalance.InflightDebitBalance.String())
	assert.Equal(t, 0, shadowBalance.Balance.Cmp(big.NewInt(0)),
		"Shadow balance should be 0 (not committed yet)")
	assert.Equal(t, 0, shadowBalance.InflightDebitBalance.Cmp(big.NewInt(10000)),
		"Shadow inflight debit balance should be 10000")

	// Aggregate balance should have inflight credit
	aggregateBalance, err := ds.GetBalanceByIDLite(mappings[0].AggregateBalanceID)
	require.NoError(t, err)
	t.Logf("Aggregate balance: Balance=%s, InflightCreditBalance=%s",
		aggregateBalance.Balance.String(), aggregateBalance.InflightCreditBalance.String())
	assert.Equal(t, 0, aggregateBalance.Balance.Cmp(big.NewInt(0)),
		"Aggregate balance should be 0 (not committed yet)")
	assert.Equal(t, 0, aggregateBalance.InflightCreditBalance.Cmp(big.NewInt(10000)),
		"Aggregate inflight credit balance should be 10000")

	t.Log("=== Inflight hold test completed ===")
}

func TestLineageInflightCommit(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping lineage integration test in short mode")
	}

	ctx := context.Background()

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_test",
			IndexQueue:       "index_queue_test",
			TransactionQueue: "transaction_queue_test",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{
			SecretKey: "test-secret",
		},
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	// Start the lineage outbox processor to process lineage entries asynchronously
	processor := NewLineageOutboxProcessor(blnk).WithPollInterval(100 * time.Millisecond)
	processor.Start(ctx)
	defer processor.Stop()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	identity, err := ds.CreateIdentity(model.Identity{
		FirstName: fmt.Sprintf("Commit_%s", suffix),
		LastName:  "Test",
		Category:  "individual",
	})
	require.NoError(t, err)

	balance, err := ds.CreateBalance(model.Balance{
		Currency:           "USD",
		LedgerID:           GeneralLedgerID,
		IdentityID:         identity.IdentityID,
		TrackFundLineage:   true,
		AllocationStrategy: "FIFO",
	})
	require.NoError(t, err)

	inflightTxn, err := blnk.QueueTransaction(ctx, &model.Transaction{
		Reference:      fmt.Sprintf("inflight_commit_%s", suffix),
		Source:         "@world",
		Destination:    balance.BalanceID,
		Amount:         50,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		Inflight:       true,
		SkipQueue:      true,
		MetaData:       map[string]interface{}{"BLNK_LINEAGE_PROVIDER": "paypal"},
	})
	require.NoError(t, err)
	t.Logf("Created inflight transaction: %s (status: %s)", inflightTxn.TransactionID, inflightTxn.Status)
	require.Equal(t, StatusInflight, inflightTxn.Status, "Transaction should be INFLIGHT")

	// Poll for lineage mapping (processed asynchronously by outbox worker)
	mappings, err := pollForLineageMappings(ctx, ds, balance.BalanceID, 1, 100*time.Millisecond, 10*time.Second)
	require.NoError(t, err, "Failed to poll for lineage mappings")
	require.Len(t, mappings, 1, "Should have 1 lineage mapping")

	// Shadow transactions exist immediately
	shadowTxnsBefore, err := ds.GetTransactionsByShadowFor(ctx, inflightTxn.TransactionID)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(shadowTxnsBefore), 1, "Should have shadow transactions")

	for _, s := range shadowTxnsBefore {
		t.Logf("Shadow transaction before commit: %s (status: %s)", s.TransactionID, s.Status)
		assert.Equal(t, StatusInflight, s.Status, "Shadow should be INFLIGHT before commit")
	}

	// Commit the main transaction
	commitTxn, err := blnk.CommitInflightTransaction(ctx, inflightTxn.TransactionID, big.NewInt(5000))
	require.NoError(t, err)
	t.Logf("Committed main transaction: %s (parent: %s)", commitTxn.TransactionID, commitTxn.ParentTransaction)
	require.Equal(t, StatusApplied, commitTxn.Status, "Committed transaction should have APPLIED status")

	// Main balance should be committed immediately
	mainBalance, err := ds.GetBalanceByIDLite(balance.BalanceID)
	require.NoError(t, err)
	t.Logf("Main balance after commit: Balance=%s, InflightBalance=%s",
		mainBalance.Balance.String(), mainBalance.InflightBalance.String())
	assert.Equal(t, 0, mainBalance.Balance.Cmp(big.NewInt(5000)),
		"Main balance should be 5000 after commit")
	assert.Equal(t, 0, mainBalance.InflightBalance.Cmp(big.NewInt(0)),
		"Main inflight balance should be 0 after commit")

	// Shadow balance should be committed (debit balance non-zero, inflight zero)
	shadowBalance, err := ds.GetBalanceByIDLite(mappings[0].ShadowBalanceID)
	require.NoError(t, err)
	t.Logf("Shadow balance after commit: DebitBalance=%s, InflightDebitBalance=%s",
		shadowBalance.DebitBalance.String(), shadowBalance.InflightDebitBalance.String())
	assert.Equal(t, 0, shadowBalance.DebitBalance.Cmp(big.NewInt(5000)),
		"Shadow debit balance should be 5000 after commit")

	// Aggregate balance should be committed
	aggregateBalance, err := ds.GetBalanceByIDLite(mappings[0].AggregateBalanceID)
	require.NoError(t, err)
	t.Logf("Aggregate balance after commit: CreditBalance=%s, InflightCreditBalance=%s",
		aggregateBalance.CreditBalance.String(), aggregateBalance.InflightCreditBalance.String())
	assert.Equal(t, 0, aggregateBalance.CreditBalance.Cmp(big.NewInt(5000)),
		"Aggregate credit balance should be 5000 after commit")

	t.Log("=== Phase 2: Spend to merchant ===")

	// Create a merchant balance (non-lineage)
	merchantBalance, err := ds.CreateBalance(model.Balance{
		Currency:         "USD",
		LedgerID:         GeneralLedgerID,
		TrackFundLineage: false,
	})
	require.NoError(t, err)
	t.Logf("Created merchant balance: %s", merchantBalance.BalanceID)

	// Spend $30 (3000 precise) from main balance to merchant
	spendTxn, err := blnk.QueueTransaction(ctx, &model.Transaction{
		Reference:   fmt.Sprintf("spend_to_merchant_%s", suffix),
		Source:      balance.BalanceID,
		Destination: merchantBalance.BalanceID,
		Amount:      30,
		Currency:    "USD",
		Precision:   100,
		SkipQueue:   true,
	})
	require.NoError(t, err)
	t.Logf("Spend transaction: %s (status: %s)", spendTxn.TransactionID, spendTxn.Status)
	require.Equal(t, StatusApplied, spendTxn.Status, "Spend transaction should be APPLIED")

	// Check main balance after spend
	mainBalanceAfterSpend, err := ds.GetBalanceByIDLite(balance.BalanceID)
	require.NoError(t, err)
	t.Logf("Main balance after spend: Balance=%s", mainBalanceAfterSpend.Balance.String())
	assert.Equal(t, 0, mainBalanceAfterSpend.Balance.Cmp(big.NewInt(2000)),
		"Main balance should be 2000 after spending 3000")

	// Check merchant balance received the funds
	merchantBalanceAfter, err := ds.GetBalanceByIDLite(merchantBalance.BalanceID)
	require.NoError(t, err)
	t.Logf("Merchant balance after spend: Balance=%s", merchantBalanceAfter.Balance.String())
	assert.Equal(t, 0, merchantBalanceAfter.Balance.Cmp(big.NewInt(3000)),
		"Merchant balance should be 3000 after receiving spend")

	// Poll for shadow balance after spend - debit lineage is processed asynchronously
	// Shadow: DebitBalance=5000 (allocated), CreditBalance=3000 (released)
	// Remaining from provider = DebitBalance - CreditBalance = 2000
	shadowBalanceAfterSpend, err := pollForShadowCreditBalance(ctx, ds, mappings[0].ShadowBalanceID, big.NewInt(3000), 100*time.Millisecond, 10*time.Second)
	require.NoError(t, err, "Failed to poll for shadow credit balance update")
	t.Logf("Shadow balance after spend: Balance=%s, DebitBalance=%s, CreditBalance=%s",
		shadowBalanceAfterSpend.Balance.String(),
		shadowBalanceAfterSpend.DebitBalance.String(),
		shadowBalanceAfterSpend.CreditBalance.String())
	assert.Equal(t, 0, shadowBalanceAfterSpend.DebitBalance.Cmp(big.NewInt(5000)),
		"Shadow debit balance should still be 5000 (total allocated)")
	assert.Equal(t, 0, shadowBalanceAfterSpend.CreditBalance.Cmp(big.NewInt(3000)),
		"Shadow credit balance should be 3000 (released/spent)")

	// Poll for aggregate balance after spend - debit lineage is processed asynchronously
	// Aggregate mirrors shadow: CreditBalance=5000 (received), DebitBalance=3000 (released)
	aggregateBalanceAfterSpend, err := pollForAggregateDebitBalance(ctx, ds, mappings[0].AggregateBalanceID, big.NewInt(3000), 100*time.Millisecond, 10*time.Second)
	require.NoError(t, err, "Failed to poll for aggregate debit balance update")
	t.Logf("Aggregate balance after spend: Balance=%s, CreditBalance=%s, DebitBalance=%s",
		aggregateBalanceAfterSpend.Balance.String(),
		aggregateBalanceAfterSpend.CreditBalance.String(),
		aggregateBalanceAfterSpend.DebitBalance.String())
	assert.Equal(t, 0, aggregateBalanceAfterSpend.CreditBalance.Cmp(big.NewInt(5000)),
		"Aggregate credit balance should still be 5000 (total received)")
	assert.Equal(t, 0, aggregateBalanceAfterSpend.DebitBalance.Cmp(big.NewInt(3000)),
		"Aggregate debit balance should be 3000 (released/spent)")

	t.Log("=== Inflight commit and spend test completed ===")
}

func TestLineageInflightVoid(t *testing.T) {
	if testing.Short() {
		t.Skip("Skipping lineage integration test in short mode")
	}

	ctx := context.Background()

	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		DataSource: config.DataSourceConfig{
			Dns: "postgres://postgres:password@localhost:5432/blnk?sslmode=disable",
		},
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue_test",
			IndexQueue:       "index_queue_test",
			TransactionQueue: "transaction_queue_test",
			NumberOfQueues:   1,
		},
		Server: config.ServerConfig{
			SecretKey: "test-secret",
		},
		Transaction: config.TransactionConfig{
			BatchSize:        100,
			MaxQueueSize:     1000,
			LockDuration:     time.Second * 30,
			IndexQueuePrefix: "test_index",
		},
	}
	config.ConfigStore.Store(cnf)

	ds, err := database.NewDataSource(cnf)
	require.NoError(t, err, "Failed to create datasource")

	blnk, err := NewBlnk(ds)
	require.NoError(t, err, "Failed to create Blnk instance")

	// Start the lineage outbox processor to process lineage entries asynchronously
	processor := NewLineageOutboxProcessor(blnk).WithPollInterval(100 * time.Millisecond)
	processor.Start(ctx)
	defer processor.Stop()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())

	identity, err := ds.CreateIdentity(model.Identity{
		FirstName: fmt.Sprintf("Void_%s", suffix),
		LastName:  "Test",
		Category:  "individual",
	})
	require.NoError(t, err)

	balance, err := ds.CreateBalance(model.Balance{
		Currency:           "USD",
		LedgerID:           GeneralLedgerID,
		IdentityID:         identity.IdentityID,
		TrackFundLineage:   true,
		AllocationStrategy: "FIFO",
	})
	require.NoError(t, err)

	inflightTxn, err := blnk.QueueTransaction(ctx, &model.Transaction{
		Reference:      fmt.Sprintf("inflight_void_%s", suffix),
		Source:         "@world",
		Destination:    balance.BalanceID,
		Amount:         75,
		Currency:       "USD",
		AllowOverdraft: true,
		Precision:      100,
		Inflight:       true,
		SkipQueue:      true,
		MetaData:       map[string]interface{}{"BLNK_LINEAGE_PROVIDER": "bank"},
	})
	require.NoError(t, err)
	t.Logf("Created inflight transaction: %s (status: %s)", inflightTxn.TransactionID, inflightTxn.Status)
	require.Equal(t, StatusInflight, inflightTxn.Status, "Transaction should be INFLIGHT")

	// Poll for lineage mapping (processed asynchronously by outbox worker)
	mappings, err := pollForLineageMappings(ctx, ds, balance.BalanceID, 1, 100*time.Millisecond, 10*time.Second)
	require.NoError(t, err, "Failed to poll for lineage mappings")
	require.Len(t, mappings, 1, "Should have 1 lineage mapping")

	// Check shadow balance before void
	shadowBalanceBefore, err := ds.GetBalanceByIDLite(mappings[0].ShadowBalanceID)
	require.NoError(t, err)
	t.Logf("Shadow balance before void: InflightDebitBalance=%s", shadowBalanceBefore.InflightDebitBalance.String())
	assert.Equal(t, 0, shadowBalanceBefore.InflightDebitBalance.Cmp(big.NewInt(7500)),
		"Shadow inflight debit balance should be 7500 before void")

	// Void the main transaction
	voidTxn, err := blnk.VoidInflightTransaction(ctx, inflightTxn.TransactionID)
	require.NoError(t, err)
	t.Logf("Voided main transaction: %s (parent: %s)", voidTxn.TransactionID, voidTxn.ParentTransaction)
	require.Equal(t, StatusVoid, voidTxn.Status, "Voided transaction should have VOID status")

	// Main balance should have inflight released immediately
	mainBalance, err := ds.GetBalanceByIDLite(balance.BalanceID)
	require.NoError(t, err)
	t.Logf("Main balance after void: Balance=%s, InflightBalance=%s",
		mainBalance.Balance.String(), mainBalance.InflightBalance.String())
	assert.Equal(t, 0, mainBalance.InflightBalance.Cmp(big.NewInt(0)),
		"Main inflight balance should be 0 after void")
	assert.Equal(t, 0, mainBalance.Balance.Cmp(big.NewInt(0)),
		"Main balance should be 0 (nothing committed)")

	// Shadow balance should have inflight released
	shadowBalanceAfter, err := ds.GetBalanceByIDLite(mappings[0].ShadowBalanceID)
	require.NoError(t, err)
	t.Logf("Shadow balance after void: InflightDebitBalance=%s",
		shadowBalanceAfter.InflightDebitBalance.String())
	assert.Equal(t, 0, shadowBalanceAfter.InflightDebitBalance.Cmp(big.NewInt(0)),
		"Shadow inflight debit balance should be 0 after void")

	// Aggregate balance should have inflight released
	aggregateBalanceAfter, err := ds.GetBalanceByIDLite(mappings[0].AggregateBalanceID)
	require.NoError(t, err)
	t.Logf("Aggregate balance after void: InflightCreditBalance=%s",
		aggregateBalanceAfter.InflightCreditBalance.String())
	assert.Equal(t, 0, aggregateBalanceAfter.InflightCreditBalance.Cmp(big.NewInt(0)),
		"Aggregate inflight credit balance should be 0 after void")

	t.Log("=== Inflight void test completed ===")
}
