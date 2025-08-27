package model

import (
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestGenerateUUIDWithSuffix(t *testing.T) {
	module := "test_module"
	id := GenerateUUIDWithSuffix(module)
	assert.Contains(t, id, module+"_")
}

func TestInt64ToBigInt(t *testing.T) {
	value := int64(123456789)
	bigIntValue := Int64ToBigInt(value)
	expected := big.NewInt(value)
	assert.Equal(t, expected, bigIntValue)
}

func TestTransaction_HashTxn(t *testing.T) {
	txn := &Transaction{
		Amount:      100.0,
		Reference:   "ref123",
		Currency:    "USD",
		Source:      "source_account",
		Destination: "dest_account",
	}
	data := "100.000000ref123USDsource_accountdest_account"
	expectedHash := sha256.Sum256([]byte(data))
	expectedHashStr := hex.EncodeToString(expectedHash[:])

	txnHash := txn.HashTxn()
	assert.Equal(t, expectedHashStr, txnHash)
}

func TestCompare(t *testing.T) {
	value := big.NewInt(10)
	compareTo := big.NewInt(20)

	assert.True(t, compare(value, "<", compareTo))
	assert.False(t, compare(value, ">", compareTo))
	assert.True(t, compare(value, "<=", compareTo))
	assert.False(t, compare(value, ">=", compareTo))
	assert.True(t, compare(value, "!=", compareTo))
	assert.False(t, compare(value, "==", compareTo))
}

func TestBalance_AddCredit(t *testing.T) {
	balance := &Balance{
		CreditBalance: big.NewInt(0),
	}
	amount := new(big.Int)
	amount.SetString("922337203685477580800", 10)
	balance.addCredit(amount, false)
	expected := new(big.Int)
	expected.SetString("922337203685477580800", 10)
	assert.Equal(t, expected, balance.CreditBalance)
}

func TestBalance_AddDebit(t *testing.T) {
	balance := &Balance{
		DebitBalance: big.NewInt(0),
	}
	amount := Int64ToBigInt(300)
	balance.addDebit(amount, false)
	expected := big.NewInt(300)
	assert.Equal(t, expected, balance.DebitBalance)
}

func TestBalance_ComputeBalance(t *testing.T) {
	balance := &Balance{
		CreditBalance: big.NewInt(1000),
		DebitBalance:  big.NewInt(300),
		Balance:       big.NewInt(0),
	}
	balance.computeBalance(false)
	expected := big.NewInt(700)
	assert.Equal(t, expected, balance.Balance)
}

func TestCanProcessTransaction(t *testing.T) {
	t.Run("Sufficient funds", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance: big.NewInt(500),
		}
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(400),
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})

	t.Run("Insufficient funds, no overdraft", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance: big.NewInt(500),
		}
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(600),
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.Error(t, err)
		assert.EqualError(t, err, "insufficient funds in source balance")
	})

	t.Run("Unconditional overdraft", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance: big.NewInt(200),
		}
		txn := &Transaction{
			PreciseAmount:  Int64ToBigInt(1000),
			AllowOverdraft: true,
			OverdraftLimit: 0,
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})

	t.Run("Overdraft within limit", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance: big.NewInt(500),
		}
		txn := &Transaction{
			PreciseAmount:  Int64ToBigInt(1000), // This would result in -500 balance
			Precision:      1,
			OverdraftLimit: 600, // Limit allows up to -600
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})

	t.Run("Overdraft exceeding limit", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance: big.NewInt(500),
		}
		txn := &Transaction{
			PreciseAmount:  Int64ToBigInt(1200), // This would result in -700 balance
			Precision:      1,
			OverdraftLimit: 600, // Limit only allows up to -600
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.Error(t, err)
		assert.EqualError(t, err, "transaction exceeds overdraft limit")
	})

	t.Run("Sufficient funds with no inflight debits", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:         big.NewInt(1000),
			InflightBalance: big.NewInt(0),
		}
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(500),
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})

	t.Run("Insufficient funds due to inflight debits", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(1000),
			InflightDebitBalance: big.NewInt(600), // 600 already reserved
		}
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(500), // Trying to reserve 500 more, but only 400 available
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.Error(t, err)
		assert.EqualError(t, err, "insufficient funds in source balance")
	})

	t.Run("Sufficient funds considering inflight debits", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(1000),
			InflightDebitBalance: big.NewInt(600), // 600 already reserved
		}
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(400), // Trying to reserve 400, exactly what's available
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})

	t.Run("Overdraft with inflight debits within limit", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(500),
			InflightDebitBalance: big.NewInt(300), // 300 already reserved, available = 200
		}
		txn := &Transaction{
			PreciseAmount:  Int64ToBigInt(600), // Would result in -400 from available balance
			Precision:      1,
			OverdraftLimit: 500, // Limit allows up to -500
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})

	t.Run("Overdraft with inflight debits exceeding limit", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(500),
			InflightDebitBalance: big.NewInt(300), // 300 already reserved, available = 200
		}
		txn := &Transaction{
			PreciseAmount:  Int64ToBigInt(800), // Would result in -600 from available balance
			Precision:      1,
			OverdraftLimit: 500, // Limit only allows up to -500
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.Error(t, err)
		assert.EqualError(t, err, "transaction exceeds overdraft limit")
	})

	t.Run("multiple inflight transactions", func(t *testing.T) {
		// Account has balance of 5000
		sourceBalance := &Balance{
			Balance:              big.NewInt(5000),
			InflightDebitBalance: big.NewInt(5000), // 5 transactions of 1000 each already inflight
		}
		// Trying to create 6th transaction of 1000
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(1000),
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.Error(t, err)
		assert.EqualError(t, err, "insufficient funds in source balance")
	})

	// Tests for queued balance functionality (when enable_queued_checks is on)
	t.Run("Sufficient funds with no queued debits", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(1000),
			InflightDebitBalance: big.NewInt(0),
			QueuedDebitBalance:   big.NewInt(0),
			QueuedCreditBalance:  big.NewInt(0),
		}
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(500),
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})

	t.Run("Insufficient funds due to queued debits", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(1000),
			InflightDebitBalance: big.NewInt(0),
			QueuedDebitBalance:   big.NewInt(600), // 600 already queued for processing
			QueuedCreditBalance:  big.NewInt(0),
		}
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(500), // Trying to process 500 more, but only 400 available
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.Error(t, err)
		assert.EqualError(t, err, "insufficient funds in source balance")
	})

	t.Run("Sufficient funds considering queued debits", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(1000),
			InflightDebitBalance: big.NewInt(0),
			QueuedDebitBalance:   big.NewInt(600), // 600 already queued
			QueuedCreditBalance:  big.NewInt(0),
		}
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(400), // Trying to process 400, exactly what's available
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})

	t.Run("Insufficient funds due to combined inflight and queued debits", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(1000),
			InflightDebitBalance: big.NewInt(300), // 300 already inflight
			QueuedDebitBalance:   big.NewInt(400), // 400 already queued
			QueuedCreditBalance:  big.NewInt(0),
		}
		// Available balance = 1000 - 300 - 400 = 300
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(400), // Trying to process 400, but only 300 available
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.Error(t, err)
		assert.EqualError(t, err, "insufficient funds in source balance")
	})

	t.Run("Sufficient funds with combined inflight and queued debits", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(1000),
			InflightDebitBalance: big.NewInt(300), // 300 already inflight
			QueuedDebitBalance:   big.NewInt(400), // 400 already queued
			QueuedCreditBalance:  big.NewInt(0),
		}
		// Available balance = 1000 - 300 - 400 = 300
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(300), // Trying to process 300, exactly what's available
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})

	t.Run("Overdraft with queued debits within limit", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(500),
			InflightDebitBalance: big.NewInt(200), // 200 already inflight
			QueuedDebitBalance:   big.NewInt(100), // 100 already queued
			QueuedCreditBalance:  big.NewInt(0),
		}
		// Available balance = 500 - 200 - 100 = 200
		txn := &Transaction{
			PreciseAmount:  Int64ToBigInt(600), // Would result in -400 from available balance
			Precision:      1,
			OverdraftLimit: 500, // Limit allows up to -500
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})

	t.Run("Overdraft with queued debits exceeding limit", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(500),
			InflightDebitBalance: big.NewInt(200), // 200 already inflight
			QueuedDebitBalance:   big.NewInt(100), // 100 already queued
			QueuedCreditBalance:  big.NewInt(0),
		}
		// Available balance = 500 - 200 - 100 = 200
		txn := &Transaction{
			PreciseAmount:  Int64ToBigInt(800), // Would result in -600 from available balance
			Precision:      1,
			OverdraftLimit: 500, // Limit only allows up to -500
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.Error(t, err)
		assert.EqualError(t, err, "transaction exceeds overdraft limit")
	})

	t.Run("Queued balance is nil (queued checks disabled)", func(t *testing.T) {
		sourceBalance := &Balance{
			Balance:              big.NewInt(1000),
			InflightDebitBalance: big.NewInt(300),
			QueuedDebitBalance:   nil, // Queued checks disabled, should be ignored
			QueuedCreditBalance:  nil,
		}
		// Available balance = 1000 - 300 = 700 (queued balance ignored)
		txn := &Transaction{
			PreciseAmount: Int64ToBigInt(600),
		}
		err := canProcessTransaction(txn, sourceBalance)
		assert.NoError(t, err)
	})
}

func TestBalance_CommitInflightDebit(t *testing.T) {
	balance := &Balance{
		InflightDebitBalance: big.NewInt(500),
		DebitBalance:         big.NewInt(200),
		Balance:              big.NewInt(1000),
		InflightBalance:      big.NewInt(500),
	}

	txn := &Transaction{
		Amount:    300,
		Precision: 1,
	}

	balance.CommitInflightDebit(txn)

	// Expected: InflightDebitBalance should decrease by 300
	assert.Equal(t, big.NewInt(200), balance.InflightDebitBalance)

	// Expected: DebitBalance should increase by 300
	assert.Equal(t, big.NewInt(500), balance.DebitBalance)

	assert.Equal(t, big.NewInt(-500), balance.Balance)         // with credit balance as 0
	assert.Equal(t, big.NewInt(-200), balance.InflightBalance) // with inflight credit balance as 0
}

func TestBalance_CommitInflightCredit(t *testing.T) {
	balance := &Balance{
		InflightCreditBalance: big.NewInt(400),
		CreditBalance:         big.NewInt(100),
	}
	txn := &Transaction{
		Amount:    300,
		Precision: 1,
	}
	balance.CommitInflightCredit(txn)
	assert.Equal(t, big.NewInt(100), balance.InflightCreditBalance)
	assert.Equal(t, big.NewInt(400), balance.CreditBalance)
}

func TestBalance_RollbackInflightCredit(t *testing.T) {
	balance := &Balance{
		InflightCreditBalance: big.NewInt(400),
	}
	amount := big.NewInt(200)
	balance.RollbackInflightCredit(amount)
	assert.Equal(t, big.NewInt(200), balance.InflightCreditBalance)
}

func TestBalance_RollbackInflightDebit(t *testing.T) {
	balance := &Balance{
		InflightDebitBalance: big.NewInt(500),
	}
	amount := big.NewInt(300)
	balance.RollbackInflightDebit(amount)
	assert.Equal(t, big.NewInt(200), balance.InflightDebitBalance)
}

func TestApplyPrecision(t *testing.T) {
	txn := &Transaction{
		Amount:    123.45,
		Precision: 100,
	}
	preciseAmount := ApplyPrecision(txn)
	expected := big.NewInt(12345)
	assert.Equal(t, expected, preciseAmount)
}

func TestApplyRate(t *testing.T) {
	tests := []struct {
		name          string
		preciseAmount *big.Int
		rate          float64
		expected      *big.Int
	}{
		{
			name:          "regular rate",
			preciseAmount: Int64ToBigInt(1000),
			rate:          1.5,
			expected:      big.NewInt(1500),
		},
		{
			name:          "zero rate defaults to 1",
			preciseAmount: Int64ToBigInt(1000),
			rate:          0,
			expected:      big.NewInt(1000),
		},
		{
			name:          "rate less than 1",
			preciseAmount: Int64ToBigInt(1000),
			rate:          0.5,
			expected:      big.NewInt(500),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := ApplyRate(tt.preciseAmount, tt.rate)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestTransaction_Validate(t *testing.T) {
	txn := &Transaction{
		Amount: 100.0,
	}
	err := txn.validate()
	assert.NoError(t, err)

	txn.Amount = -50.0
	err = txn.validate()
	assert.Error(t, err)
	assert.EqualError(t, err, "transaction amount must be positive")
}

func TestUpdateBalances(t *testing.T) {
	sourceBalance := &Balance{
		Balance: big.NewInt(1000),
	}
	destinationBalance := &Balance{
		Balance: big.NewInt(500),
	}
	txn := &Transaction{
		Amount:    200.0,
		Precision: 1,
	}
	err := UpdateBalances(txn, sourceBalance, destinationBalance)
	assert.NoError(t, err)

	// Source balance should decrease
	assert.Equal(t, big.NewInt(-200), sourceBalance.Balance)
	// Destination balance should increase
	assert.Equal(t, big.NewInt(200), destinationBalance.Balance)
}

func TestUpdateBalances_WithRate(t *testing.T) {
	sourceBalance := &Balance{
		Balance: big.NewInt(0),
	}
	destinationBalance := &Balance{
		Balance: big.NewInt(0),
	}

	txn := &Transaction{
		Amount:         100.0,
		AllowOverdraft: true,
		Precision:      100, // Will make precise amount 10000
		Rate:           1.5, // Should make destination receive 15000
	}

	err := UpdateBalances(txn, sourceBalance, destinationBalance)
	assert.NoError(t, err)

	// Source balance should decrease by precise amount
	assert.Equal(t, big.NewInt(-10000), sourceBalance.Balance)
	// Destination balance should increase by rate-adjusted amount
	assert.Equal(t, big.NewInt(15000), destinationBalance.Balance)
}

func TestBalanceMonitor_CheckCondition(t *testing.T) {
	balance := &Balance{
		Balance: big.NewInt(1000),
	}
	monitor := &BalanceMonitor{
		Condition: AlertCondition{
			Field:        "balance",
			Operator:     ">",
			PreciseValue: big.NewInt(500),
		},
	}
	result := monitor.CheckCondition(balance)
	assert.True(t, result)

	monitor.Condition.Operator = "<"
	result = monitor.CheckCondition(balance)
	assert.False(t, result)
}

func TestExternalTransaction_ToInternalTransaction(t *testing.T) {
	extTxn := &ExternalTransaction{
		ID:          "ext123",
		Amount:      150.0,
		Reference:   "ref_ext",
		Currency:    "EUR",
		Date:        time.Now(),
		Description: "External transaction",
	}
	intTxn := extTxn.ToInternalTransaction()
	assert.Equal(t, extTxn.ID, intTxn.TransactionID)
	assert.Equal(t, extTxn.Amount, intTxn.Amount)
	assert.Equal(t, extTxn.Reference, intTxn.Reference)
	assert.Equal(t, extTxn.Currency, intTxn.Currency)
	assert.Equal(t, extTxn.Date, intTxn.CreatedAt)
	assert.Equal(t, extTxn.Description, intTxn.Description)
}

func TestApplyPrecisionWithEmptyAmount(t *testing.T) {
	// Test case where amount is empty but precise amount is provided
	t.Run("Convert PreciseAmount to Amount", func(t *testing.T) {
		// Set up a big.Int with the exact value
		preciseAmount := new(big.Int)
		preciseAmount.SetString("922337203684775808", 10)

		// Create transaction with precise amount but no amount
		txn := &Transaction{
			PreciseAmount: preciseAmount,
			Precision:     10000000000, // 10 billion
			// Amount is purposely left as 0
		}

		// Apply precision, which should calculate the amount
		result := ApplyPrecision(txn)

		// Check that precise amount remains unchanged
		assert.Equal(t, preciseAmount, result)

		// The expected amount should be:
		// 922337203684775808 ÷ 10000000000 = 92233720.3684775808
		expectedAmount := 92233720.3684775808
		assert.InDelta(t, expectedAmount, txn.Amount, 0.0000000001)

		// If we've implemented AmountString, verify that too
		if txn.AmountString != "" {
			expectedAmountString := "92233720.3684775808"
			assert.Equal(t, expectedAmountString, txn.AmountString)
		}
	})
}
