package model

import (
	"crypto/sha256"
	"encoding/hex"
	"math/big"
	"testing"
	"time"

	"github.com/shopspring/decimal"
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

func TestGenerateKey(t *testing.T) {
	key, err := GenerateKey()
	assert.NoError(t, err)
	assert.NotEmpty(t, key)
	assert.Greater(t, len(key), 20)

	key2, err := GenerateKey()
	assert.NoError(t, err)
	assert.NotEqual(t, key, key2)
}

func TestNewAPIKey(t *testing.T) {
	name := "Test Key"
	ownerID := "owner_123"
	scopes := []string{"read", "write"}
	expiresAt := time.Now().Add(24 * time.Hour)

	apiKey, err := NewAPIKey(name, ownerID, scopes, expiresAt)

	assert.NoError(t, err)
	assert.NotNil(t, apiKey)
	assert.Contains(t, apiKey.APIKeyID, "api_key_")
	assert.NotEmpty(t, apiKey.Key)
	assert.Equal(t, name, apiKey.Name)
	assert.Equal(t, ownerID, apiKey.OwnerID)
	assert.Equal(t, scopes, apiKey.Scopes)
	assert.Equal(t, expiresAt, apiKey.ExpiresAt)
	assert.False(t, apiKey.IsRevoked)
}

func TestAPIKey_IsValid(t *testing.T) {
	t.Run("Valid key - not revoked and not expired", func(t *testing.T) {
		apiKey := &APIKey{
			IsRevoked: false,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		}
		assert.True(t, apiKey.IsValid())
	})

	t.Run("Invalid key - revoked", func(t *testing.T) {
		apiKey := &APIKey{
			IsRevoked: true,
			ExpiresAt: time.Now().Add(24 * time.Hour),
		}
		assert.False(t, apiKey.IsValid())
	})

	t.Run("Invalid key - expired", func(t *testing.T) {
		apiKey := &APIKey{
			IsRevoked: false,
			ExpiresAt: time.Now().Add(-24 * time.Hour),
		}
		assert.False(t, apiKey.IsValid())
	})

	t.Run("Invalid key - revoked and expired", func(t *testing.T) {
		apiKey := &APIKey{
			IsRevoked: true,
			ExpiresAt: time.Now().Add(-24 * time.Hour),
		}
		assert.False(t, apiKey.IsValid())
	})
}

func TestAPIKey_HasScope(t *testing.T) {
	apiKey := &APIKey{
		Scopes: []string{"read", "write", "admin"},
	}

	t.Run("Has scope - read", func(t *testing.T) {
		assert.True(t, apiKey.HasScope("read"))
	})

	t.Run("Has scope - admin", func(t *testing.T) {
		assert.True(t, apiKey.HasScope("admin"))
	})

	t.Run("Does not have scope", func(t *testing.T) {
		assert.False(t, apiKey.HasScope("delete"))
	})

	t.Run("Empty scopes", func(t *testing.T) {
		emptyKey := &APIKey{Scopes: []string{}}
		assert.False(t, emptyKey.HasScope("read"))
	})
}

func TestConvertToStructFieldName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"firstName", "FirstName"},
		{"email", "Email"},
		{"First_name", "First_name"},
		{"", ""},
		{"a", "A"},
		{"ABC", "ABC"},
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			result := convertToStructFieldName(tc.input)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestIdentity_IsFieldTokenized(t *testing.T) {
	t.Run("Nil metadata", func(t *testing.T) {
		identity := &Identity{MetaData: nil}
		assert.False(t, identity.IsFieldTokenized("email"))
	})

	t.Run("No tokenized_fields key", func(t *testing.T) {
		identity := &Identity{MetaData: map[string]interface{}{}}
		assert.False(t, identity.IsFieldTokenized("email"))
	})

	t.Run("Field is tokenized - map[string]bool", func(t *testing.T) {
		identity := &Identity{
			MetaData: map[string]interface{}{
				"tokenized_fields": map[string]bool{"Email": true},
			},
		}
		assert.True(t, identity.IsFieldTokenized("email"))
		assert.True(t, identity.IsFieldTokenized("Email"))
	})

	t.Run("Field is tokenized - map[string]interface{}", func(t *testing.T) {
		identity := &Identity{
			MetaData: map[string]interface{}{
				"tokenized_fields": map[string]interface{}{"Email": true},
			},
		}
		assert.True(t, identity.IsFieldTokenized("email"))
	})

	t.Run("Field is not tokenized", func(t *testing.T) {
		identity := &Identity{
			MetaData: map[string]interface{}{
				"tokenized_fields": map[string]bool{"Phone": true},
			},
		}
		assert.False(t, identity.IsFieldTokenized("email"))
	})
}

func TestIdentity_MarkFieldAsTokenized(t *testing.T) {
	t.Run("Nil metadata - creates new", func(t *testing.T) {
		identity := &Identity{MetaData: nil}
		identity.MarkFieldAsTokenized("email")

		assert.NotNil(t, identity.MetaData)
		tokenizedFields := identity.MetaData["tokenized_fields"].(map[string]bool)
		assert.True(t, tokenizedFields["Email"])
	})

	t.Run("Existing metadata - adds field", func(t *testing.T) {
		identity := &Identity{
			MetaData: map[string]interface{}{
				"tokenized_fields": map[string]bool{"Phone": true},
			},
		}
		identity.MarkFieldAsTokenized("email")

		tokenizedFields := identity.MetaData["tokenized_fields"].(map[string]bool)
		assert.True(t, tokenizedFields["Email"])
		assert.True(t, tokenizedFields["Phone"])
	})

	t.Run("Handle map[string]interface{} conversion", func(t *testing.T) {
		identity := &Identity{
			MetaData: map[string]interface{}{
				"tokenized_fields": map[string]interface{}{"Phone": true},
			},
		}
		identity.MarkFieldAsTokenized("email")

		tokenizedFields := identity.MetaData["tokenized_fields"].(map[string]bool)
		assert.True(t, tokenizedFields["Email"])
		assert.True(t, tokenizedFields["Phone"])
	})
}

func TestTransaction_ToJSON(t *testing.T) {
	txn := &Transaction{
		TransactionID: "txn_123",
		Amount:        100.50,
		Currency:      "USD",
		Reference:     "ref_abc",
		Source:        "src_123",
		Destination:   "dst_456",
		Status:        "APPLIED",
	}

	jsonBytes, err := txn.ToJSON()

	assert.NoError(t, err)
	assert.NotNil(t, jsonBytes)
	assert.Contains(t, string(jsonBytes), "txn_123")
	assert.Contains(t, string(jsonBytes), "100.5")
	assert.Contains(t, string(jsonBytes), "USD")
}

func TestPrecisionBankersRound(t *testing.T) {
	tests := []struct {
		name      string
		num       float64
		precision float64
		expected  float64
	}{
		{"Round down", 1.234, 100, 1.23},
		{"Round up", 1.236, 100, 1.24},
		{"Banker's round even - round down", 1.225, 100, 1.22},
		{"Banker's round odd - round up", 1.235, 100, 1.24},
		{"No rounding needed", 1.50, 100, 1.50},
		{"Higher precision", 1.23456, 10000, 1.2346},
		{"Whole number", 5.0, 100, 5.0},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result := PrecisionBankersRound(tc.num, tc.precision)
			assert.InDelta(t, tc.expected, result, 0.0001)
		})
	}
}

func TestFindLargestDecimal(t *testing.T) {
	t.Run("Find largest", func(t *testing.T) {
		amounts := map[string]decimal.Decimal{
			"a": decimal.NewFromFloat(100.50),
			"b": decimal.NewFromFloat(200.75),
			"c": decimal.NewFromFloat(50.25),
		}
		key, amount := findLargestDecimal(amounts)
		assert.Equal(t, "b", key)
		assert.True(t, amount.Equal(decimal.NewFromFloat(200.75)))
	})

	t.Run("Single element", func(t *testing.T) {
		amounts := map[string]decimal.Decimal{
			"only": decimal.NewFromFloat(123.45),
		}
		key, amount := findLargestDecimal(amounts)
		assert.Equal(t, "only", key)
		assert.True(t, amount.Equal(decimal.NewFromFloat(123.45)))
	})

	t.Run("Empty map", func(t *testing.T) {
		amounts := map[string]decimal.Decimal{}
		key, amount := findLargestDecimal(amounts)
		assert.Equal(t, "", key)
		assert.True(t, amount.IsZero())
	})
}

func TestHandleZeroAmount(t *testing.T) {
	t.Run("Multiple distributions", func(t *testing.T) {
		distributions := []Distribution{
			{Identifier: "id1"},
			{Identifier: "id2"},
			{Identifier: "id3"},
		}
		result := handleZeroAmount(distributions)

		assert.Len(t, result, 3)
		assert.Equal(t, big.NewInt(0), result["id1"])
		assert.Equal(t, big.NewInt(0), result["id2"])
		assert.Equal(t, big.NewInt(0), result["id3"])
	})

	t.Run("Single distribution", func(t *testing.T) {
		distributions := []Distribution{
			{Identifier: "single"},
		}
		result := handleZeroAmount(distributions)

		assert.Len(t, result, 1)
		assert.Equal(t, big.NewInt(0), result["single"])
	})

	t.Run("Empty distributions", func(t *testing.T) {
		distributions := []Distribution{}
		result := handleZeroAmount(distributions)

		assert.Len(t, result, 0)
	})
}

func TestCheckCondition_Extended(t *testing.T) {
	balance := &Balance{
		Balance:       big.NewInt(10000),
		CreditBalance: big.NewInt(15000),
		DebitBalance:  big.NewInt(5000),
	}

	t.Run("Balance greater than - using BalanceMonitor", func(t *testing.T) {
		monitor := &BalanceMonitor{
			Condition: AlertCondition{
				Field:        "balance",
				Operator:     ">",
				Value:        50,
				Precision:    100,
				PreciseValue: big.NewInt(5000),
			},
		}
		result := monitor.CheckCondition(balance)
		assert.True(t, result)
	})

	t.Run("Credit balance less than", func(t *testing.T) {
		monitor := &BalanceMonitor{
			Condition: AlertCondition{
				Field:        "credit_balance",
				Operator:     "<",
				Value:        200,
				Precision:    100,
				PreciseValue: big.NewInt(20000),
			},
		}
		result := monitor.CheckCondition(balance)
		assert.True(t, result)
	})

	t.Run("Debit balance equals", func(t *testing.T) {
		monitor := &BalanceMonitor{
			Condition: AlertCondition{
				Field:        "debit_balance",
				Operator:     "==",
				Value:        50,
				Precision:    100,
				PreciseValue: big.NewInt(5000),
			},
		}
		result := monitor.CheckCondition(balance)
		assert.True(t, result)
	})

	t.Run("Unknown field returns false", func(t *testing.T) {
		monitor := &BalanceMonitor{
			Condition: AlertCondition{
				Field:        "unknown_field",
				Operator:     ">",
				Value:        0,
				PreciseValue: big.NewInt(0),
			},
		}
		result := monitor.CheckCondition(balance)
		assert.False(t, result)
	})
}
