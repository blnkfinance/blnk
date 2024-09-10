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
	amount := int64(500)
	balance.addCredit(amount, false)
	expected := big.NewInt(500)
	assert.Equal(t, expected, balance.CreditBalance)
}

func TestBalance_AddDebit(t *testing.T) {
	balance := &Balance{
		DebitBalance: big.NewInt(0),
	}
	amount := int64(300)
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
	sourceBalance := &Balance{
		Balance: big.NewInt(500),
	}
	txn := &Transaction{
		PreciseAmount: 400,
	}
	err := canProcessTransaction(txn, sourceBalance)
	assert.NoError(t, err)

	txn.PreciseAmount = 600
	err = canProcessTransaction(txn, sourceBalance)
	assert.Error(t, err)
	assert.EqualError(t, err, "insufficient funds in source balance")
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
	expected := int64(12345)
	assert.Equal(t, expected, preciseAmount)
}

func TestApplyRate(t *testing.T) {
	txn := &Transaction{
		Amount: 100.0,
		Rate:   1.2,
	}
	adjustedAmount := ApplyRate(txn)
	expected := 120.0
	assert.Equal(t, expected, adjustedAmount)
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
