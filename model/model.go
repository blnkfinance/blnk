package model

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

func GenerateUUIDWithSuffix(module string) string {

	// Generate a new UUID
	id := uuid.New()

	// Convert the UUID to a string
	uuidStr := id.String()

	// Add the module suffix
	idWithSuffix := fmt.Sprintf("%s_%s", module, uuidStr)

	return idWithSuffix
}

func (transaction *Transaction) HashTxn() string {

	data := fmt.Sprintf("%d%s%s%s%s", transaction.Amount, transaction.Reference, transaction.Currency, transaction.Source, transaction.Destination)
	// Compute SHA-256 hash
	hash := sha256.Sum256([]byte(data))
	// Return the hexadecimal encoding of the hash
	return hex.EncodeToString(hash[:])
}

func compare(value int64, condition string, compareTo int64) bool {
	switch condition {
	case ">":
		return value > compareTo
	case "<":
		return value < compareTo
	case ">=":
		return value >= compareTo
	case "<=":
		return value <= compareTo
	case "==":
		return value == compareTo
	}
	return false
}

func (balance *Balance) addCredit(amount int64, inflight bool) {
	//if transaction is an inflight transaction compute the inflight balance
	if inflight {
		balance.InflightCreditBalance += amount
		return
	}

	balance.CreditBalance += amount
}

func (balance *Balance) addDebit(amount int64, inflight bool) {
	//if transaction is an inflight transaction compute the inflight balance
	if inflight {
		balance.InflightDebitBalance += amount
		return
	}
	balance.DebitBalance += amount
}

func (balance *Balance) computeBalance(inflight bool) {
	//if transaction is an inflight transaction compute the inflight balance
	if inflight {
		balance.InflightBalance = balance.InflightCreditBalance - balance.InflightDebitBalance
		return
	}
	balance.Balance = balance.CreditBalance - balance.DebitBalance
}

func (balance *Balance) CommitInflightDebit(amount int64) {
	if balance.InflightDebitBalance >= amount {
		balance.InflightDebitBalance -= amount
		balance.DebitBalance += amount
		balance.computeBalance(true)  // Update inflight balance
		balance.computeBalance(false) // Update normal balance
	}
}

func (balance *Balance) CommitInflightCredit(amount int64) {
	if balance.InflightCreditBalance >= amount {
		balance.InflightCreditBalance -= amount
		balance.CreditBalance += amount
		balance.computeBalance(true)  // Update inflight balance
		balance.computeBalance(false) // Update normal balance
	}
}

// RollbackInflightCredit decreases the InflightCreditBalance by the specified amount
func (balance *Balance) RollbackInflightCredit(amount int64) {
	fmt.Println(amount, balance.InflightCreditBalance)
	if balance.InflightCreditBalance >= amount {
		balance.InflightCreditBalance -= amount
		balance.computeBalance(true) // Update inflight balance
	}
}

// RollbackInflightDebit decreases the InflightDebitBalance by the specified amount
func (balance *Balance) RollbackInflightDebit(amount int64) {
	if balance.InflightDebitBalance >= amount {
		balance.InflightDebitBalance -= amount
		balance.computeBalance(true) // Update inflight balance
	}
}

func (balance *Balance) applyMultiplier(transaction *Transaction) {
	if balance.CurrencyMultiplier == 0 {
		balance.CurrencyMultiplier = 1
	}
	transaction.Amount = transaction.Amount * balance.CurrencyMultiplier
}

func canProcessTransaction(transaction *Transaction, sourceBalance *Balance) error {
	if transaction.AllowOverdraft {
		// Skip balance check if overdraft is allowed
		return nil
	}

	// Use the provided sourceBalance for the check
	if sourceBalance.Balance < transaction.Amount {
		return fmt.Errorf("insufficient funds in source balance")
	}

	return nil
}

func (transaction *Transaction) validate() error {
	if transaction.Amount <= 0 {
		return errors.New("transaction amount must be positive")
	}

	return nil
}

func UpdateBalances(transaction *Transaction, source, destination *Balance) error {
	// Validate transaction
	err := transaction.validate()
	if err != nil {
		return err
	}

	err = canProcessTransaction(transaction, source)
	if err != nil {
		return err
	}

	//compute source balance
	source.applyMultiplier(transaction)
	source.addDebit(transaction.Amount, transaction.Inflight)
	source.computeBalance(transaction.Inflight)

	//compute destination balance
	destination.applyMultiplier(transaction)
	destination.addCredit(transaction.Amount, transaction.Inflight)
	destination.computeBalance(transaction.Inflight)
	return nil
}

func (bm *BalanceMonitor) CheckCondition(b *Balance) bool {
	switch bm.Condition.Field {
	case "debit_balance":
		return compare(b.DebitBalance, bm.Condition.Operator, bm.Condition.Value)
	case "credit_balance":
		return compare(b.CreditBalance, bm.Condition.Operator, bm.Condition.Value)
	case "balance":
		return compare(b.Balance, bm.Condition.Operator, bm.Condition.Value)
	}
	return false
}
