package model

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"github.com/google/uuid"
)

func GenerateUUIDWithSuffix(module string) string {
	id := uuid.New()
	uuidStr := id.String()
	idWithSuffix := fmt.Sprintf("%s_%s", module, uuidStr)
	return idWithSuffix
}

func (transaction *Transaction) HashTxn() string {
	data := fmt.Sprintf("%f%s%s%s%s", transaction.Amount, transaction.Reference, transaction.Currency, transaction.Source, transaction.Destination)
	hash := sha256.Sum256([]byte(data))
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
	case "!=":
		return value != compareTo
	case "==":
		return value == compareTo
	}
	return false
}

func (balance *Balance) addCredit(amount int64, inflight bool) {
	if inflight {
		balance.InflightCreditBalance += amount
		return
	}

	balance.CreditBalance += amount
}

func (balance *Balance) addDebit(amount int64, inflight bool) {
	if inflight {
		balance.InflightDebitBalance += amount
		return
	}
	balance.DebitBalance += amount
}

func (balance *Balance) computeBalance(inflight bool) {
	if inflight {
		balance.InflightBalance = balance.InflightCreditBalance - balance.InflightDebitBalance
		return
	}
	balance.Balance = balance.CreditBalance - balance.DebitBalance
}

func canProcessTransaction(transaction *Transaction, sourceBalance *Balance) error {
	if transaction.AllowOverdraft {
		// Skip balance check if overdraft is allowed
		return nil
	}

	// Use the provided sourceBalance for the check
	if sourceBalance.Balance < transaction.PreciseAmount {
		return fmt.Errorf("insufficient funds in source balance")
	}

	return nil
}

func (balance *Balance) CommitInflightDebit(transaction *Transaction) {
	preciseAmount := ApplyPrecision(transaction)

	transaction.PreciseAmount = preciseAmount //set transaction precision amount
	if balance.InflightDebitBalance >= preciseAmount {
		balance.InflightDebitBalance -= preciseAmount
		balance.DebitBalance += preciseAmount
		balance.computeBalance(true)  // Update inflight balance
		balance.computeBalance(false) // Update normal balance
	}
}

func (balance *Balance) CommitInflightCredit(transaction *Transaction) {
	preciseAmount := ApplyPrecision(transaction)
	transaction.PreciseAmount = preciseAmount
	if balance.InflightCreditBalance >= preciseAmount {
		balance.InflightCreditBalance -= preciseAmount
		balance.CreditBalance += preciseAmount
		balance.computeBalance(true)  // Update inflight balance
		balance.computeBalance(false) // Update normal balance
	}
}

// RollbackInflightCredit decreases the InflightCreditBalance by the specified amount
func (balance *Balance) RollbackInflightCredit(amount int64) {
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

func ApplyPrecision(transaction *Transaction) int64 {
	if transaction.Precision == 0 {
		transaction.Precision = 1
	}
	return int64(transaction.Amount * transaction.Precision)
}

func ApplyRate(transaction *Transaction) float64 {
	if transaction.Rate == 0 {
		transaction.Rate = 1
	}
	return transaction.Amount * transaction.Rate
}

func (transaction *Transaction) validate() error {
	if transaction.Amount <= 0 {
		return errors.New("transaction amount must be positive")
	}

	return nil
}

func UpdateBalances(transaction *Transaction, source, destination *Balance) error {
	transaction.PreciseAmount = ApplyPrecision(transaction)
	origanialAmount := transaction.Amount
	err := transaction.validate()
	if err != nil {
		return err
	}

	err = canProcessTransaction(transaction, source)
	if err != nil {
		return err
	}

	//compute source balance
	source.addDebit(transaction.PreciseAmount, transaction.Inflight)
	source.computeBalance(transaction.Inflight)

	//compute destination balance
	transaction.Amount = ApplyRate(transaction) //apply exchange rate to destination if rate is passed.
	transaction.PreciseAmount = ApplyPrecision(transaction)
	destination.addCredit(transaction.PreciseAmount, transaction.Inflight)
	destination.computeBalance(transaction.Inflight)

	transaction.Amount = origanialAmount //revert *Transaction.Amount back original amount after modification for destination rates
	transaction.PreciseAmount = ApplyPrecision(transaction)
	return nil
}

func (bm *BalanceMonitor) CheckCondition(b *Balance) bool {
	switch bm.Condition.Field {
	case "debit_balance":
		return compare(b.DebitBalance, bm.Condition.Operator, bm.Condition.PreciseValue)
	case "credit_balance":
		return compare(b.CreditBalance, bm.Condition.Operator, bm.Condition.PreciseValue)
	case "balance":
		return compare(b.Balance, bm.Condition.Operator, bm.Condition.PreciseValue)
	case "inflight_debit_balance":
		return compare(b.InflightDebitBalance, bm.Condition.Operator, bm.Condition.PreciseValue)
	case "inflight_credit_balance":
		return compare(b.InflightCreditBalance, bm.Condition.Operator, bm.Condition.PreciseValue)
	case "inflight_balance":
		return compare(b.InflightBalance, bm.Condition.Operator, bm.Condition.PreciseValue)
	}
	return false
}
