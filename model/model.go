package model

import (
	"errors"
	"fmt"
)

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

func (balance *Balance) addCredit(amount int64) {
	balance.CreditBalance += amount
}

func (balance *Balance) addDebit(amount int64) {
	balance.DebitBalance += amount
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

	source.applyMultiplier(transaction)
	source.addDebit(transaction.Amount)
	source.ComputeBalance()
	destination.applyMultiplier(transaction)
	destination.addCredit(transaction.Amount)
	destination.ComputeBalance()
	return nil
}

func (balance *Balance) ComputeBalance() {
	balance.Balance = balance.CreditBalance - balance.DebitBalance
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
