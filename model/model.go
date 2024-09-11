package model

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math/big"

	"github.com/google/uuid"
)

// GenerateUUIDWithSuffix generates a UUID with a given module name as a suffix.
// This is useful for creating unique identifiers with context-specific prefixes.
func GenerateUUIDWithSuffix(module string) string {
	id := uuid.New() // Generate a new UUID.
	uuidStr := id.String()
	idWithSuffix := fmt.Sprintf("%s_%s", module, uuidStr) // Append the module as a suffix to the UUID.
	return idWithSuffix
}

// Int64ToBigInt converts an int64 value to a *big.Int.
// This is useful for handling large numbers in computations such as financial transactions.
func Int64ToBigInt(value int64) *big.Int {
	return big.NewInt(value) // Create a new big.Int from an int64 value.
}

// HashTxn generates a SHA-256 hash of a transaction's relevant fields.
// This ensures the integrity of the transaction by creating a unique hash from its details.
func (transaction *Transaction) HashTxn() string {
	// Concatenate the transaction's fields into a single string.
	data := fmt.Sprintf("%f%s%s%s%s", transaction.Amount, transaction.Reference, transaction.Currency, transaction.Source, transaction.Destination)
	hash := sha256.Sum256([]byte(data)) // Hash the concatenated data.
	return hex.EncodeToString(hash[:])  // Return the hex-encoded hash.
}

// compare compares two *big.Int values based on the provided condition (e.g., >, <, ==).
// Returns true if the condition holds, otherwise false.
func compare(value *big.Int, condition string, compareTo *big.Int) bool {
	cmp := value.Cmp(compareTo) // Compare value and compareTo.
	switch condition {
	case ">":
		return cmp > 0
	case "<":
		return cmp < 0
	case ">=":
		return cmp >= 0
	case "<=":
		return cmp <= 0
	case "!=":
		return cmp != 0
	case "==":
		return cmp == 0
	}
	return false
}

// InitializeBalanceFields initializes all the fields of the Balance struct that might be nil.
// This ensures that all balance-related fields have valid *big.Int values for further operations.
func (balance *Balance) InitializeBalanceFields() {
	if balance.InflightDebitBalance == nil {
		balance.InflightDebitBalance = big.NewInt(0)
	}
	if balance.InflightCreditBalance == nil {
		balance.InflightCreditBalance = big.NewInt(0)
	}
	if balance.InflightBalance == nil {
		balance.InflightBalance = big.NewInt(0)
	}
	if balance.DebitBalance == nil {
		balance.DebitBalance = big.NewInt(0)
	}
	if balance.CreditBalance == nil {
		balance.CreditBalance = big.NewInt(0)
	}
	if balance.Balance == nil {
		balance.Balance = big.NewInt(0)
	}
}

// addCredit adds the specified amount to the credit balances (either inflight or regular).
// inflight indicates whether the credit is inflight or not.
func (balance *Balance) addCredit(amount int64, inflight bool) {
	balance.InitializeBalanceFields()     // Ensure balance fields are initialized.
	amountBigInt := Int64ToBigInt(amount) // Convert the amount to *big.Int.
	if inflight {
		balance.InflightCreditBalance.Add(balance.InflightCreditBalance, amountBigInt)
	} else {
		balance.CreditBalance.Add(balance.CreditBalance, amountBigInt)
	}
}

// addDebit adds the specified amount to the debit balances (either inflight or regular).
// inflight indicates whether the debit is inflight or not.
func (balance *Balance) addDebit(amount int64, inflight bool) {
	balance.InitializeBalanceFields()
	amountBigInt := Int64ToBigInt(amount)
	if inflight {
		balance.InflightDebitBalance.Add(balance.InflightDebitBalance, amountBigInt)
	} else {
		balance.DebitBalance.Add(balance.DebitBalance, amountBigInt)
	}
}

// computeBalance computes the overall balance for inflight and normal balances.
// inflight indicates whether the inflight balance or regular balance should be computed.
func (balance *Balance) computeBalance(inflight bool) {
	balance.InitializeBalanceFields()
	if inflight {
		balance.InflightBalance.Sub(balance.InflightCreditBalance, balance.InflightDebitBalance)
		return
	}
	balance.Balance.Sub(balance.CreditBalance, balance.DebitBalance)
}

// canProcessTransaction checks if a transaction can be processed given the source balance.
// It returns an error if the balance is insufficient and overdraft is not allowed.
func canProcessTransaction(transaction *Transaction, sourceBalance *Balance) error {
	if transaction.AllowOverdraft {
		// Overdraft allowed, skip balance check.
		return nil
	}

	// Convert transaction.PreciseAmount to *big.Int for comparison.
	transactionAmount := new(big.Int).SetInt64(transaction.PreciseAmount)

	if sourceBalance.Balance.Cmp(transactionAmount) < 0 {
		// Insufficient funds.
		return fmt.Errorf("insufficient funds in source balance")
	}

	return nil
}

// CommitInflightDebit commits a debit from the inflight balance and adds it to the debit balance.
// This is part of the finalization process for inflight transactions.
func (balance *Balance) CommitInflightDebit(transaction *Transaction) {
	balance.InitializeBalanceFields()
	preciseAmount := ApplyPrecision(transaction)              // Apply precision to the transaction amount.
	transactionAmount := new(big.Int).SetInt64(preciseAmount) // Convert to *big.Int.

	if balance.InflightDebitBalance.Cmp(transactionAmount) >= 0 {
		// Deduct from inflight and add to regular debit balance.
		balance.InflightDebitBalance.Sub(balance.InflightDebitBalance, transactionAmount)
		balance.DebitBalance.Add(balance.DebitBalance, transactionAmount)
		balance.computeBalance(true)  // Recompute inflight balance.
		balance.computeBalance(false) // Recompute regular balance.
	}
}

// CommitInflightCredit commits a credit from the inflight balance and adds it to the credit balance.
func (balance *Balance) CommitInflightCredit(transaction *Transaction) {
	balance.InitializeBalanceFields()
	preciseAmount := ApplyPrecision(transaction)
	transactionAmount := new(big.Int).SetInt64(preciseAmount)

	if balance.InflightCreditBalance.Cmp(transactionAmount) >= 0 {
		// Deduct from inflight and add to regular credit balance.
		balance.InflightCreditBalance.Sub(balance.InflightCreditBalance, transactionAmount)
		balance.CreditBalance.Add(balance.CreditBalance, transactionAmount)
		balance.computeBalance(true)  // Recompute inflight balance.
		balance.computeBalance(false) // Recompute regular balance.
	}
}

// RollbackInflightCredit rolls back (decreases) the inflight credit balance by the specified amount.
func (balance *Balance) RollbackInflightCredit(amount *big.Int) {
	balance.InitializeBalanceFields()
	if balance.InflightCreditBalance.Cmp(amount) >= 0 {
		balance.InflightCreditBalance.Sub(balance.InflightCreditBalance, amount)
		balance.computeBalance(true) // Update inflight balance.
	}
}

// RollbackInflightDebit rolls back (decreases) the inflight debit balance by the specified amount.
func (balance *Balance) RollbackInflightDebit(amount *big.Int) {
	balance.InitializeBalanceFields()
	if balance.InflightDebitBalance.Cmp(amount) >= 0 {
		balance.InflightDebitBalance.Sub(balance.InflightDebitBalance, amount)
		balance.computeBalance(true) // Update inflight balance.
	}
}

// ApplyPrecision applies precision to the transaction amount by multiplying it by the transaction precision value.
func ApplyPrecision(transaction *Transaction) int64 {
	if transaction.Precision == 0 {
		transaction.Precision = 1
	}
	return int64(transaction.Amount * transaction.Precision)
}

// ApplyRate applies the exchange rate to the transaction amount.
// If no rate is provided, it defaults to 1.
func ApplyRate(transaction *Transaction) float64 {
	if transaction.Rate == 0 {
		transaction.Rate = 1
	}
	return transaction.Amount * transaction.Rate
}

// validate checks if the transaction is valid (e.g., ensuring positive amount).
func (transaction *Transaction) validate() error {
	if transaction.Amount <= 0 {
		return errors.New("transaction amount must be positive")
	}
	return nil
}

// UpdateBalances updates the balances for both the source and destination based on the transaction details.
// It ensures precision is applied and checks for overdraft before updating.
func UpdateBalances(transaction *Transaction, source, destination *Balance) error {
	transaction.PreciseAmount = ApplyPrecision(transaction)
	originalAmount := transaction.Amount
	err := transaction.validate()
	if err != nil {
		return err
	}

	// Ensure the source balance has sufficient funds.
	err = canProcessTransaction(transaction, source)
	if err != nil {
		return err
	}

	source.InitializeBalanceFields()
	destination.InitializeBalanceFields()

	// Compute the source balance after debiting.
	source.addDebit(transaction.PreciseAmount, transaction.Inflight)
	source.computeBalance(transaction.Inflight)

	// Apply exchange rate to the destination if needed.
	transaction.Amount = ApplyRate(transaction)
	transaction.PreciseAmount = ApplyPrecision(transaction)
	destination.addCredit(transaction.PreciseAmount, transaction.Inflight)
	destination.computeBalance(transaction.Inflight)

	// Revert the transaction amount to its original state.
	transaction.Amount = originalAmount
	transaction.PreciseAmount = ApplyPrecision(transaction)
	return nil
}

// CheckCondition checks if a balance meets the condition specified by a BalanceMonitor.
// It compares various balance fields (e.g., debit balance, credit balance) against the precise value.
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

// ToInternalTransaction converts an ExternalTransaction to an InternalTransaction.
// This is useful when reconciling external transactions with internal records.
func (et *ExternalTransaction) ToInternalTransaction() *Transaction {
	return &Transaction{
		TransactionID: et.ID,
		Amount:        et.Amount,
		Reference:     et.Reference,
		Currency:      et.Currency,
		CreatedAt:     et.Date,
		Description:   et.Description,
	}
}
