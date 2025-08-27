package model

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"log"
	"math/big"
	"strconv"

	pgconn "github.com/blnkfinance/blnk/internal/pg-conn"
	"github.com/google/uuid"

	"github.com/shopspring/decimal"
)

// precisionCache stores transaction precisions fetched from the database.
var precisionCache map[string]float64

// init initializes the precisionCache.
func init() {
	precisionCache = make(map[string]float64)
}

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
func (balance *Balance) addCredit(amountBigInt *big.Int, inflight bool) {
	balance.InitializeBalanceFields() // Ensure balance fields are initialized.

	if inflight {
		balance.InflightCreditBalance.Add(balance.InflightCreditBalance, amountBigInt)
	} else {
		balance.CreditBalance.Add(balance.CreditBalance, amountBigInt)
	}
}

// addDebit adds the specified amount to the debit balances (either inflight or regular).
// inflight indicates whether the debit is inflight or not.
func (balance *Balance) addDebit(amountBigInt *big.Int, inflight bool) {
	balance.InitializeBalanceFields()
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
// This function includes inflight balances and optionally queued balances in the available balance calculation
// to prevent creating transactions that would exceed the actual available funds.
func canProcessTransaction(transaction *Transaction, sourceBalance *Balance) error {
	if transaction.AllowOverdraft && transaction.OverdraftLimit == 0 {
		// If unconditional overdraft is allowed, skip all balance checks
		return nil
	}

	// Initialize balance fields to ensure inflight balances are not nil
	sourceBalance.InitializeBalanceFields()

	// Convert transaction.PreciseAmount to *big.Int for comparison.
	transactionAmount := transaction.PreciseAmount

	// Calculate available balance by subtracting inflight balances from committed balance
	// This ensures inflight balances are considered when checking if new transactions can be processed
	availableBalance := new(big.Int).Sub(sourceBalance.Balance, sourceBalance.InflightDebitBalance)

	// If queued balances are available (when enable_queued_checks is on), also subtract queued debit balance
	// This provides an even more comprehensive check by considering pending queued transactions
	if sourceBalance.QueuedDebitBalance != nil {
		availableBalance = new(big.Int).Sub(availableBalance, sourceBalance.QueuedDebitBalance)
	}

	if availableBalance.Cmp(transactionAmount) >= 0 {
		// Sufficient funds considering inflight and queued debits
		return nil
	}

	// Insufficient funds, check if within overdraft limit
	if transaction.OverdraftLimit > 0 {
		// Calculate the resulting balance after transaction, considering inflight and queued debits
		resultingBalance := new(big.Int).Sub(availableBalance, transactionAmount)

		// Convert overdraft limit to big.Int with precision applied
		overdraftLimitPrecise := int64(transaction.OverdraftLimit * transaction.Precision)
		overdraftLimitBigInt := new(big.Int).SetInt64(overdraftLimitPrecise)

		// Negative of overdraft limit (as balance will be negative)
		negativeOverdraftLimit := new(big.Int).Neg(overdraftLimitBigInt)

		// Check if resulting balance is within overdraft limit
		if resultingBalance.Cmp(negativeOverdraftLimit) >= 0 {
			return nil
		}
		return fmt.Errorf("transaction exceeds overdraft limit")
	}

	// Insufficient funds and no overdraft allowed
	return fmt.Errorf("insufficient funds in source balance")
}

// CommitInflightDebit commits a debit from the inflight balance and adds it to the debit balance.
// This is part of the finalization process for inflight transactions.
func (balance *Balance) CommitInflightDebit(transaction *Transaction) {
	balance.InitializeBalanceFields()
	preciseAmount := ApplyPrecision(transaction) // Apply precision to the transaction amount.
	transactionAmount := preciseAmount           // Convert to *big.Int.

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
	transactionAmount := preciseAmount

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

// ApplyPrecision handles both operations involving precision:
// 1. If PreciseAmount exists: converts it to a decimal Amount
// 2. If Amount exists: converts it to a PreciseAmount
// This function is now a wrapper that sets default precision if needed and calls applyPrecisionLogic.
func ApplyPrecision(transaction *Transaction) *big.Int {
	if transaction.Precision == 0 {
		transaction.Precision = 1
	}
	return applyPrecisionLogic(transaction)
}

// applyPrecisionLogic contains the core logic for converting amounts based on precision.
// It assumes transaction.Precision has been set appropriately before this call.
func applyPrecisionLogic(transaction *Transaction) *big.Int {
	// Ensure precision is not zero to avoid division by zero, though ApplyPrecision and ApplyPrecisionWithDBLookup should handle this.
	if transaction.Precision == 0 {
		log.Println("Warning: applyPrecisionLogic called with transaction.Precision = 0. Defaulting to 1.")
		transaction.Precision = 1
	}

	if transaction.PreciseAmount != nil && transaction.PreciseAmount.Cmp(big.NewInt(0)) > 0 {
		convertPreciseToDecimal(transaction)
		return transaction.PreciseAmount
	}

	transaction.PreciseAmount = convertDecimalToPrecise(transaction)
	return transaction.PreciseAmount
}

// fetchTransactionPrecisionFromDB is a placeholder for fetching precision from the database.
// In a real application, this would query your database.
// It returns the precision, a boolean indicating if found, and an error.
func fetchTransactionPrecisionFromDB(db *sql.DB, transactionID string) (float64, bool, error) {
	// Check cache first
	if precision, found := precisionCache[transactionID]; found {
		log.Printf("Cache hit for transaction ID %s: precision %f", transactionID, precision)
		return precision, true, nil
	}

	var precision float64
	// First try to find precision by transaction_id
	query := `
		SELECT precision 
		FROM blnk.transactions 
		WHERE transaction_id = $1 
		OR parent_transaction = $1
		LIMIT 1`
	err := db.QueryRow(query, transactionID).Scan(&precision)
	if err == sql.ErrNoRows {
		return 0, false, nil
	}
	if err != nil {
		log.Printf("Error querying precision for transaction ID %s: %v", transactionID, err)
		return 0, false, err
	}
	if precision <= 0 { // Or any other validation for valid precision
		log.Printf("Invalid precision %f found in DB for transaction ID %s", precision, transactionID)
		return 0, false, nil // Treat invalid precision as not found for fallback logic
	}

	// Store in cache
	precisionCache[transactionID] = precision
	log.Printf("Cache miss for transaction ID %s: fetched precision %f from DB and cached", transactionID, precision)
	return precision, true, nil
}

// ApplyPrecisionWithDBLookup attempts to fetch precision from the database
// and then applies it to the transaction. Falls back to transaction-defined
// precision or a default of 1 if DB lookup fails or precision is invalid.
func ApplyPrecisionWithDBLookup(transaction *Transaction, ds *pgconn.Datasource) *big.Int {
	var dbPrecision float64
	var found bool
	var err error

	if ds != nil && ds.Conn != nil && transaction.TransactionID != "" {
		dbPrecision, found, err = fetchTransactionPrecisionFromDB(ds.Conn, transaction.TransactionID)
		if err != nil {
			log.Printf("Error fetching precision from DB for transaction %s: %v. Will use local/default precision.", transaction.TransactionID, err)
			// Fall through to use local or default precision
		}
	} else {
		log.Println("Datasource, DB connection, or TransactionID is nil/empty; skipping DB lookup for precision.")
	}

	if found && dbPrecision > 0 {
		transaction.Precision = dbPrecision
		log.Printf("Using precision %f from DB for transaction %s", transaction.Precision, transaction.TransactionID)
	} else {
		if transaction.Precision == 0 {
			log.Printf("Precision not found in DB or invalid for transaction %s (or DB lookup skipped). Defaulting precision to 1.", transaction.TransactionID)
			transaction.Precision = 1
		} else {
			log.Printf("Precision not found in DB or invalid for transaction %s (or DB lookup skipped). Using pre-set precision: %f", transaction.TransactionID, transaction.Precision)
		}
	}

	return applyPrecisionLogic(transaction)
}

// convertPreciseToDecimal converts the precise integer amount to a decimal value
// by dividing by precision, storing the exact string representation
func convertPreciseToDecimal(transaction *Transaction) {
	// Use the decimal package for exact decimal arithmetic
	preciseAmountStr := transaction.PreciseAmount.String()
	preciseAmountDec, _ := decimal.NewFromString(preciseAmountStr)

	// Create decimal for precision
	precisionDec := decimal.NewFromFloat(transaction.Precision)

	// Perform division with exact decimal arithmetic
	resultDec := preciseAmountDec.Div(precisionDec)

	// Store the exact string representation
	transaction.AmountString = resultDec.String()

	// Also store the float64 for backward compatibility
	// This may still have precision issues but is kept for existing code
	transaction.Amount, _ = resultDec.Float64()
}

// convertDecimalToPrecise converts a decimal amount to precise integer
// by multiplying by precision
func convertDecimalToPrecise(transaction *Transaction) *big.Int {
	// We should avoid float multiplication due to precision loss
	// Convert the components to strings first and use the decimal package

	// Using decimal package approach
	amountStr := strconv.FormatFloat(transaction.Amount, 'f', -1, 64)
	precisionStr := strconv.FormatFloat(transaction.Precision, 'f', 0, 64)

	amountDec, _ := decimal.NewFromString(amountStr)
	precisionDec, _ := decimal.NewFromString(precisionStr)

	preciseAmount := amountDec.Mul(precisionDec)

	// Convert to big.Int
	result := new(big.Int)
	result.SetString(preciseAmount.String(), 10)

	return result
}

// ApplyRate applies the exchange rate to the precise amount and returns a *big.Int.
// The rate is applied after precision to maintain accuracy.
func ApplyRate(preciseAmount *big.Int, rate float64) *big.Int {
	if rate == 0 {
		rate = 1
	}

	// Create a new big.Float from the precise amount
	preciseAmountFloat := new(big.Float).SetInt(preciseAmount)

	// Create a big.Float for the rate
	rateFloat := new(big.Float).SetFloat64(rate)

	// Multiply the amount by the rate
	result := new(big.Float).Mul(preciseAmountFloat, rateFloat)

	// Convert back to big.Int (rounding if necessary)
	resultBigInt := new(big.Int)
	result.Int(resultBigInt)

	return resultBigInt
}

// validate checks if the transaction is valid (e.g., ensuring positive amount).
func (transaction *Transaction) validate() error {
	if transaction.Amount <= 0 && transaction.PreciseAmount == nil {
		return errors.New("transaction amount must be positive")
	}
	return nil
}

// UpdateBalances updates the balances for both the source and destination based on the transaction details.
// It ensures precision is applied and checks for overdraft before updating.
func UpdateBalances(transaction *Transaction, source, destination *Balance) error {
	// Apply precision to get precise amount
	transaction.PreciseAmount = ApplyPrecision(transaction)
	err := transaction.validate()
	if err != nil {
		return err
	}

	// Check if source has sufficient funds
	err = canProcessTransaction(transaction, source)
	if err != nil {
		return err
	}

	source.InitializeBalanceFields()
	destination.InitializeBalanceFields()

	// Update source balance with original precise amount
	source.addDebit(transaction.PreciseAmount, transaction.Inflight)
	source.computeBalance(transaction.Inflight)

	// Calculate destination amount with rate
	destinationAmount := ApplyRate(transaction.PreciseAmount, transaction.Rate)

	// Update destination balance with rate-adjusted amount
	destination.addCredit(destinationAmount, transaction.Inflight)
	destination.computeBalance(transaction.Inflight)

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
