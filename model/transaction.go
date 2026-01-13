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
package model

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/shopspring/decimal"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("blnk.model.transaction")

type Distribution struct {
	Identifier          string `json:"identifier"`
	Distribution        string `json:"distribution"`                   // Can be a percentage (e.g., "10%"), a fixed amount (e.g., "100"), or "left"
	PreciseDistribution string `json:"precise_distribution,omitempty"` // Fixed amount in minor units (e.g., "1006" for 1006 cents)
	TransactionID       string `json:"transaction_id"`
}

// distributionState holds the shared state during distribution calculation
type distributionState struct {
	totalAmountDec  decimal.Decimal
	amountLeftDec   decimal.Decimal
	precisionDec    decimal.Decimal
	totalPercentage decimal.Decimal
	fixedTotalDec   decimal.Decimal
	fixedAmounts    map[string]decimal.Decimal
	percentAmounts  map[string]decimal.Decimal
	result          map[string]*big.Int
}

type Transaction struct {
	ID                 int64                  `json:"-"`
	PreciseAmount      *big.Int               `json:"precise_amount,omitempty"`
	Amount             float64                `json:"amount"`
	AmountString       string                 `json:"amount_string,omitempty"`
	Rate               float64                `json:"rate"`
	Precision          float64                `json:"precision"`
	OverdraftLimit     float64                `json:"overdraft_limit"`
	TransactionID      string                 `json:"transaction_id"`
	ParentTransaction  string                 `json:"parent_transaction"`
	Source             string                 `json:"source,omitempty"`
	Destination        string                 `json:"destination,omitempty"`
	Reference          string                 `json:"reference"`
	Currency           string                 `json:"currency"`
	Description        string                 `json:"description,omitempty"`
	Status             string                 `json:"status"`
	Hash               string                 `json:"hash"`
	AllowOverdraft     bool                   `json:"allow_overdraft"`
	Inflight           bool                   `json:"inflight"`
	SkipBalanceUpdate  bool                   `json:"-"`
	SkipQueue          bool                   `json:"skip_queue"`
	Atomic             bool                   `json:"atomic"`
	GroupIds           []string               `json:"-"`
	Sources            []Distribution         `json:"sources,omitempty"`
	Destinations       []Distribution         `json:"destinations,omitempty"`
	CreatedAt          time.Time              `json:"created_at"`
	EffectiveDate      *time.Time             `json:"effective_date,omitempty"`
	ScheduledFor       time.Time              `json:"scheduled_for,omitempty"`
	InflightExpiryDate time.Time              `json:"inflight_expiry_date,omitempty"`
	MetaData           map[string]interface{} `json:"meta_data,omitempty"`
}

func (transaction *Transaction) ToJSON() ([]byte, error) {
	_, span := tracer.Start(context.Background(), "ToJSON")
	defer span.End()

	data, err := json.Marshal(transaction)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}
	span.AddEvent("Transaction serialized to JSON")
	return data, nil
}

func PrecisionBankersRound(num float64, precision float64) float64 {
	// // For standard 2-decimal precision, use the original bankersRound
	// if precision == 100 {
	// 	return bankersRound(num)
	// }

	// Directly use the precision as the scale factor
	// This is more accurate than calculating decimal places with log10
	shifted := num * precision
	whole := math.Floor(shifted)
	fraction := shifted - whole

	// Apply banker's rounding logic (round half to even)
	if math.Abs(fraction-0.5) < 1e-10 {
		if math.Mod(whole, 2) == 0 {
			return whole / precision // Round down for even
		}
		return (whole + 1) / precision // Round up for odd
	}

	return math.Round(shifted) / precision
}

func (t *Transaction) GetEffectiveDate() time.Time {
	if t.EffectiveDate != nil {
		return *t.EffectiveDate
	}
	return t.CreatedAt // Fall back to CreatedAt for old records
}

// isLeftDistribution checks if a distribution is a "left" type
func (d *Distribution) isLeftDistribution() bool {
	return d.Distribution == "left" || d.PreciseDistribution == "left"
}

// isPercentageDistribution checks if a distribution is a percentage type
func (d *Distribution) isPercentageDistribution() bool {
	return strings.HasSuffix(d.Distribution, "%")
}

// processFixedDistributions handles the first pass: fixed amount distributions
func processFixedDistributions(distributions []Distribution, state *distributionState) error {
	for _, dist := range distributions {
		if dist.isLeftDistribution() || dist.isPercentageDistribution() {
			continue
		}

		fixedAmountDec, err := parseFixedAmount(dist, state.precisionDec)
		if err != nil {
			return err
		}
		if fixedAmountDec.IsZero() {
			continue
		}

		if fixedAmountDec.Cmp(state.amountLeftDec) > 0 {
			return errors.New("fixed amount exceeds remaining transaction amount")
		}

		state.fixedAmounts[dist.Identifier] = fixedAmountDec
		state.fixedTotalDec = state.fixedTotalDec.Add(fixedAmountDec)
		state.amountLeftDec = state.amountLeftDec.Sub(fixedAmountDec)
	}
	return nil
}

// parseFixedAmount parses a fixed amount from a distribution
func parseFixedAmount(dist Distribution, precisionDec decimal.Decimal) (decimal.Decimal, error) {
	if dist.PreciseDistribution != "" {
		preciseAmount, err := strconv.ParseInt(dist.PreciseDistribution, 10, 64)
		if err != nil {
			return decimal.Zero, errors.New("invalid precise_distribution format: must be an integer value in minor units")
		}
		return decimal.NewFromInt(preciseAmount), nil
	}

	if dist.Distribution != "" {
		fixedAmount, err := strconv.ParseFloat(dist.Distribution, 64)
		if err != nil {
			return decimal.Zero, errors.New("invalid fixed amount format")
		}
		return decimal.NewFromFloat(fixedAmount).Mul(precisionDec), nil
	}

	return decimal.Zero, nil
}

// processPercentageDistributions handles the second pass: percentage distributions
func processPercentageDistributions(distributions []Distribution, state *distributionState) error {
	hundredDec := decimal.NewFromInt(100)
	minValueDec := decimal.NewFromInt(1)

	for _, dist := range distributions {
		if dist.isLeftDistribution() || !dist.isPercentageDistribution() {
			continue
		}

		percentageStr := dist.Distribution[:len(dist.Distribution)-1]
		percentage, err := strconv.ParseFloat(percentageStr, 64)
		if err != nil {
			return errors.New("invalid percentage format")
		}

		percentageDec := decimal.NewFromFloat(percentage)
		state.totalPercentage = state.totalPercentage.Add(percentageDec)

		rawAmountDec := state.totalAmountDec.Mul(percentageDec).Div(hundredDec)

		if rawAmountDec.Sign() > 0 && rawAmountDec.Cmp(minValueDec) < 0 {
			state.percentAmounts[dist.Identifier] = decimal.Zero
		} else {
			state.percentAmounts[dist.Identifier] = rawAmountDec
		}
	}
	return nil
}

// validateDistributions checks that total distributions don't exceed limits
func validateDistributions(state *distributionState) error {
	hundredDec := decimal.NewFromInt(100)
	if state.totalPercentage.Cmp(hundredDec) > 0 || state.fixedTotalDec.Cmp(state.totalAmountDec) > 0 {
		return errors.New("total distributions exceed 100% or total amount")
	}
	return nil
}

// adjustPercentageAmounts adjusts percentage amounts to maintain the correct total
func adjustPercentageAmounts(state *distributionState) {
	if len(state.percentAmounts) == 0 {
		return
	}

	hundredDec := decimal.NewFromInt(100)
	targetTotal := state.totalAmountDec.Mul(state.totalPercentage).Div(hundredDec)

	currentTotal := decimal.Zero
	for _, amount := range state.percentAmounts {
		currentTotal = currentTotal.Add(amount)
	}

	diff := targetTotal.Sub(currentTotal)
	if diff.Abs().Cmp(decimal.NewFromInt(0)) <= 0 {
		return
	}

	largestKey, largestAmount := findLargestDecimal(state.percentAmounts)
	if largestKey != "" && largestAmount.Sign() > 0 {
		state.percentAmounts[largestKey] = state.percentAmounts[largestKey].Add(diff)
	}
}

// findLargestDecimal finds the key with the largest decimal value
func findLargestDecimal(amounts map[string]decimal.Decimal) (string, decimal.Decimal) {
	var largestKey string
	var largestAmount decimal.Decimal
	for id, amount := range amounts {
		if amount.Cmp(largestAmount) > 0 {
			largestAmount = amount
			largestKey = id
		}
	}
	return largestKey, largestAmount
}

// combineDistributions merges fixed and percentage amounts into the result
func combineDistributions(state *distributionState) {
	for id, amountDec := range state.fixedAmounts {
		state.result[id] = amountDec.BigInt()
	}

	for id, amountDec := range state.percentAmounts {
		state.result[id] = amountDec.BigInt()
		state.amountLeftDec = state.amountLeftDec.Sub(amountDec)
	}
}

// processLeftDistribution handles the "left" distribution type
func processLeftDistribution(distributions []Distribution, state *distributionState) error {
	for _, dist := range distributions {
		if !dist.isLeftDistribution() {
			continue
		}

		if _, exists := state.result[dist.Identifier]; exists {
			return errors.New("multiple identifiers with 'left' distribution")
		}

		state.result[dist.Identifier] = state.amountLeftDec.BigInt()
		break
	}
	return nil
}

// balanceDistributions ensures the sum of all distributions equals the total amount
func balanceDistributions(state *distributionState) {
	sumDec := decimal.Zero
	for _, amount := range state.result {
		sumDec = sumDec.Add(decimal.NewFromBigInt(amount, 0))
	}

	if sumDec.Cmp(state.totalAmountDec) == 0 {
		return
	}

	var largestKey string
	largestAmount := big.NewInt(0)
	for id, amount := range state.result {
		if amount.Cmp(largestAmount) > 0 {
			largestAmount = amount
			largestKey = id
		}
	}

	if largestKey != "" {
		diff := state.totalAmountDec.Sub(sumDec)
		state.result[largestKey] = new(big.Int).Add(state.result[largestKey], diff.BigInt())
	}
}

// CalculateDistributionsPrecise calculates distributions using big.Int for precision
func CalculateDistributionsPrecise(ctx context.Context, totalPreciseAmount *big.Int, distributions []Distribution, precision int64) (map[string]*big.Int, error) {
	_, span := tracer.Start(ctx, "CalculateDistributionsPrecise")
	defer span.End()

	span.AddEvent("Starting precise distribution calculation", trace.WithAttributes(
		attribute.String("total_precise_amount", totalPreciseAmount.String()),
		attribute.Int("distribution.count", len(distributions)),
	))

	if totalPreciseAmount.Cmp(big.NewInt(0)) == 0 {
		return handleZeroAmount(distributions), nil
	}

	totalAmountDec := decimal.NewFromBigInt(totalPreciseAmount, 0)

	if result := handleSmallAmount(totalAmountDec, totalPreciseAmount, distributions); result != nil {
		return result, nil
	}

	state := &distributionState{
		totalAmountDec: totalAmountDec,
		amountLeftDec:  totalAmountDec,
		precisionDec:   decimal.NewFromInt(precision),
		fixedAmounts:   make(map[string]decimal.Decimal),
		percentAmounts: make(map[string]decimal.Decimal),
		result:         make(map[string]*big.Int),
	}

	if err := processFixedDistributions(distributions, state); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := processPercentageDistributions(distributions, state); err != nil {
		span.RecordError(err)
		return nil, err
	}

	if err := validateDistributions(state); err != nil {
		span.RecordError(err)
		return nil, err
	}

	adjustPercentageAmounts(state)
	combineDistributions(state)

	if err := processLeftDistribution(distributions, state); err != nil {
		span.RecordError(err)
		return nil, err
	}

	balanceDistributions(state)

	span.AddEvent("Precise distribution calculation completed")
	return state.result, nil
}

// handleZeroAmount returns zero amounts for all distributions when total is zero
func handleZeroAmount(distributions []Distribution) map[string]*big.Int {
	result := make(map[string]*big.Int)
	for _, dist := range distributions {
		result[dist.Identifier] = big.NewInt(0)
	}
	return result
}

// handleSmallAmount assigns entire amount to first distribution for very small amounts
func handleSmallAmount(totalAmountDec decimal.Decimal, totalPreciseAmount *big.Int, distributions []Distribution) map[string]*big.Int {
	minAmountDec := decimal.NewFromInt(1)
	if totalAmountDec.Cmp(minAmountDec) > 0 || totalAmountDec.Sign() <= 0 || len(distributions) == 0 {
		return nil
	}

	result := make(map[string]*big.Int)
	result[distributions[0].Identifier] = totalPreciseAmount
	for i := 1; i < len(distributions); i++ {
		result[distributions[i].Identifier] = big.NewInt(0)
	}
	return result
}

// Function to integrate the new CalculateDistributionsPrecise into SplitTransaction
func (transaction *Transaction) SplitTransactionPrecise(ctx context.Context) ([]*Transaction, error) {
	ctx, span := tracer.Start(ctx, "SplitTransactionPrecise")
	defer span.End()

	var ds []Distribution
	if len(transaction.Sources) > 0 {
		ds = transaction.Sources
	} else if len(transaction.Destinations) > 0 {
		ds = transaction.Destinations
	}

	span.AddEvent("Starting precise distribution calculation", trace.WithAttributes(
		attribute.String("transaction.id", transaction.TransactionID),
		attribute.String("transaction.precise_amount", transaction.PreciseAmount.String()),
		attribute.Int("distribution.count", len(ds)),
	))

	// Use PreciseAmount for distribution calculation
	precisionInt := int64(transaction.Precision)
	distributions, err := CalculateDistributionsPrecise(ctx, transaction.PreciseAmount, ds, precisionInt)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var transactions []*Transaction
	counter := 1
	for direction, preciseAmount := range distributions {
		newTransaction := *transaction                               // Create a copy of the original transaction
		newTransaction.TransactionID = GenerateUUIDWithSuffix("txn") // Set the transaction ID
		newTransaction.PreciseAmount = preciseAmount                 // Set the precise amount based on the distribution

		// Convert PreciseAmount to Amount for backward compatibility
		convertPreciseToDecimal(&newTransaction)

		newTransaction.Hash = newTransaction.HashTxn()               // Set the transaction hash
		newTransaction.Sources = nil                                 // Clear the Sources slice
		newTransaction.Destinations = nil                            // Clear the Destinations slice
		newTransaction.ParentTransaction = transaction.TransactionID // Set the parent transaction ID

		if len(transaction.Sources) > 0 {
			newTransaction.Source = direction // Set the source
			transaction.Sources[counter-1].TransactionID = newTransaction.TransactionID
		} else if len(transaction.Destinations) > 0 {
			newTransaction.Destination = direction // Set the destination
			transaction.Destinations[counter-1].TransactionID = newTransaction.TransactionID
		}

		newTransaction.Reference = fmt.Sprintf("%s-%d", transaction.Reference, counter)
		counter++
		transactions = append(transactions, &newTransaction)

		span.AddEvent("Created new transaction", trace.WithAttributes(
			attribute.String("transaction.id", newTransaction.TransactionID),
			attribute.String("transaction.precise_amount", newTransaction.PreciseAmount.String()),
			attribute.String("transaction.source", newTransaction.Source),
			attribute.String("transaction.destination", newTransaction.Destination),
		))
	}

	span.AddEvent("Transaction split completed", trace.WithAttributes(
		attribute.Int("new_transactions.count", len(transactions)),
	))
	return transactions, nil
}

// BulkTransactionRequest encapsulates the data needed for a bulk transaction request.
type BulkTransactionRequest struct {
	Transactions []*Transaction `json:"transactions"`
	Inflight     bool           `json:"inflight"`
	Atomic       bool           `json:"atomic"`
	RunAsync     bool           `json:"run_async"`
	SkipQueue    bool           `json:"skip_queue"`
}

// BulkTransactionResult represents the outcome of a bulk transaction operation.
type BulkTransactionResult struct {
	BatchID          string `json:"batch_id"`
	Status           string `json:"status"` // e.g., "processing", "applied", "inflight", "failed"
	TransactionCount int    `json:"transaction_count,omitempty"`
	Error            string `json:"error,omitempty"`
}
