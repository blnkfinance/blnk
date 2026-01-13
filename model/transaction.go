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

// CalculateDistributionsPrecise calculates distributions using big.Int for precision
func CalculateDistributionsPrecise(ctx context.Context, totalPreciseAmount *big.Int, distributions []Distribution, precision int64) (map[string]*big.Int, error) {
	_, span := tracer.Start(ctx, "CalculateDistributionsPrecise")
	defer span.End()

	span.AddEvent("Starting precise distribution calculation", trace.WithAttributes(
		attribute.String("total_precise_amount", totalPreciseAmount.String()),
		attribute.Int("distribution.count", len(distributions)),
	))

	// Convert precision to decimal for calculations
	precisionDec := decimal.NewFromInt(precision)

	// Handle zero total amount case
	if totalPreciseAmount.Cmp(big.NewInt(0)) == 0 {
		result := make(map[string]*big.Int)
		for _, dist := range distributions {
			result[dist.Identifier] = big.NewInt(0)
		}
		return result, nil
	}

	// Convert totalPreciseAmount to decimal for calculations
	totalAmountDec := decimal.NewFromBigInt(totalPreciseAmount, 0)

	resultDistributions := make(map[string]*big.Int)
	amountLeftDec := totalAmountDec
	var totalPercentage decimal.Decimal
	var fixedTotalDec decimal.Decimal

	// Special handling for very small amounts
	minAmountDec := decimal.NewFromInt(1)
	if totalAmountDec.Cmp(minAmountDec) <= 0 && totalAmountDec.Sign() > 0 {
		// Assign the entire amount to the first distribution
		if len(distributions) > 0 {
			resultDistributions[distributions[0].Identifier] = totalPreciseAmount
			for i := 1; i < len(distributions); i++ {
				resultDistributions[distributions[i].Identifier] = big.NewInt(0)
			}
			return resultDistributions, nil
		}
	}

	// First pass: Handle fixed amounts
	fixedAmounts := make(map[string]decimal.Decimal)
	for _, dist := range distributions {
		// Skip percentage and "left" distributions
		isLeftDist := dist.Distribution == "left" || dist.PreciseDistribution == "left"
		isPercentDist := strings.HasSuffix(dist.Distribution, "%")

		if isLeftDist || isPercentDist {
			continue
		}

		var fixedAmountDec decimal.Decimal

		// Check if precise_distribution is provided (already in minor units)
		if dist.PreciseDistribution != "" {
			preciseAmount, err := strconv.ParseInt(dist.PreciseDistribution, 10, 64)
			if err != nil {
				span.RecordError(err)
				return nil, errors.New("invalid precise_distribution format: must be an integer value in minor units")
			}
			fixedAmountDec = decimal.NewFromInt(preciseAmount)
		} else if dist.Distribution != "" {
			// Existing behavior: parse as major units and multiply by precision
			fixedAmount, err := strconv.ParseFloat(dist.Distribution, 64)
			if err != nil {
				span.RecordError(err)
				return nil, errors.New("invalid fixed amount format")
			}
			fixedAmountDec = decimal.NewFromFloat(fixedAmount).Mul(precisionDec)
		} else {
			continue
		}

		if fixedAmountDec.Cmp(amountLeftDec) > 0 {
			err := errors.New("fixed amount exceeds remaining transaction amount")
			span.RecordError(err)
			return nil, err
		}

		fixedAmounts[dist.Identifier] = fixedAmountDec
		fixedTotalDec = fixedTotalDec.Add(fixedAmountDec)
		amountLeftDec = amountLeftDec.Sub(fixedAmountDec)
	}

	// Second pass: Handle percentage distributions
	percentageAmounts := make(map[string]decimal.Decimal)
	for _, dist := range distributions {
		if dist.Distribution == "left" || !strings.HasSuffix(dist.Distribution, "%") {
			continue
		}

		percentageStr := dist.Distribution[:len(dist.Distribution)-1]
		percentage, err := strconv.ParseFloat(percentageStr, 64)
		if err != nil {
			span.RecordError(err)
			return nil, errors.New("invalid percentage format")
		}

		// Using decimal for precise percentage calculation
		percentageDec := decimal.NewFromFloat(percentage)
		totalPercentage = totalPercentage.Add(percentageDec)

		// Calculate raw amount based on percentage of total
		hundredDec := decimal.NewFromInt(100)
		rawAmountDec := totalAmountDec.Mul(percentageDec).Div(hundredDec)

		// Handle very small amounts
		minValueDec := decimal.NewFromInt(1)
		if rawAmountDec.Sign() > 0 && rawAmountDec.Cmp(minValueDec) < 0 {
			percentageAmounts[dist.Identifier] = decimal.Zero
		} else {
			percentageAmounts[dist.Identifier] = rawAmountDec
		}
	}

	// Validate total percentage
	hundredDec := decimal.NewFromInt(100)
	if totalPercentage.Cmp(hundredDec) > 0 || fixedTotalDec.Cmp(totalAmountDec) > 0 {
		err := errors.New("total distributions exceed 100% or total amount")
		span.RecordError(err)
		return nil, err
	}

	// Adjust percentage amounts to maintain total
	if len(percentageAmounts) > 0 {
		targetPercentageTotalDec := totalAmountDec.Mul(totalPercentage).Div(hundredDec)

		// Calculate current total
		currentTotalDec := decimal.Zero
		for _, amount := range percentageAmounts {
			currentTotalDec = currentTotalDec.Add(amount)
		}

		diffDec := targetPercentageTotalDec.Sub(currentTotalDec)

		// If difference is significant, adjust the largest amount
		if diffDec.Abs().Cmp(decimal.NewFromInt(0)) > 0 {
			var largestKey string
			var largestAmountDec decimal.Decimal
			for id, amount := range percentageAmounts {
				if amount.Cmp(largestAmountDec) > 0 {
					largestAmountDec = amount
					largestKey = id
				}
			}

			if largestKey != "" {
				percentageAmounts[largestKey] = percentageAmounts[largestKey].Add(diffDec)
			}
		}
	}

	// Combine all amounts and convert to big.Int
	for id, amountDec := range fixedAmounts {
		resultDistributions[id] = amountDec.BigInt()
	}

	for id, amountDec := range percentageAmounts {
		resultDistributions[id] = amountDec.BigInt()
		amountLeftDec = amountLeftDec.Sub(amountDec)
	}

	// Final pass: Handle "left" distribution
	for _, dist := range distributions {
		if dist.Distribution == "left" || dist.PreciseDistribution == "left" {
			if _, exists := resultDistributions[dist.Identifier]; exists {
				err := errors.New("multiple identifiers with 'left' distribution")
				span.RecordError(err)
				return nil, err
			}

			resultDistributions[dist.Identifier] = amountLeftDec.BigInt()
			break
		}
	}

	// Verify the sum of all distributions equals the total amount
	sumDec := decimal.Zero
	for _, amount := range resultDistributions {
		sumDec = sumDec.Add(decimal.NewFromBigInt(amount, 0))
	}

	if sumDec.Cmp(totalAmountDec) != 0 {
		// Find the largest distribution to adjust
		var largestKey string
		largestAmount := big.NewInt(0)
		for id, amount := range resultDistributions {
			if amount.Cmp(largestAmount) > 0 {
				largestAmount = amount
				largestKey = id
			}
		}

		if largestKey != "" {
			diffDec := totalAmountDec.Sub(sumDec)
			resultDistributions[largestKey] = new(big.Int).Add(
				resultDistributions[largestKey],
				diffDec.BigInt(),
			)
		}
	}

	span.AddEvent("Precise distribution calculation completed")
	return resultDistributions, nil
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
