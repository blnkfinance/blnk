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
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

var tracer = otel.Tracer("blnk.model.transaction")

type Distribution struct {
	Identifier    string `json:"identifier"`
	Distribution  string `json:"distribution"` // Can be a percentage (e.g., "10%"), a fixed amount (e.g., "100"), or "left"
	TransactionID string `json:"transaction_id"`
}

type Transaction struct {
	ID                 int64                  `json:"-"`
	PreciseAmount      *big.Int               `json:"precise_amount,omitempty"`
	Amount             float64                `json:"amount"`
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

func (transaction *Transaction) SplitTransaction(ctx context.Context) ([]*Transaction, error) {
	ctx, span := tracer.Start(ctx, "SplitTransaction")
	defer span.End()

	var ds []Distribution
	if len(transaction.Sources) > 0 {
		ds = transaction.Sources
	} else if len(transaction.Destinations) > 0 {
		ds = transaction.Destinations
	}

	span.AddEvent("Starting distribution calculation", trace.WithAttributes(
		attribute.String("transaction.id", transaction.TransactionID),
		attribute.Float64("transaction.amount", transaction.Amount),
		attribute.Int("distribution.count", len(ds)),
	))

	distributions, err := CalculateDistributions(ctx, transaction.Amount, ds)
	if err != nil {
		span.RecordError(err)
		return nil, err
	}

	var transactions []*Transaction
	counter := 1
	for direction, amount := range distributions {
		newTransaction := *transaction                               // Create a copy of the original transaction
		newTransaction.TransactionID = GenerateUUIDWithSuffix("txn") // Set the transaction ID
		newTransaction.Hash = newTransaction.HashTxn()               // Set the transaction hash
		newTransaction.Amount = amount                               // Set the amount based on the distribution
		newTransaction.Sources = nil                                 // Clear the Sources slice since we're dealing with individual sources now
		newTransaction.Destinations = nil                            // Clear the Destinations slice since we're dealing with individual sources now
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
			attribute.Float64("transaction.amount", newTransaction.Amount),
			attribute.String("transaction.source", newTransaction.Source),
			attribute.String("transaction.destination", newTransaction.Destination),
		))
	}

	span.AddEvent("Transaction split completed", trace.WithAttributes(
		attribute.Int("new_transactions.count", len(transactions)),
	))
	return transactions, nil
}

// bankersRound implements round-half-to-even rounding
func bankersRound(num float64) float64 {
	// Handle very small amounts specially
	if math.Abs(num) < 1e-10 {
		return 0
	}

	// Move two decimal places left
	shifted := num * 100
	whole := math.Floor(shifted)
	fraction := shifted - whole

	// If exactly 0.5, round to nearest even number
	if math.Abs(fraction-0.5) < 1e-10 {
		if math.Mod(whole, 2) == 0 {
			return whole / 100 // Round down for even
		}
		return (whole + 1) / 100 // Round up for odd
	}

	return math.Round(shifted) / 100
}

// handleSmallAmount ensures proper handling of very small amounts
func handleSmallAmount(amount float64) float64 {
	if amount > 0 && amount < 0.01 {
		return 0
	}
	return bankersRound(amount)
}

// adjustRoundingDifference distributes rounding difference to maintain total
func adjustRoundingDifference(amounts map[string]float64, targetTotal float64) map[string]float64 {
	// Handle special case for very small total
	if targetTotal <= 0.01 && targetTotal > 0 {
		// Find the first non-zero amount and adjust it
		for id := range amounts {
			amounts[id] = 0
		}
		// Assign the entire amount to the first entry
		if len(amounts) > 0 {
			for id := range amounts {
				amounts[id] = targetTotal
				break
			}
		}
		return amounts
	}

	// Calculate current total after rounding
	currentTotal := 0.0
	for _, amount := range amounts {
		currentTotal = bankersRound(currentTotal + amount)
	}

	diff := bankersRound(targetTotal - currentTotal)
	if math.Abs(diff) < 1e-10 {
		return amounts
	}

	// Find the largest amount to adjust
	var largestKey string
	var largestAmount float64
	for id, amount := range amounts {
		if amount > largestAmount {
			largestAmount = amount
			largestKey = id
		}
	}

	if largestKey != "" {
		amounts[largestKey] = bankersRound(amounts[largestKey] + diff)
	}

	return amounts
}

func CalculateDistributions(ctx context.Context, totalAmount float64, distributions []Distribution) (map[string]float64, error) {
	_, span := tracer.Start(ctx, "CalculateDistributions")
	defer span.End()

	// Handle zero total amount case
	if totalAmount == 0 {
		result := make(map[string]float64)
		for _, dist := range distributions {
			result[dist.Identifier] = 0
		}
		return result, nil
	}

	// Round total amount using banker's rounding
	totalAmount = bankersRound(totalAmount)

	resultDistributions := make(map[string]float64)
	var amountLeft = totalAmount
	var totalPercentage float64 = 0
	var fixedTotal float64 = 0

	span.AddEvent("Starting distribution calculation", trace.WithAttributes(
		attribute.Float64("total_amount", totalAmount),
		attribute.Int("distribution.count", len(distributions)),
	))

	// Special handling for very small amounts (≤ 0.01)
	if totalAmount <= 0.01 {
		// Assign the entire amount to the first distribution
		if len(distributions) > 0 {
			resultDistributions[distributions[0].Identifier] = totalAmount
			for i := 1; i < len(distributions); i++ {
				resultDistributions[distributions[i].Identifier] = 0
			}
			return resultDistributions, nil
		}
	}

	// First pass: Handle fixed amounts
	fixedAmounts := make(map[string]float64)
	for _, dist := range distributions {
		if dist.Distribution == "left" || dist.Distribution[len(dist.Distribution)-1] == '%' {
			continue
		}

		fixedAmount, err := strconv.ParseFloat(dist.Distribution, 64)
		if err != nil {
			span.RecordError(err)
			return nil, errors.New("invalid fixed amount format")
		}

		fixedAmount = bankersRound(fixedAmount)
		if fixedAmount > amountLeft {
			err := errors.New("fixed amount exceeds remaining transaction amount")
			span.RecordError(err)
			return nil, err
		}

		fixedAmounts[dist.Identifier] = fixedAmount
		fixedTotal = bankersRound(fixedTotal + fixedAmount)
		amountLeft = bankersRound(amountLeft - fixedAmount)
	}

	// Second pass: Handle percentage distributions
	percentageAmounts := make(map[string]float64)
	for _, dist := range distributions {
		if dist.Distribution == "left" || dist.Distribution[len(dist.Distribution)-1] != '%' {
			continue
		}

		percentage, err := strconv.ParseFloat(dist.Distribution[:len(dist.Distribution)-1], 64)
		if err != nil {
			span.RecordError(err)
			return nil, errors.New("invalid percentage format")
		}

		totalPercentage = bankersRound(totalPercentage + percentage)
		rawAmount := (percentage / 100) * totalAmount
		percentageAmounts[dist.Identifier] = handleSmallAmount(rawAmount)
	}

	// Validate total percentage
	if totalPercentage > 100 || fixedTotal > totalAmount {
		err := errors.New("total distributions exceed 100% or total amount")
		span.RecordError(err)
		return nil, err
	}

	// Adjust percentage amounts to maintain total
	if len(percentageAmounts) > 0 {
		targetPercentageTotal := bankersRound(totalAmount * (totalPercentage / 100))
		percentageAmounts = adjustRoundingDifference(percentageAmounts, targetPercentageTotal)
	}

	// Combine all amounts
	for id, amount := range fixedAmounts {
		resultDistributions[id] = amount
	}
	for id, amount := range percentageAmounts {
		resultDistributions[id] = amount
		amountLeft = bankersRound(amountLeft - amount)
	}

	// Final pass: Handle "left" distribution
	for _, dist := range distributions {
		if dist.Distribution == "left" {
			if _, exists := resultDistributions[dist.Identifier]; exists {
				err := errors.New("multiple identifiers with 'left' distribution")
				span.RecordError(err)
				return nil, err
			}
			resultDistributions[dist.Identifier] = bankersRound(amountLeft)
			break
		}
	}

	// Final validation and adjustment
	finalTotal := 0.0
	for _, amount := range resultDistributions {
		finalTotal = bankersRound(finalTotal + amount)
	}

	if math.Abs(finalTotal-totalAmount) > 1e-10 {
		resultDistributions = adjustRoundingDifference(resultDistributions, totalAmount)
	}

	span.AddEvent("Distribution calculation completed", trace.WithAttributes(
		attribute.Float64("final_total", finalTotal),
		attribute.Int("result.count", len(resultDistributions)),
	))

	return resultDistributions, nil
}

func (t *Transaction) GetEffectiveDate() time.Time {
	if t.EffectiveDate != nil {
		return *t.EffectiveDate
	}
	return t.CreatedAt // Fall back to CreatedAt for old records
}
