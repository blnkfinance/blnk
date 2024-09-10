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
	PreciseAmount      int64                  `json:"precise_amount,omitempty"`
	Amount             float64                `json:"amount"`
	Rate               float64                `json:"rate"`
	Precision          float64                `json:"precision"`
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
	GroupIds           []string               `json:"-"`
	Sources            []Distribution         `json:"sources,omitempty"`
	Destinations       []Distribution         `json:"destinations,omitempty"`
	CreatedAt          time.Time              `json:"created_at"`
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

// CalculateDistributions calculates and returns the amount for each identifier (source or destination) based on its distribution.
func CalculateDistributions(ctx context.Context, totalAmount float64, distributions []Distribution) (map[string]float64, error) {
	_, span := tracer.Start(ctx, "CalculateDistributions")
	defer span.End()

	resultDistributions := make(map[string]float64)
	var amountLeft = totalAmount
	var totalPercentage float64 = 0
	var fixedTotal float64 = 0

	span.AddEvent("Starting distribution calculation", trace.WithAttributes(
		attribute.Float64("total_amount", totalAmount),
		attribute.Int("distribution.count", len(distributions)),
	))

	// First pass: calculate fixed and percentage amounts, track total percentage
	for _, dist := range distributions {
		if dist.Distribution == "left" {
			continue // Handle "left" distribution later
		} else if dist.Distribution[len(dist.Distribution)-1] == '%' {
			// Percentage distribution
			percentage, err := strconv.ParseFloat(dist.Distribution[:len(dist.Distribution)-1], 64)
			if err != nil {
				span.RecordError(err)
				return nil, errors.New("invalid percentage format")
			}
			totalPercentage += percentage
			amount := (percentage / 100) * totalAmount
			resultDistributions[dist.Identifier] = amount
			amountLeft -= amount
			span.AddEvent("Calculated percentage distribution", trace.WithAttributes(
				attribute.String("identifier", dist.Identifier),
				attribute.Float64("percentage", percentage),
				attribute.Float64("amount", amount),
			))
		} else {
			// Fixed amount distribution
			fixedAmount, err := strconv.ParseFloat(dist.Distribution, 64)
			if err != nil {
				span.RecordError(err)
				return nil, errors.New("invalid fixed amount format")
			}
			if fixedAmount > amountLeft {
				err := errors.New("fixed amount exceeds remaining transaction amount")
				span.RecordError(err)
				return nil, err
			}
			resultDistributions[dist.Identifier] = fixedAmount
			fixedTotal += fixedAmount
			amountLeft -= fixedAmount
			span.AddEvent("Calculated fixed amount distribution", trace.WithAttributes(
				attribute.String("identifier", dist.Identifier),
				attribute.Float64("fixed_amount", fixedAmount),
				attribute.Float64("amount_left", amountLeft),
			))
		}
	}

	// Validate total percentage and fixed amounts do not exceed 100% or total amount
	if totalPercentage > 100 || fixedTotal > totalAmount {
		err := errors.New("total distributions exceed 100% or total amount")
		span.RecordError(err)
		return nil, err
	}

	// Second pass: calculate "left" distribution
	for _, dist := range distributions {
		if dist.Distribution == "left" {
			if _, exists := resultDistributions[dist.Identifier]; exists {
				err := errors.New("multiple identifiers with 'left' distribution")
				span.RecordError(err)
				return nil, err
			}
			resultDistributions[dist.Identifier] = amountLeft
			span.AddEvent("Calculated 'left' distribution", trace.WithAttributes(
				attribute.String("identifier", dist.Identifier),
				attribute.Float64("amount_left", amountLeft),
			))
			break // Only one identifier should have "left" distribution
		}
	}

	span.AddEvent("Distribution calculation completed", trace.WithAttributes(
		attribute.Float64("amount_left", amountLeft),
		attribute.Int("result.count", len(resultDistributions)),
	))

	return resultDistributions, nil
}
