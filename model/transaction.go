package model

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"
)

type Distribution struct {
	Identifier   string `json:"identifier"`
	Distribution string `json:"distribution"` // Can be a percentage (e.g., "10%"), a fixed amount (e.g., "100"), or "left"
}

type Transaction struct {
	ID                int64                  `json:"-"`
	Amount            int64                  `json:"amount"`
	TransactionID     string                 `json:"id"`
	AllowOverdraft    bool                   `json:"allow_overdraft"`
	Inflight          bool                   `json:"infligt"`
	SkipBalanceUpdate bool                   `json:"-"`
	Source            string                 `json:"source,omitempty"`
	Destination       string                 `json:"destination,omitempty"`
	Reference         string                 `json:"reference"`
	Currency          string                 `json:"currency"`
	Description       string                 `json:"description"`
	Status            string                 `json:"status"`
	Hash              string                 `json:"hash"`
	GroupIds          []string               `json:"group_ids"`
	MetaData          map[string]interface{} `json:"meta_data,omitempty"`
	Sources           []Distribution         `json:"sources,omitempty"`
	Destinations      []Distribution         `json:"destinations,omitempty"`
	CreatedAt         time.Time              `json:"created_at"`
	ScheduledFor      time.Time              `json:"scheduled_for,omitempty"`
}

func (transaction *Transaction) ToJSON() ([]byte, error) {
	return json.Marshal(transaction)
}

func (transaction *Transaction) SplitTransaction() ([]Transaction, error) {
	var ds []Distribution

	if len(transaction.Sources) > 0 {
		ds = transaction.Sources

	} else if len(transaction.Destinations) > 0 {
		ds = transaction.Destinations
	}

	distributions, err := CalculateDistributions(transaction.Amount, ds)
	if err != nil {
		return nil, err
	}

	var transactions []Transaction
	counter := 1
	for direction, amount := range distributions {
		newTransaction := *transaction                               // Create a copy of the original transaction
		newTransaction.TransactionID = GenerateUUIDWithSuffix("txn") // Set the transacrtionid
		newTransaction.Hash = newTransaction.HashTxn()               // Set the transacrtion hash
		newTransaction.Amount = amount                               // Set the amount based on the distribution
		newTransaction.Sources = nil                                 // Clear the Sources slice since we're dealing with individual sources now
		newTransaction.Destinations = nil                            // Clear the Sources slice since we're dealing with individual sources now
		if len(transaction.Sources) > 0 {
			newTransaction.Source = direction // Set the source

		} else if len(transaction.Destinations) > 0 {
			newTransaction.Destination = direction // Set the destination
		}

		newTransaction.Reference = fmt.Sprintf("%s-%d", transaction.Reference, counter)
		counter++
		transactions = append(transactions, newTransaction)
	}

	return transactions, nil
}

// CalculateDistributions calculates and returns the amount for each identifier (source or destination) based on its distribution.
func CalculateDistributions(totalAmount int64, distributions []Distribution) (map[string]int64, error) {
	resultDistributions := make(map[string]int64)
	var amountLeft = totalAmount
	var totalPercentage float64 = 0
	var fixedTotal int64 = 0

	// First pass: calculate fixed and percentage amounts, track total percentage
	for _, dist := range distributions {
		fmt.Println(dist)
		if dist.Distribution == "left" {
			continue // Handle "left" distribution later
		} else if dist.Distribution[len(dist.Distribution)-1] == '%' {
			// Percentage distribution
			percentage, err := strconv.ParseFloat(dist.Distribution[:len(dist.Distribution)-1], 64)
			if err != nil {
				return nil, errors.New("invalid percentage format")
			}
			totalPercentage += percentage
			amount := int64((percentage / 100) * float64(totalAmount))
			resultDistributions[dist.Identifier] = amount
			amountLeft -= amount
		} else {
			// Fixed amount distribution
			fixedAmount, err := strconv.ParseInt(dist.Distribution, 10, 64)
			if err != nil {
				return nil, errors.New("invalid fixed amount format")
			}
			if fixedAmount > amountLeft {
				return nil, errors.New("fixed amount exceeds remaining transaction amount")
			}
			resultDistributions[dist.Identifier] = fixedAmount
			fixedTotal += fixedAmount
			amountLeft -= fixedAmount
		}
	}

	// Validate total percentage and fixed amounts do not exceed 100% or total amount
	if totalPercentage > 100 || fixedTotal > totalAmount {
		return nil, errors.New("total distributions exceed 100% or total amount")
	}

	// Second pass: calculate "left" distribution
	for _, dist := range distributions {
		if dist.Distribution == "left" {
			if _, exists := resultDistributions[dist.Identifier]; exists {
				return nil, errors.New("multiple identifiers with 'left' distribution")
			}
			resultDistributions[dist.Identifier] = amountLeft
			break // Only one identifier should have "left" distribution
		}
	}

	return resultDistributions, nil
}
