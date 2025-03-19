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
	"math"
	"math/big"
	"testing"

	"github.com/shopspring/decimal"
)

// Helper function for float64 comparisons
func floatEquals(a, b float64) bool {
	const epsilon = 1e-9
	return math.Abs(a-b) < epsilon
}

// Helper function for big.Int map comparisons
func bigIntMapsEqual(got, want map[string]*big.Int) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if gotVal, ok := got[k]; !ok || gotVal.Cmp(v) != 0 {
			return false
		}
	}
	return true
}

// Helper to convert float64 to *big.Int with precision applied
func floatToBigInt(value float64, precision int64) *big.Int {
	decValue := decimal.NewFromFloat(value)
	decPrecision := decimal.NewFromInt(precision)
	preciseValue := decValue.Mul(decPrecision)

	bigIntValue := new(big.Int)
	bigIntValue = preciseValue.BigInt()
	return bigIntValue
}

// Helper to convert map of float64 to map of *big.Int with precision applied
func floatMapToBigIntMap(floatMap map[string]float64, precision int64) map[string]*big.Int {
	bigIntMap := make(map[string]*big.Int)
	for k, v := range floatMap {
		bigIntMap[k] = floatToBigInt(v, precision)
	}
	return bigIntMap
}

func TestCalculateDistributionsPrecise(t *testing.T) {
	tests := []struct {
		name          string
		totalAmount   float64
		precision     int64
		distributions []Distribution
		want          map[string]float64
		wantErr       bool
	}{
		{
			name:        "Fixed and Percentage",
			totalAmount: 1000,
			precision:   100,
			distributions: []Distribution{
				{Identifier: "A", Distribution: "200"},  // Fixed amount
				{Identifier: "B", Distribution: "50%"},  // Percentage
				{Identifier: "C", Distribution: "left"}, // Leftover
			},
			want: map[string]float64{
				"A": 200, // Fixed
				"B": 500, // 50% of 1000
				"C": 300, // Leftover (1000 - 200 - 500)
			},
			wantErr: false,
		},
		{
			name:        "Ride Fee Distribution",
			totalAmount: 1.5,
			precision:   100,
			distributions: []Distribution{
				{Identifier: "@TaxiPay_REV", Distribution: "50%"},
				{Identifier: "@B_REV", Distribution: "45%"},
				{Identifier: "@Assoc_REV", Distribution: "left"},
			},
			want: map[string]float64{
				"@TaxiPay_REV": 0.76, // 50% of 1.5
				"@B_REV":       0.67, // 45% of 1.5
				"@Assoc_REV":   0.07, // 5% of 1.5
			},
			wantErr: false,
		},
		{
			name:        "Multiple Fixed Amounts",
			totalAmount: 1000,
			precision:   100,
			distributions: []Distribution{
				{Identifier: "A", Distribution: "200"},
				{Identifier: "B", Distribution: "300"},
				{Identifier: "C", Distribution: "left"},
			},
			want: map[string]float64{
				"A": 200,
				"B": 300,
				"C": 500,
			},
			wantErr: false,
		},
		{
			name:        "Rounding Edge Case",
			totalAmount: 100.01,
			precision:   100,
			distributions: []Distribution{
				{Identifier: "A", Distribution: "33.33%"},
				{Identifier: "B", Distribution: "33.33%"},
				{Identifier: "C", Distribution: "33.34%"},
			},
			want: map[string]float64{
				"A": 33.33,
				"B": 33.33,
				"C": 33.35,
			},
			wantErr: false,
		},
		{
			name:        "Small Amount Distribution",
			totalAmount: 0.01,
			precision:   100,
			distributions: []Distribution{
				{Identifier: "A", Distribution: "50%"},
				{Identifier: "B", Distribution: "50%"},
			},
			want: map[string]float64{
				"A": 0.01,
				"B": 0.00,
			},
			wantErr: false,
		},
		{
			name:        "Complex Mixed Distribution",
			totalAmount: 1234.56,
			precision:   100,
			distributions: []Distribution{
				{Identifier: "A", Distribution: "500"},
				{Identifier: "B", Distribution: "25%"},
				{Identifier: "C", Distribution: "10%"},
				{Identifier: "D", Distribution: "left"},
			},
			want: map[string]float64{
				"A": 500.01,
				"B": 308.64,
				"C": 123.45,
				"D": 302.46,
			},
			wantErr: false,
		},
		{
			name:        "Crypto Distribution with High Precision",
			totalAmount: 3,
			precision:   1000000,
			distributions: []Distribution{
				{Identifier: "@Dest1", Distribution: "left"},
				{Identifier: "@Dest2", Distribution: "4.300000%"},
			},
			want: map[string]float64{
				"@Dest1": 2.871,
				"@Dest2": 0.129,
			},
			wantErr: false,
		},
	}

	// Test new precise function
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Convert total amount to *big.Int
			totalPreciseAmount := floatToBigInt(tt.totalAmount, tt.precision)

			// Convert expected results to *big.Int
			wantBigInt := floatMapToBigIntMap(tt.want, tt.precision)

			// Call the precise function
			got, err := CalculateDistributionsPrecise(context.Background(), totalPreciseAmount, tt.distributions, tt.precision)
			if (err != nil) != tt.wantErr {
				t.Errorf("CalculateDistributionsPrecise() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			// Compare results
			if !bigIntMapsEqual(got, wantBigInt) {
				t.Errorf("CalculateDistributionsPrecise() got = %v, want %v", got, wantBigInt)

				// Print values for debugging
				t.Logf("Got values (converted to float):")
				for k, v := range got {
					decValue := decimal.NewFromBigInt(v, 0)
					floatValue := decValue.Div(decimal.NewFromInt(tt.precision)).InexactFloat64()
					t.Logf("  %s: %v", k, floatValue)
				}

				t.Logf("Want values:")
				for k, v := range tt.want {
					t.Logf("  %s: %v", k, v)
				}
			}
		})
	}
}

// Test for SplitTransactionPrecise
func TestSplitTransactionPrecise(t *testing.T) {
	// Sample transaction with sources
	transaction := &Transaction{
		TransactionID: "txn_test_123",
		Reference:     "REF123",
		Amount:        1000.00,
		Precision:     100,
		Sources: []Distribution{
			{Identifier: "A", Distribution: "200"},
			{Identifier: "B", Distribution: "50%"},
			{Identifier: "C", Distribution: "left"},
		},
	}

	// Apply precision to ensure PreciseAmount is set
	ApplyPrecision(transaction)

	// Call the precise split function
	splitTxns, err := transaction.SplitTransactionPrecise(context.Background())
	if err != nil {
		t.Errorf("SplitTransactionPrecise() error = %v", err)
		return
	}

	// Check if we have the expected number of transactions
	expectedCount := 3
	if len(splitTxns) != expectedCount {
		t.Errorf("SplitTransactionPrecise() returned %d transactions, expected %d", len(splitTxns), expectedCount)
	}

	// Check each transaction's amount
	expectedAmounts := map[string]float64{
		"A": 200.00,
		"B": 500.00,
		"C": 300.00,
	}

	for _, txn := range splitTxns {
		// Find which source this transaction corresponds to
		for identifier, expectedAmount := range expectedAmounts {
			if txn.Source == identifier {
				if !floatEquals(txn.Amount, expectedAmount) {
					t.Errorf("Transaction for source %s has amount %v, expected %v",
						identifier, txn.Amount, expectedAmount)
				}

				// Verify PreciseAmount matches Amount
				decPreciseAmount := decimal.NewFromBigInt(txn.PreciseAmount, 0)
				decPrecision := decimal.NewFromFloat(txn.Precision)
				floatAmount := decPreciseAmount.Div(decPrecision).InexactFloat64()

				if !floatEquals(floatAmount, txn.Amount) {
					t.Errorf("Transaction for source %s has PreciseAmount equivalent to %v, expected %v",
						identifier, floatAmount, txn.Amount)
				}

				break
			}
		}
	}
}
