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
	"testing"
)

func floatEquals(a, b float64) bool {
	const epsilon = 1e-9
	return math.Abs(a-b) < epsilon
}

func floatMapsEqual(got, want map[string]float64) bool {
	if len(got) != len(want) {
		return false
	}
	for k, v := range want {
		if gotVal, ok := got[k]; !ok || !floatEquals(gotVal, v) {
			return false
		}
	}
	return true
}

func TestCalculateDistributions(t *testing.T) {
	tests := []struct {
		name          string
		totalAmount   float64
		distributions []Distribution
		want          map[string]float64
		wantErr       bool
	}{
		{
			name:        "Fixed and Percentage",
			totalAmount: 1000,
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
			distributions: []Distribution{
				{Identifier: "@TaxiPay_REV", Distribution: "50%"},
				{Identifier: "@B_REV", Distribution: "45%"},
				{Identifier: "@Assoc_REV", Distribution: "left"},
			},
			want: map[string]float64{
				"@TaxiPay_REV": 0.74, // 50% of 1.5
				"@B_REV":       0.68, // 45% of 1.5
				"@Assoc_REV":   0.08, // 5% of 1.5
			},
			wantErr: false,
		},
		{
			name:        "Multiple Fixed Amounts",
			totalAmount: 1000,
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
			distributions: []Distribution{
				{Identifier: "A", Distribution: "500"},
				{Identifier: "B", Distribution: "25%"},
				{Identifier: "C", Distribution: "10%"},
				{Identifier: "D", Distribution: "left"},
			},
			want: map[string]float64{
				"A": 500.00,
				"B": 308.64,
				"C": 123.46,
				"D": 302.46,
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CalculateDistributions(context.Background(), tt.totalAmount, tt.distributions)
			if (err != nil) != tt.wantErr {
				t.Errorf("CalculateDistributions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !floatMapsEqual(got, tt.want) {
				t.Errorf("CalculateDistributions() got = %v, want %v", got, tt.want)
			}
		})
	}
}
