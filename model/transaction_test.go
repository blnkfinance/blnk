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
	"reflect"
	"testing"
)

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
			name:        "Exceeding Percentage",
			totalAmount: 1000,
			distributions: []Distribution{
				{Identifier: "A", Distribution: "110%"}, // Exceeding 100%
			},
			want:    nil,
			wantErr: true,
		},
		{
			name:        "Invalid Format",
			totalAmount: 1000,
			distributions: []Distribution{
				{Identifier: "A", Distribution: "abc"}, // Invalid format
			},
			want:    nil,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := CalculateDistributions(context.Background(), tt.totalAmount, tt.distributions)
			if (err != nil) != tt.wantErr {
				t.Errorf("CalculateDistributions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("CalculateDistributions() got = %v, want %v", got, tt.want)
			}
		})
	}
}
