package model

import (
	"reflect"
	"testing"
)

func TestCalculateDistributions(t *testing.T) {
	tests := []struct {
		name          string
		totalAmount   int64
		distributions []Distribution
		want          map[string]int64
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
			want: map[string]int64{
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
			got, err := CalculateDistributions(tt.totalAmount, tt.distributions)
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
