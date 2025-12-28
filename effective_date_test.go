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

package blnk

import (
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
)

// TestSetTransactionMetadata_EffectiveDateDefault tests that setTransactionMetadata
// sets EffectiveDate to CreatedAt when EffectiveDate is nil
func TestSetTransactionMetadata_EffectiveDateDefault(t *testing.T) {
	t.Run("EffectiveDate nil - should default to CreatedAt", func(t *testing.T) {
		txn := &model.Transaction{
			Amount:        100.0,
			Precision:     100,
			EffectiveDate: nil, // Not set
		}

		setTransactionMetadata(txn)

		// CreatedAt should be set
		assert.False(t, txn.CreatedAt.IsZero(), "CreatedAt should be set")

		// EffectiveDate should now be set and equal to CreatedAt
		assert.NotNil(t, txn.EffectiveDate, "EffectiveDate should be set when nil")
		assert.True(t, txn.EffectiveDate.Equal(txn.CreatedAt),
			"EffectiveDate should equal CreatedAt when defaulted")
	})

	t.Run("EffectiveDate already set - should not be overwritten", func(t *testing.T) {
		customDate := time.Date(2024, 1, 15, 10, 0, 0, 0, time.UTC)
		txn := &model.Transaction{
			Amount:        100.0,
			Precision:     100,
			EffectiveDate: &customDate, // Explicitly set
		}

		setTransactionMetadata(txn)

		// CreatedAt should be set to now
		assert.False(t, txn.CreatedAt.IsZero(), "CreatedAt should be set")

		// EffectiveDate should remain unchanged
		assert.NotNil(t, txn.EffectiveDate, "EffectiveDate should still be set")
		assert.True(t, txn.EffectiveDate.Equal(customDate),
			"EffectiveDate should remain as the custom date, got %v", txn.EffectiveDate)
	})

	t.Run("Backdated transaction - EffectiveDate before CreatedAt", func(t *testing.T) {
		// Simulate a backdated transaction (e.g., importing historical data)
		pastDate := time.Now().Add(-7 * 24 * time.Hour) // 7 days ago
		txn := &model.Transaction{
			Amount:        100.0,
			Precision:     100,
			EffectiveDate: &pastDate,
		}

		setTransactionMetadata(txn)

		// EffectiveDate should remain in the past
		assert.True(t, txn.EffectiveDate.Before(txn.CreatedAt),
			"Backdated EffectiveDate should remain before CreatedAt")
		assert.True(t, txn.EffectiveDate.Equal(pastDate),
			"EffectiveDate should not be modified for backdated transactions")
	})
}
