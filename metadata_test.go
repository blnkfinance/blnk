package blnk

import (
	"context"
	"testing"

	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestGetEntityTypeFromID(t *testing.T) {
	tests := []struct {
		name     string
		id       string
		want     string
		wantErr  bool
		errorMsg string
	}{
		{"Transaction ID", "txn_123", "transactions", false, ""},
		{"Bulk Transaction ID", "bulk_123", "transactions", false, ""},
		{"Ledger ID", "ldg_123", "ledgers", false, ""},
		{"Balance ID", "bln_123", "balances", false, ""},
		{"Identity ID", "idt_123", "identities", false, ""},
		{"Invalid ID", "invalid_123", "", true, "invalid entity ID format: invalid_123"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getEntityTypeFromID(tt.id)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Equal(t, tt.errorMsg, err.Error())
			} else {
				assert.NoError(t, err)
				assert.Equal(t, tt.want, got)
			}
		})
	}
}

func TestUpdateMetadata(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	blnk := &Blnk{datasource: mockDS}
	ctx := context.Background()

	t.Run("Update Ledger Metadata", func(t *testing.T) {
		existingMetadata := map[string]interface{}{"existing": "value"}
		ledger := &model.Ledger{MetaData: existingMetadata}
		mockDS.On("GetLedgerByID", "ldg_123").Return(ledger, nil)
		mockDS.On("UpdateLedgerMetadata", "ldg_123", mock.Anything).Return(nil)

		newMetadata := map[string]interface{}{"new": "value"}
		result, err := blnk.UpdateMetadata(ctx, "ldg_123", newMetadata)

		assert.NoError(t, err)
		assert.Contains(t, result, "existing")
		assert.Contains(t, result, "new")
	})

	t.Run("Update Transaction Metadata", func(t *testing.T) {
		// Set up expectations for transaction exists check
		mockDS.On("TransactionExistsByIDOrParentID", ctx, "txn_123").Return(true, nil)

		// Set up expectation for metadata update
		newMetadata := map[string]interface{}{"new": "value"}
		mockDS.On("UpdateTransactionMetadata", ctx, "txn_123", newMetadata).Return(nil)

		result, err := blnk.UpdateMetadata(ctx, "txn_123", newMetadata)

		assert.NoError(t, err)
		assert.Equal(t, newMetadata, result) // Should return just the new metadata
		mockDS.AssertExpectations(t)
	})

	t.Run("Update Balance Metadata", func(t *testing.T) {
		existingMetadata := map[string]interface{}{"existing": "value"}
		balance := &model.Balance{MetaData: existingMetadata}

		mockDS.On("GetBalanceByID", "bln_123", mock.Anything, false).Return(balance, nil)
		mockDS.On("UpdateBalanceMetadata", mock.Anything, "bln_123", mock.Anything).Return(nil)

		newMetadata := map[string]interface{}{"new": "value"}
		result, err := blnk.UpdateMetadata(ctx, "bln_123", newMetadata)

		assert.NoError(t, err)
		assert.Contains(t, result, "existing")
		assert.Contains(t, result, "new")
		mockDS.AssertExpectations(t)
	})

	t.Run("Update Identity Metadata", func(t *testing.T) {
		existingMetadata := map[string]interface{}{"existing": "value"}
		identity := &model.Identity{MetaData: existingMetadata}

		mockDS.On("GetIdentityByID", "idt_123").Return(identity, nil)
		mockDS.On("UpdateIdentityMetadata", "idt_123", mock.Anything).Return(nil)

		newMetadata := map[string]interface{}{"new": "value"}
		result, err := blnk.UpdateMetadata(ctx, "idt_123", newMetadata)

		assert.NoError(t, err)
		assert.Contains(t, result, "existing")
		assert.Contains(t, result, "new")
		mockDS.AssertExpectations(t)
	})

	t.Run("Invalid Entity ID", func(t *testing.T) {
		_, err := blnk.UpdateMetadata(ctx, "invalid_123", map[string]interface{}{})
		assert.Error(t, err)
	})
}

func TestMergeMetadata(t *testing.T) {
	tests := []struct {
		name     string
		current  map[string]interface{}
		new      map[string]interface{}
		expected map[string]interface{}
	}{
		{
			name:     "Merge with empty current",
			current:  nil,
			new:      map[string]interface{}{"new": "value"},
			expected: map[string]interface{}{"new": "value"},
		},
		{
			name:     "Merge with existing values",
			current:  map[string]interface{}{"existing": "value"},
			new:      map[string]interface{}{"new": "value"},
			expected: map[string]interface{}{"existing": "value", "new": "value"},
		},
		{
			name:    "Override existing values",
			current: map[string]interface{}{"key": "old"},
			new:     map[string]interface{}{"key": "new"},
			expected: map[string]interface{}{
				"key": "new",
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := mergeMetadata(tt.current, tt.new)
			assert.Equal(t, tt.expected, result)
		})
	}
}
