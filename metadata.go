package blnk

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// getEntityTypeFromID determines the entity type from the ID prefix.
// It analyzes the prefix of the provided ID and returns the corresponding entity type.
//
// Parameters:
// - id: A string representing the entity ID to analyze.
//
// Returns:
// - string: The determined entity type ("transactions", "ledgers", "balances", or "identities").
// - error: An error if the ID format is invalid.
func getEntityTypeFromID(id string) (string, error) {
	switch {
	case strings.HasPrefix(id, "txn_"):
		return "transactions", nil
	case strings.HasPrefix(id, "bulk_"):
		return "transactions", nil
	case strings.HasPrefix(id, "ldg_"):
		return "ledgers", nil
	case strings.HasPrefix(id, "bln_"):
		return "balances", nil
	case strings.HasPrefix(id, "idt_"):
		return "identities", nil
	default:
		return "", fmt.Errorf("invalid entity ID format: %s", id)
	}
}

// UpdateMetadata updates the metadata for a given entity ID.
// It first determines the entity type, retrieves current metadata, merges it with new metadata,
// and updates the entity with the merged metadata.
//
// Parameters:
// - ctx: The context for the operation.
// - entityID: A string representing the ID of the entity to update.
// - newMetadata: A map containing the new metadata to merge.
//
// Returns:
// - map[string]interface{}: The merged metadata after the update.
// - error: An error if the update operation fails.
func (l *Blnk) UpdateMetadata(ctx context.Context, entityID string, newMetadata map[string]interface{}) (map[string]interface{}, error) {
	entityType, err := getEntityTypeFromID(entityID)
	if err != nil {
		return nil, err
	}

	// Check if entity exists first
	switch entityType {
	case "ledgers":
		ledger, err := l.GetLedgerByID(entityID)
		if err != nil {
			return nil, errors.New("entity not found")
		}
		currentMetadata := ledger.MetaData
		mergedMetadata := mergeMetadata(currentMetadata, newMetadata)
		err = l.updateEntityMetadata(ctx, entityType, entityID, mergedMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}
		return mergedMetadata, nil

	case "transactions":
		// Check if transaction exists either by direct ID or as parent ID
		exists, err := l.datasource.TransactionExistsByIDOrParentID(ctx, entityID)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, errors.New("entity not found")
		}

		// Apply metadata updates directly without trying to get current metadata
		// This preserves existing metadata in child transactions
		err = l.updateEntityMetadata(ctx, entityType, entityID, newMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}

		return newMetadata, nil

	case "balances":
		balance, err := l.GetBalanceByID(ctx, entityID, nil, false)
		if err != nil {
			return nil, errors.New("entity not found")
		}
		currentMetadata := balance.MetaData
		mergedMetadata := mergeMetadata(currentMetadata, newMetadata)
		err = l.updateEntityMetadata(ctx, entityType, entityID, mergedMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}
		return mergedMetadata, nil

	case "identities":
		identity, err := l.GetIdentity(entityID)
		if err != nil {
			return nil, errors.New("entity not found")
		}
		currentMetadata := identity.MetaData
		mergedMetadata := mergeMetadata(currentMetadata, newMetadata)
		err = l.updateEntityMetadata(ctx, entityType, entityID, mergedMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}
		return mergedMetadata, nil

	default:
		return nil, fmt.Errorf("unsupported entity type: %s", entityType)
	}
}

// mergeMetadata merges new metadata with existing metadata.
// If the current metadata is nil, it initializes a new map.
//
// Parameters:
// - current: The existing metadata map.
// - new: The new metadata map to merge.
//
// Returns:
// - map[string]interface{}: The merged metadata map.
func mergeMetadata(current, new map[string]interface{}) map[string]interface{} {
	if current == nil {
		current = make(map[string]interface{})
	}

	for k, v := range new {
		current[k] = v
	}

	return current
}

// updateEntityMetadata updates the metadata for a specific entity.
// It routes the update operation to the appropriate datasource method based on the entity type.
//
// Parameters:
// - ctx: The context for the operation.
// - entityType: The type of entity being updated.
// - entityID: The ID of the entity being updated.
// - metadata: The new metadata to set.
//
// Returns:
// - error: An error if the update operation fails.
func (l *Blnk) updateEntityMetadata(ctx context.Context, entityType, entityID string, metadata map[string]interface{}) error {
	switch entityType {
	case "ledgers":
		return l.datasource.UpdateLedgerMetadata(entityID, metadata)

	case "transactions":
		return l.datasource.UpdateTransactionMetadata(ctx, entityID, metadata)

	case "balances":
		return l.datasource.UpdateBalanceMetadata(ctx, entityID, metadata)

	case "identities":
		return l.datasource.UpdateIdentityMetadata(entityID, metadata)

	default:
		return fmt.Errorf("unsupported entity type: %s", entityType)
	}
}
