package blnk

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/blnkfinance/blnk/internal/notification"
)

// ErrEntityNotFound is returned by UpdateMetadata when the target entity
// does not exist. API handlers match it with errors.Is to return 404.
var ErrEntityNotFound = errors.New("entity not found")

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

// metadataUpdatedEventName returns the webhook event name for a metadata update
// on the given entity type collection name (e.g. "ledgers" → "ledger.metadata.updated").
func metadataUpdatedEventName(entityType string) string {
	switch entityType {
	case "ledgers":
		return "ledger.metadata.updated"
	case "balances":
		return "balance.metadata.updated"
	case "identities":
		return "identity.metadata.updated"
	case "transactions":
		return "transaction.metadata.updated"
	default:
		return entityType + ".metadata.updated"
	}
}

// postMetadataUpdateActions reindexes the updated entity in Typesense and emits a
// metadata-updated webhook. Only called from UpdateMetadata (the public metadata
// API) so internal writers that use updateEntityMetadata or the datasource
// directly do not spam consumers.
//
// asynqClient is nil-guarded so unit tests that construct &Blnk{datasource: mock}
// without NewBlnk do not panic in the goroutine. When neither queue nor asynq
// is configured, this is a no-op (same as the previous Typesense-only path).
func (l *Blnk) postMetadataUpdateActions(entityType, entityID string, fallbackMetadata map[string]interface{}) {
	if l.queue == nil && l.asynqClient == nil {
		return
	}

	go func() {
		payload, canIndex := l.loadMetadataUpdatePayload(entityType, entityID, fallbackMetadata)

		if canIndex && l.queue != nil {
			if err := l.queue.queueIndexData(entityID, entityType, payload); err != nil {
				notification.NotifyError(err)
			}
		}

		if l.asynqClient == nil {
			return
		}
		if err := l.SendWebhook(NewWebhook{
			Event:   metadataUpdatedEventName(entityType),
			Payload: payload,
		}); err != nil {
			notification.NotifyError(err)
		}
	}()
}

// loadMetadataUpdatePayload re-fetches the entity after a metadata write.
// On success it returns the full entity (suitable for Typesense and webhooks).
// On fetch failure it returns a minimal map with the entity id and written
// metadata so consumers still get a webhook.
func (l *Blnk) loadMetadataUpdatePayload(entityType, entityID string, fallbackMetadata map[string]interface{}) (interface{}, bool) {
	switch entityType {
	case "ledgers":
		updated, err := l.GetLedgerByID(entityID)
		if err == nil {
			return updated, true
		}
		return map[string]interface{}{
			"ledger_id": entityID,
			"meta_data": fallbackMetadata,
		}, false

	case "balances":
		updated, err := l.GetBalanceByID(context.Background(), entityID, nil, false)
		if err == nil {
			return updated, true
		}
		return map[string]interface{}{
			"balance_id": entityID,
			"meta_data":  fallbackMetadata,
		}, false

	case "identities":
		updated, err := l.GetIdentity(entityID)
		if err == nil {
			return updated, true
		}
		return map[string]interface{}{
			"identity_id": entityID,
			"meta_data":   fallbackMetadata,
		}, false

	case "transactions":
		updated, err := l.GetTransaction(context.Background(), entityID)
		if err == nil {
			return updated, true
		}
		// Parent/bulk IDs may not resolve to a single transaction row; still
		// notify with enough context for consumers to identify the update.
		return map[string]interface{}{
			"transaction_id": entityID,
			"meta_data":      fallbackMetadata,
		}, false

	default:
		return map[string]interface{}{
			"id":        entityID,
			"meta_data": fallbackMetadata,
		}, false
	}
}

// UpdateMetadata updates the metadata for a given entity ID.
// It determines the entity type, merges metadata (except transactions, which
// are merged in the database), persists the update, then asynchronously
// reindexes the entity and emits a *.metadata.updated webhook.
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
			return nil, ErrEntityNotFound
		}
		currentMetadata := ledger.MetaData
		mergedMetadata := mergeMetadata(currentMetadata, newMetadata)
		err = l.updateEntityMetadata(ctx, entityType, entityID, mergedMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}

		l.postMetadataUpdateActions(entityType, entityID, mergedMetadata)
		return mergedMetadata, nil

	case "transactions":
		// Check if transaction exists either by direct ID or as parent ID
		exists, err := l.datasource.TransactionExistsByIDOrParentID(ctx, entityID)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrEntityNotFound
		}

		// Apply metadata updates directly without trying to get current metadata
		// This preserves existing metadata in child transactions
		err = l.updateEntityMetadata(ctx, entityType, entityID, newMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}

		l.postMetadataUpdateActions(entityType, entityID, newMetadata)
		return newMetadata, nil

	case "balances":
		balance, err := l.GetBalanceByID(ctx, entityID, nil, false)
		if err != nil {
			return nil, ErrEntityNotFound
		}
		currentMetadata := balance.MetaData
		mergedMetadata := mergeMetadata(currentMetadata, newMetadata)
		err = l.updateEntityMetadata(ctx, entityType, entityID, mergedMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}

		l.postMetadataUpdateActions(entityType, entityID, mergedMetadata)
		return mergedMetadata, nil

	case "identities":
		identity, err := l.GetIdentity(entityID)
		if err != nil {
			return nil, ErrEntityNotFound
		}
		currentMetadata := identity.MetaData
		mergedMetadata := mergeMetadata(currentMetadata, newMetadata)
		err = l.updateEntityMetadata(ctx, entityType, entityID, mergedMetadata)
		if err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}

		l.postMetadataUpdateActions(entityType, entityID, mergedMetadata)
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
