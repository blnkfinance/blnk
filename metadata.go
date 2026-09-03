package blnk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/notification"
	"github.com/blnkfinance/blnk/model"
	"github.com/google/uuid"
	"github.com/sirupsen/logrus"
)

// ErrEntityNotFound is returned by UpdateMetadata when the target entity
// does not exist. API handlers match it with errors.Is to return 404.
var ErrEntityNotFound = errors.New("entity not found")

// metadataUpdatedWebhookData flattens a resource snapshot into the webhook
// `data` object, matching ledger.created / balance.created / identity.created:
// resource fields sit at the top of `data`, with event_id and timestamp added
// as sibling keys for idempotency.
//
// Contract:
//   - Represents the committed update that triggered the event (not a later
//     re-read of mutable entity state).
//   - event_id uniquely identifies this notification for consumer idempotency.
//   - timestamp is when the event was enqueued (UTC).
//   - Ledgers, balances, and identities include the resource fields plus
//     merged meta_data for this write.
//   - Transactions include transaction_id and meta_data_patch (the JSON
//     object this request applied). The DB JSONB-merges that patch onto the
//     matching row(s); this event does not re-read the merged result.
//
// Delivery is at-least-once via the webhook queue. Ordering per entity is not
// guaranteed when concurrent updates race. Consumers should dedupe on event_id.
func metadataUpdatedWebhookData(entitySnapshot interface{}) (map[string]interface{}, error) {
	raw, err := json.Marshal(entitySnapshot)
	if err != nil {
		return nil, err
	}
	var data map[string]interface{}
	if err := json.Unmarshal(raw, &data); err != nil {
		return nil, err
	}
	if data == nil {
		data = map[string]interface{}{}
	}
	data["event_id"] = uuid.NewString()
	data["timestamp"] = time.Now().UTC()
	return data, nil
}

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

// cloneMetadata returns a shallow copy of src so webhook/index snapshots are
// not affected by later in-place map mutations.
func cloneMetadata(src map[string]interface{}) map[string]interface{} {
	if src == nil {
		return map[string]interface{}{}
	}
	out := make(map[string]interface{}, len(src))
	for k, v := range src {
		out[k] = v
	}
	return out
}

// enqueueMetadataUpdatedWebhook synchronously enqueues a *.metadata.updated
// webhook after a successful metadata persistence. SendWebhook writes the task
// to Redis before returning. A non-nil error is returned to the caller so the
// HTTP handler can fail the request; a client retry of POST .../metadata is a
// merge, so a 5xx after persist is recoverable. No-ops when asynq is unset or
// no webhook URL is configured.
//
// Only called from UpdateMetadata so internal metadata writers stay silent.
func (l *Blnk) enqueueMetadataUpdatedWebhook(entityType string, entitySnapshot interface{}) error {
	if l.asynqClient == nil {
		return nil
	}

	payload, err := metadataUpdatedWebhookData(entitySnapshot)
	if err != nil {
		logrus.WithError(err).WithField("entity_type", entityType).Error("failed to build metadata.updated webhook payload")
		notification.NotifyError(err)
		return err
	}
	if err := l.SendWebhook(NewWebhook{
		Event:   metadataUpdatedEventName(entityType),
		Payload: payload,
	}); err != nil {
		logrus.WithError(err).WithFields(logrus.Fields{
			"event":       metadataUpdatedEventName(entityType),
			"event_id":    payload["event_id"],
			"entity_type": entityType,
		}).Error("failed to enqueue metadata.updated webhook")
		notification.NotifyError(err)
		return err
	}
	return nil
}

// queueMetadataIndex reindexes the committed snapshot asynchronously. Indexing
// remains best-effort and must not block the metadata API response.
func (l *Blnk) queueMetadataIndex(entityType, entityID string, snapshot interface{}) {
	if l.queue == nil {
		return
	}
	go func() {
		if err := l.queue.queueIndexData(entityID, entityType, snapshot); err != nil {
			notification.NotifyError(err)
		}
	}()
}

// ledgerMetadataSnapshot copies ledger with the committed metadata for this update.
func ledgerMetadataSnapshot(ledger *model.Ledger, committedMeta map[string]interface{}) model.Ledger {
	snap := *ledger
	snap.MetaData = cloneMetadata(committedMeta)
	return snap
}

// balanceMetadataSnapshot copies balance with the committed metadata for this update.
func balanceMetadataSnapshot(balance *model.Balance, committedMeta map[string]interface{}) *model.Balance {
	snap := balance.Clone()
	snap.MetaData = cloneMetadata(committedMeta)
	return snap
}

// identityMetadataSnapshot copies identity with the committed metadata for this update.
func identityMetadataSnapshot(identity *model.Identity, committedMeta map[string]interface{}) model.Identity {
	snap := *identity
	snap.MetaData = cloneMetadata(committedMeta)
	return snap
}

// transactionMetadataSnapshot is the immutable body for transaction metadata
// updates. meta_data_patch is the object this request applied (the DB merges
// it onto matching rows); it is not a re-read of the merged meta_data column.
func transactionMetadataSnapshot(entityID string, patch map[string]interface{}) map[string]interface{} {
	return map[string]interface{}{
		"transaction_id":  entityID,
		"meta_data_patch": cloneMetadata(patch),
	}
}

// UpdateMetadata updates the metadata for a given entity ID.
// After a successful DB write it synchronously enqueues a *.metadata.updated
// webhook (durable Redis task). Enqueue failure is returned so the HTTP layer
// can 5xx and the client can retry the merge. Typesense reindex is async.
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

	switch entityType {
	case "ledgers":
		ledger, err := l.GetLedgerByID(entityID)
		if err != nil {
			return nil, ErrEntityNotFound
		}
		mergedMetadata := mergeMetadata(ledger.MetaData, newMetadata)
		if err := l.updateEntityMetadata(ctx, entityType, entityID, mergedMetadata); err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}

		snapshot := ledgerMetadataSnapshot(ledger, mergedMetadata)
		l.queueMetadataIndex(entityType, entityID, snapshot)
		if err := l.enqueueMetadataUpdatedWebhook(entityType, snapshot); err != nil {
			return mergedMetadata, err
		}
		return mergedMetadata, nil

	case "transactions":
		exists, err := l.datasource.TransactionExistsByIDOrParentID(ctx, entityID)
		if err != nil {
			return nil, err
		}
		if !exists {
			return nil, ErrEntityNotFound
		}

		// Apply metadata updates directly without reading current metadata so
		// child rows keep DB-side JSONB merge behaviour.
		if err := l.updateEntityMetadata(ctx, entityType, entityID, newMetadata); err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}

		snapshot := transactionMetadataSnapshot(entityID, newMetadata)
		l.queueMetadataIndex(entityType, entityID, snapshot)
		if err := l.enqueueMetadataUpdatedWebhook(entityType, snapshot); err != nil {
			return newMetadata, err
		}
		return newMetadata, nil

	case "balances":
		balance, err := l.GetBalanceByID(ctx, entityID, nil, false)
		if err != nil {
			return nil, ErrEntityNotFound
		}
		mergedMetadata := mergeMetadata(balance.MetaData, newMetadata)
		if err := l.updateEntityMetadata(ctx, entityType, entityID, mergedMetadata); err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}

		snapshot := balanceMetadataSnapshot(balance, mergedMetadata)
		l.queueMetadataIndex(entityType, entityID, snapshot)
		if err := l.enqueueMetadataUpdatedWebhook(entityType, snapshot); err != nil {
			return mergedMetadata, err
		}
		return mergedMetadata, nil

	case "identities":
		identity, err := l.GetIdentity(entityID)
		if err != nil {
			return nil, ErrEntityNotFound
		}
		mergedMetadata := mergeMetadata(identity.MetaData, newMetadata)
		if err := l.updateEntityMetadata(ctx, entityType, entityID, mergedMetadata); err != nil {
			return nil, fmt.Errorf("failed to update metadata: %w", err)
		}

		snapshot := identityMetadataSnapshot(identity, mergedMetadata)
		l.queueMetadataIndex(entityType, entityID, snapshot)
		if err := l.enqueueMetadataUpdatedWebhook(entityType, snapshot); err != nil {
			return mergedMetadata, err
		}
		return mergedMetadata, nil

	default:
		return nil, fmt.Errorf("unsupported entity type: %s", entityType)
	}
}

// mergeMetadata returns a new map with keys from current overwritten by new.
// Neither input map is mutated.
func mergeMetadata(current, new map[string]interface{}) map[string]interface{} {
	out := make(map[string]interface{}, len(current)+len(new))
	for k, v := range current {
		out[k] = v
	}
	for k, v := range new {
		out[k] = v
	}
	return out
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
