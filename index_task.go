package blnk

import (
	"context"
	"encoding/json"
	"fmt"
)

// IndexModeRefresh marks an index task that carries only an entity reference.
// The worker re-reads Postgres before upserting so queue ordering cannot leave
// Typesense behind a newer metadata commit.
const IndexModeRefresh = "refresh"

// IndexTask is the payload for the Typesense index queue. Legacy tasks embed a
// snapshot in Payload; metadata updates enqueue Mode=refresh with DocumentID or
// ScopeID instead.
type IndexTask struct {
	Collection string                 `json:"collection"`
	Payload    map[string]interface{} `json:"payload,omitempty"`
	DocumentID string                 `json:"document_id,omitempty"`
	ScopeID    string                 `json:"scope_id,omitempty"`
	Mode       string                 `json:"mode,omitempty"`
}

// IndexUpserter indexes one document into a Typesense collection.
type IndexUpserter func(ctx context.Context, collection string, document interface{}) error

// DocumentToIndexMap converts a struct or map into the object Typesense upsert expects.
func DocumentToIndexMap(document interface{}) (map[string]interface{}, error) {
	if m, ok := document.(map[string]interface{}); ok {
		return m, nil
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	return out, nil
}

// ProcessIndexTask handles a single index-queue message: either upsert an
// embedded snapshot (legacy) or refresh from the database (metadata paths).
func (l *Blnk) ProcessIndexTask(ctx context.Context, task IndexTask, upsert IndexUpserter) error {
	if task.Mode == IndexModeRefresh {
		return l.processIndexRefresh(ctx, task, upsert)
	}
	if task.Payload == nil {
		return fmt.Errorf("index task for %q missing payload", task.Collection)
	}
	return upsert(ctx, task.Collection, task.Payload)
}

func (l *Blnk) processIndexRefresh(ctx context.Context, task IndexTask, upsert IndexUpserter) error {
	if task.ScopeID != "" {
		return l.refreshTransactionMetadataScope(ctx, task.ScopeID, upsert)
	}
	if task.DocumentID == "" {
		return fmt.Errorf("refresh index task for %q missing document_id", task.Collection)
	}
	doc, err := l.fetchEntityForIndex(ctx, task.Collection, task.DocumentID)
	if err != nil {
		return err
	}
	return upsert(ctx, task.Collection, doc)
}

func (l *Blnk) fetchEntityForIndex(ctx context.Context, collection, documentID string) (interface{}, error) {
	switch collection {
	case "ledgers":
		return l.GetLedgerByID(documentID)
	case "balances":
		return l.GetBalanceByID(ctx, documentID, nil, false)
	case "identities":
		return l.GetIdentity(documentID)
	case "transactions":
		txn, err := l.GetTransaction(ctx, documentID)
		if err != nil {
			return nil, err
		}
		prepareTransactionForSearchIndex(txn)
		return txn, nil
	default:
		return nil, fmt.Errorf("unsupported refresh collection: %s", collection)
	}
}

func (l *Blnk) refreshTransactionMetadataScope(ctx context.Context, scopeID string, upsert IndexUpserter) error {
	const pageSize = 100
	var offset int64
	for {
		txns, err := l.datasource.ListTransactionsByMetadataScope(ctx, scopeID, pageSize, offset)
		if err != nil {
			return err
		}
		if len(txns) == 0 {
			return nil
		}
		for _, txn := range txns {
			prepareTransactionForSearchIndex(txn)
			if err := upsert(ctx, "transactions", txn); err != nil {
				return err
			}
		}
		if len(txns) < pageSize {
			return nil
		}
		offset += int64(len(txns))
	}
}
