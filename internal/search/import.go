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

package search

import (
	"context"
	"fmt"

	"github.com/typesense/typesense-go/typesense/api"
)

type importFailure struct {
	Index int
	Error string
}

// importDocuments upserts docs with one request. Typesense replicates each
// HTTP write as a single Raft log entry, so this costs one commit per page
// instead of one per document. Documents are normalized the same way
// HandleNotification normalizes them, so the two paths produce identical
// documents. A document that cannot be normalized or that Typesense rejects
// is returned as an importFailure; an error means nothing in docs was indexed.
func (t *TypesenseClient) importDocuments(ctx context.Context, table string, docs []map[string]interface{}) ([]importFailure, error) {
	config, ok := collectionConfigs[table]
	if !ok {
		return nil, fmt.Errorf("unknown collection: %s", table)
	}
	var failures []importFailure
	payload := make([]interface{}, 0, len(docs))
	positions := make([]int, 0, len(docs))
	for i, data := range docs {
		if err := t.processMetadata(data); err != nil {
			failures = append(failures, importFailure{Index: i, Error: err.Error()})
			continue
		}
		t.convertLargeNumbers(config, data)
		t.ensureSchemaFields(config, data)
		t.normalizeTimeFields(config, data)
		if id, ok := data[config.IDField].(string); ok && id != "" {
			data["id"] = id
		}
		payload = append(payload, data)
		positions = append(positions, i)
	}
	if len(payload) == 0 {
		return failures, nil
	}

	action := "upsert"
	results, err := t.Client.Collection(table).Documents().Import(ctx, payload, &api.ImportDocumentsParams{Action: &action})
	if err != nil {
		return nil, fmt.Errorf("failed to import documents into Typesense: %w", err)
	}

	for i, result := range results {
		if result.Success {
			continue
		}
		failures = append(failures, importFailure{Index: positions[i], Error: result.Error})
	}
	return failures, nil
}
