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
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"github.com/typesense/typesense-go/typesense"
	"github.com/typesense/typesense-go/typesense/api"
)

// TypesenseClient wraps the Typesense client and provides methods to interact with it.
type TypesenseClient struct {
	Client *typesense.Client
}

// NotificationPayload represents the payload structure for notifications, containing the table and data.
type NotificationPayload struct {
	Table string                 `json:"table"`
	Data  map[string]interface{} `json:"data"`
}

// NewTypesenseClient initializes and returns a new Typesense client instance.
func NewTypesenseClient(apiKey string, hosts []string) *TypesenseClient {
	client := typesense.NewClient(
		typesense.WithServer(hosts[0]),
		typesense.WithAPIKey(apiKey),
		typesense.WithConnectionTimeout(5*time.Second),
		typesense.WithCircuitBreakerMaxRequests(50),
		typesense.WithCircuitBreakerInterval(2*time.Minute),
		typesense.WithCircuitBreakerTimeout(1*time.Minute),
	)
	return &TypesenseClient{Client: client}
}

// EnsureCollectionsExist ensures that all the necessary collections exist in the Typesense schema.
// If a collection doesn't exist, it will create the collection based on the latest schema.
func (t *TypesenseClient) EnsureCollectionsExist(ctx context.Context) error {
	collections := []string{"ledgers", "balances", "transactions", "reconciliations", "identities"}

	for _, c := range collections {
		latestSchema := getLatestSchema(c)
		if _, err := t.CreateCollection(ctx, latestSchema); err != nil {
			return fmt.Errorf("failed to create collection %s: %w", c, err)
		}
	}

	if err := t.ensureDefaultGeneralLedger(ctx); err != nil {
		logrus.Errorf("failed to ensure default general ledger: %v", err)
	}
	return nil
}

// EnsureDefaultGeneralLedger ensures that the default general ledger exists in Typesense.
func (t *TypesenseClient) ensureDefaultGeneralLedger(ctx context.Context) error {

	data := map[string]interface{}{
		"ledger_id":  "general_ledger_id",
		"name":       "General Ledger",
		"created_at": time.Now().Unix(),
	}

	return t.upsertDocument(ctx, "ledgers", data)
}

// CreateCollection creates a collection in Typesense based on the provided schema.
// If the collection already exists, it will return without error.
func (t *TypesenseClient) CreateCollection(ctx context.Context, schema *api.CollectionSchema) (*api.CollectionResponse, error) {
	resp, err := t.Client.Collections().Create(ctx, schema)
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return nil, nil
		}
		return nil, err
	}
	return resp, nil
}

// Search performs a search query on a specific collection with the provided search parameters.
func (t *TypesenseClient) Search(ctx context.Context, collection string, searchParams *api.SearchCollectionParams) (*api.SearchResult, error) {
	return t.Client.Collection(collection).Documents().Search(ctx, searchParams)
}

// MultiSearchRequest represents a request for multiple searches
type MultiSearchRequest struct {
	Searches []api.MultiSearchSearchesParameter `json:"searches"`
}

// Remove the incorrect MultiSearch method and replace with this:
func (t *TypesenseClient) MultiSearch(ctx context.Context, searchRequests api.MultiSearchSearchesParameter) (*api.MultiSearchResult, error) {
	return t.Client.MultiSearch.Perform(ctx, &api.MultiSearchParams{}, searchRequests)
}

// HandleNotification processes incoming notifications and updates Typesense collections based on the table and data.
// It ensures the required fields exist and upserts the data into Typesense.
func (t *TypesenseClient) HandleNotification(table string, data map[string]interface{}) error {
	ctx := context.Background()
	if err := t.EnsureCollectionsExist(ctx); err != nil {
		logrus.Warningf("Failed to ensure collections exist: %v", err)
	}

	// Process and normalize the data
	if err := t.processMetadata(data); err != nil {
		return err
	}
	t.convertLargeNumbers(table, data)
	t.ensureSchemaFields(table, data)
	t.normalizeTimeFields(data)

	// Upsert the document
	return t.upsertDocument(ctx, table, data)
}

// processMetadata handles metadata field normalization for object schemas
func (t *TypesenseClient) processMetadata(data map[string]interface{}) error {
	if metaData, ok := data["meta_data"]; ok {
		if metaData == nil {
			// If metadata is null, provide an empty object for object type schemas
			data["meta_data"] = make(map[string]interface{})
		} else if metaDataMap, ok := metaData.(map[string]interface{}); ok {
			data["meta_data"] = metaDataMap
		} else {
			// For backward compatibility, convert to string for old schemas
			jsonString, err := json.Marshal(metaData)
			if err != nil {
				return fmt.Errorf("failed to marshal meta_data: %w", err)
			}
			data["meta_data"] = string(jsonString)
		}
	}
	return nil
}

// convertLargeNumbers converts big.Int values to strings for Typesense compatibility
func (t *TypesenseClient) convertLargeNumbers(table string, data map[string]interface{}) {
	switch table {
	case "balances":
		balanceFields := []string{"balance", "credit_balance", "debit_balance", "inflight_balance", "inflight_credit_balance", "inflight_debit_balance"}
		for _, field := range balanceFields {
			t.convertNumberField(data, field)
		}
	case "transactions":
		t.convertNumberField(data, "precise_amount")
	}
}

// convertNumberField converts a single numeric field to string format
func (t *TypesenseClient) convertNumberField(data map[string]interface{}, field string) {
	if val, ok := data[field]; ok {
		switch v := val.(type) {
		case *big.Int:
			data[field] = v.String()
		case float64:
			// Convert scientific notation back to integer string
			data[field] = fmt.Sprintf("%.0f", v)
		}
	}
}

// ensureSchemaFields ensures all required schema fields are present with default values
func (t *TypesenseClient) ensureSchemaFields(table string, data map[string]interface{}) {
	latestSchema := getLatestSchema(table)

	optionalFieldMap := make(map[string]bool)
	for _, field := range latestSchema.Fields {
		if field.Optional != nil && *field.Optional {
			optionalFieldMap[field.Name] = true
		}
	}

	for _, field := range latestSchema.Fields {
		if _, ok := data[field.Name]; !ok {
			isOptional := field.Optional != nil && *field.Optional
			if !isOptional {
				data[field.Name] = getDefaultValue(field.Type)
			}
		}
	}

	for key, value := range data {
		if optionalFieldMap[key] {
			if strVal, ok := value.(string); ok && strVal == "" {
				delete(data, key)
			}
		}
	}
}

// normalizeTimeFields converts time fields to Unix timestamps
func (t *TypesenseClient) normalizeTimeFields(data map[string]interface{}) {
	timeFields := []string{"created_at", "dob", "scheduled_for", "inflight_expiry_date", "inflight_expires_at", "completed_at", "started_at"}
	for _, field := range timeFields {
		if fieldValue, ok := data[field]; ok {
			switch v := fieldValue.(type) {
			case time.Time:
				data[field] = v.Unix()
			case int64:
				// Time already in Unix format, no action needed
			default:
				// Set current time if value type is not recognized
				data[field] = time.Now().Unix()
			}
		}
	}
}

// getIDField returns the primary ID field name for a given table
func (t *TypesenseClient) getIDField(table string) string {
	switch table {
	case "reconciliations":
		return "reconciliation_id"
	case "identities":
		return "identity_id"
	case "ledgers":
		return "ledger_id"
	case "balances":
		return "balance_id"
	case "transactions":
		return "transaction_id"
	default:
		return ""
	}
}

// upsertDocument handles the final upsert operation to Typesense
func (t *TypesenseClient) upsertDocument(ctx context.Context, table string, data map[string]interface{}) error {
	idField := t.getIDField(table)

	if idField != "" {
		if id, ok := data[idField].(string); ok && id != "" {
			// Upsert the document in Typesense with the provided ID
			data["id"] = id
			_, err := t.Client.Collection(table).Documents().Upsert(ctx, data)
			if err != nil {
				return fmt.Errorf("failed to upsert document in Typesense: %w", err)
			}
			return nil
		}
	}

	// For other collections, perform a regular upsert
	_, err := t.Client.Collection(table).Documents().Upsert(ctx, data)
	if err != nil {
		return fmt.Errorf("failed to index document in Typesense: %w", err)
	}

	return nil
}

// MigrateTypeSenseSchema adds new fields from the latest schema to the existing collection schema in Typesense.
// This is useful when the schema has been updated, and new fields need to be added.
func (t *TypesenseClient) MigrateTypeSenseSchema(ctx context.Context, collectionName string) error {
	collection := t.Client.Collection(collectionName)

	currentSchemaResponse, err := collection.Retrieve(ctx)
	if err != nil {
		return fmt.Errorf("failed to retrieve current schema: %w", err)
	}

	currentSchema := &api.CollectionSchema{
		Name:   currentSchemaResponse.Name,
		Fields: currentSchemaResponse.Fields,
	}

	latestSchema := getLatestSchema(collectionName)

	// Compare the current schema with the latest schema and get any new fields.
	newFields := compareSchemas(currentSchema, latestSchema)

	// Add each new field to the collection.
	for _, field := range newFields {
		updateSchema := &api.CollectionUpdateSchema{
			Fields: []api.Field{field},
		}

		_, err := collection.Update(ctx, updateSchema)
		if err != nil {
			return fmt.Errorf("failed to add field %s: %w", field.Name, err)
		}
		logrus.Infof("Added new field %s to collection %s", field.Name, collectionName)
	}

	return nil
}

// compareSchemas compares the old schema with the new schema and returns any new fields that are present in the new schema but not in the old one.
func compareSchemas(oldSchema, newSchema *api.CollectionSchema) []api.Field {
	var newFields []api.Field
	oldFieldMap := make(map[string]bool)

	// Create a map of the old fields.
	for _, field := range oldSchema.Fields {
		oldFieldMap[field.Name] = true
	}

	// Identify new fields that are in the new schema but not in the old schema.
	for _, field := range newSchema.Fields {
		if !oldFieldMap[field.Name] {
			newFields = append(newFields, field)
		}
	}

	return newFields
}

// getDefaultValue returns the default value for a given field type in Typesense.
func getDefaultValue(fieldType string) interface{} {
	switch fieldType {
	case "string":
		return ""
	case "int32", "int64":
		return int64(0)
	case "float":
		return float64(0)
	case "bool":
		return false
	case "string[]":
		return []string{}
	default:
		return nil
	}
}

// getLatestSchema returns the latest schema for a given collection name. This function should be updated whenever the schema changes.
func getLatestSchema(collectionName string) *api.CollectionSchema {
	switch collectionName {
	case "ledgers":
		return getLedgerSchema()
	case "balances":
		return getBalanceSchema()
	case "transactions":
		return getTransactionSchema()
	case "reconciliations":
		return getReconciliationSchema()
	case "identities":
		return getIdentitySchema()
	default:
		return nil
	}
}

// getLedgerSchema returns the schema for the "ledgers" collection.
func getLedgerSchema() *api.CollectionSchema {
	facet := true
	sortBy := "created_at"
	enableNested := true
	return &api.CollectionSchema{
		Name: "ledgers",
		Fields: []api.Field{
			{Name: "ledger_id", Type: "string", Facet: &facet},
			{Name: "name", Type: "string", Facet: &facet},
			{Name: "created_at", Type: "int64", Facet: &facet},
			{Name: "meta_data", Type: "object", Facet: &facet, Optional: &enableNested},
		},
		DefaultSortingField: &sortBy,
		EnableNestedFields:  &enableNested,
	}
}

// getBalanceSchema returns the schema for the "balances" collection.
func getBalanceSchema() *api.CollectionSchema {
	facet := true
	sortBy := "created_at"
	enableNested := true
	identityId := "identities.identity_id"
	ledgerId := "ledgers.ledger_id"
	return &api.CollectionSchema{
		Name: "balances",
		Fields: []api.Field{
			{Name: "balance", Type: "string", Facet: &facet},
			{Name: "version", Type: "int64", Facet: &facet},
			{Name: "inflight_balance", Type: "string", Facet: &facet},
			{Name: "credit_balance", Type: "string", Facet: &facet},
			{Name: "inflight_credit_balance", Type: "string", Facet: &facet},
			{Name: "debit_balance", Type: "string", Facet: &facet},
			{Name: "inflight_debit_balance", Type: "string", Facet: &facet},
			{Name: "precision", Type: "float", Facet: &facet},
			{Name: "ledger_id", Type: "string", Reference: &ledgerId, Facet: &facet},
			{Name: "identity_id", Type: "string", Facet: &facet, Reference: &identityId, Optional: &enableNested},
			{Name: "balance_id", Type: "string", Facet: &facet},
			{Name: "indicator", Type: "string", Facet: &facet},
			{Name: "currency", Type: "string", Facet: &facet},
			{Name: "created_at", Type: "int64", Facet: &facet},
			{Name: "inflight_expires_at", Type: "int64", Facet: &facet},
			{Name: "meta_data", Type: "object", Facet: &facet, Optional: &enableNested},
		},
		DefaultSortingField: &sortBy,
		EnableNestedFields:  &enableNested,
	}
}

// getTransactionSchema returns the schema for the "transactions" collection.
func getTransactionSchema() *api.CollectionSchema {
	facet := true
	sortBy := "created_at"
	enableNested := true
	sourceId := "balances.balance_id"
	destinationId := "balances.balance_id"
	sourcesId := "balances.balance_id"
	destinationsId := "balances.balance_id"
	return &api.CollectionSchema{
		Name: "transactions",
		Fields: []api.Field{
			{Name: "precise_amount", Type: "string", Facet: &facet},
			{Name: "amount", Type: "float", Facet: &facet},
			{Name: "rate", Type: "float", Facet: &facet},
			{Name: "precision", Type: "float", Facet: &facet},
			{Name: "transaction_id", Type: "string", Facet: &facet},
			{Name: "parent_transaction", Type: "string", Facet: &facet},
			{Name: "source", Type: "string", Reference: &sourceId, Facet: &facet},
			{Name: "destination", Type: "string", Reference: &destinationId, Facet: &facet},
			{Name: "reference", Type: "string", Facet: &facet},
			{Name: "currency", Type: "string", Facet: &facet},
			{Name: "description", Type: "string", Facet: &facet},
			{Name: "status", Type: "string", Facet: &facet},
			{Name: "hash", Type: "string", Facet: &facet},
			{Name: "allow_overdraft", Type: "bool", Facet: &facet},
			{Name: "inflight", Type: "bool", Facet: &facet},
			{Name: "sources", Type: "string[]", Reference: &sourcesId, Facet: &facet},
			{Name: "destinations", Type: "string[]", Reference: &destinationsId, Facet: &facet},
			{Name: "created_at", Type: "int64", Facet: &facet},
			{Name: "scheduled_for", Type: "int64", Facet: &facet},
			{Name: "inflight_expiry_date", Type: "int64", Facet: &facet},
			{Name: "meta_data", Type: "object", Facet: &facet, Optional: &enableNested},
		},
		DefaultSortingField: &sortBy,
		EnableNestedFields:  &enableNested,
	}
}

// getReconciliationSchema returns the schema for the "reconciliations" collection.
func getReconciliationSchema() *api.CollectionSchema {
	facet := true
	sortBy := "started_at"
	return &api.CollectionSchema{
		Name: "reconciliations",
		Fields: []api.Field{
			{Name: "reconciliation_id", Type: "string", Facet: &facet},
			{Name: "upload_id", Type: "string", Facet: &facet},
			{Name: "status", Type: "string", Facet: &facet},
			{Name: "matched_transactions", Type: "int32", Facet: &facet},
			{Name: "unmatched_transactions", Type: "int32", Facet: &facet},
			{Name: "started_at", Type: "int64", Facet: &facet},
			{Name: "completed_at", Type: "int64", Facet: &facet},
		},
		DefaultSortingField: &sortBy,
	}
}

// getIdentitySchema returns the schema for the "identities" collection.
func getIdentitySchema() *api.CollectionSchema {
	facet := true
	sortBy := "created_at"
	enableNested := true
	return &api.CollectionSchema{
		Name: "identities",
		Fields: []api.Field{
			{Name: "identity_id", Type: "string", Facet: &facet},
			{Name: "identity_type", Type: "string", Facet: &facet},
			{Name: "organization_name", Type: "string", Facet: &facet},
			{Name: "category", Type: "string", Facet: &facet},
			{Name: "first_name", Type: "string", Facet: &facet},
			{Name: "last_name", Type: "string", Facet: &facet},
			{Name: "other_names", Type: "string", Facet: &facet},
			{Name: "gender", Type: "string", Facet: &facet},
			{Name: "email_address", Type: "string", Facet: &facet},
			{Name: "phone_number", Type: "string", Facet: &facet},
			{Name: "nationality", Type: "string", Facet: &facet},
			{Name: "street", Type: "string", Facet: &facet},
			{Name: "country", Type: "string", Facet: &facet},
			{Name: "state", Type: "string", Facet: &facet},
			{Name: "post_code", Type: "string", Facet: &facet},
			{Name: "city", Type: "string", Facet: &facet},
			{Name: "dob", Type: "int64", Facet: &facet},
			{Name: "created_at", Type: "int64", Facet: &facet},
			{Name: "meta_data", Type: "object", Facet: &facet, Optional: &enableNested},
		},
		DefaultSortingField: &sortBy,
		EnableNestedFields:  &enableNested,
	}
}
