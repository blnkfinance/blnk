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
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/typesense/typesense-go/typesense/api"
)

// TestTransactionSchemaHasEffectiveDate verifies that the transaction schema
// includes the effective_date field for sorting support
func TestTransactionSchemaHasEffectiveDate(t *testing.T) {
	schema := getTransactionSchema()

	// Find effective_date field in schema
	var foundEffectiveDate bool
	var effectiveDateType string

	for _, field := range schema.Fields {
		if field.Name == "effective_date" {
			foundEffectiveDate = true
			effectiveDateType = field.Type
			break
		}
	}

	assert.True(t, foundEffectiveDate, "Transaction schema should include effective_date field")
	assert.Equal(t, "int64", effectiveDateType, "effective_date should be int64 type for Unix timestamp")
}

// TestTransactionCollectionConfigHasEffectiveDateInTimeFields verifies that
// effective_date is included in the TimeFields for proper timestamp normalization
func TestTransactionCollectionConfigHasEffectiveDateInTimeFields(t *testing.T) {
	config, ok := collectionConfigs[CollectionTransactions]
	assert.True(t, ok, "Transaction collection config should exist")

	// Check that effective_date is in TimeFields
	var foundInTimeFields bool
	for _, field := range config.TimeFields {
		if field == "effective_date" {
			foundInTimeFields = true
			break
		}
	}

	assert.True(t, foundInTimeFields,
		"effective_date should be in TimeFields for timestamp normalization. Current TimeFields: %v",
		config.TimeFields)
}

// TestTransactionSchemaDefaultSortField verifies that created_at remains
// the default sort field (no breaking change)
func TestTransactionSchemaDefaultSortField(t *testing.T) {
	schema := getTransactionSchema()

	assert.NotNil(t, schema.DefaultSortingField, "Default sorting field should be set")
	assert.Equal(t, "created_at", *schema.DefaultSortingField,
		"Default sorting field should remain created_at to avoid breaking changes")
}

// TestTransactionSchemaTimeFieldsComplete verifies all time-related fields
// are properly configured
func TestTransactionSchemaTimeFieldsComplete(t *testing.T) {
	config := collectionConfigs[CollectionTransactions]

	expectedTimeFields := []string{
		"created_at",
		"scheduled_for",
		"inflight_expiry_date",
		"effective_date",
	}

	for _, expected := range expectedTimeFields {
		var found bool
		for _, actual := range config.TimeFields {
			if actual == expected {
				found = true
				break
			}
		}
		assert.True(t, found, "TimeFields should include %s", expected)
	}
}

func TestNewIndexBatch(t *testing.T) {
	batch := NewIndexBatch("test-batch-id")

	assert.NotNil(t, batch)
	assert.Equal(t, "test-batch-id", batch.ID)
	assert.NotNil(t, batch.Dependencies)
	assert.Len(t, batch.Dependencies, 0)
	assert.Nil(t, batch.Primary)
	assert.False(t, batch.CreatedAt.IsZero())
}

func TestIndexBatch_AddDependency(t *testing.T) {
	batch := NewIndexBatch("test-batch")

	data := map[string]interface{}{"key": "value"}
	batch.AddDependency("balances", "bln_123", data)

	assert.Len(t, batch.Dependencies, 1)
	assert.Equal(t, "balances", batch.Dependencies[0].Collection)
	assert.Equal(t, "bln_123", batch.Dependencies[0].DocumentID)
	assert.Equal(t, data, batch.Dependencies[0].Data)
}

func TestIndexBatch_AddMultipleDependencies(t *testing.T) {
	batch := NewIndexBatch("test-batch")

	batch.AddDependency("balances", "bln_source", map[string]interface{}{"id": "source"})
	batch.AddDependency("balances", "bln_dest", map[string]interface{}{"id": "dest"})
	batch.AddDependency("ledgers", "led_123", map[string]interface{}{"id": "ledger"})

	assert.Len(t, batch.Dependencies, 3)
}

func TestIndexBatch_SetPrimary(t *testing.T) {
	batch := NewIndexBatch("test-batch")

	data := map[string]interface{}{"transaction_id": "txn_123"}
	batch.SetPrimary("transactions", "txn_123", data)

	assert.NotNil(t, batch.Primary)
	assert.Equal(t, "transactions", batch.Primary.Collection)
	assert.Equal(t, "txn_123", batch.Primary.DocumentID)
	assert.Equal(t, data, batch.Primary.Data)
}

func TestIndexBatch_Deduplicate(t *testing.T) {
	batch := NewIndexBatch("test-batch")

	// Add duplicate dependencies
	batch.AddDependency("balances", "bln_123", map[string]interface{}{"id": "1"})
	batch.AddDependency("balances", "bln_123", map[string]interface{}{"id": "2"}) // Duplicate
	batch.AddDependency("balances", "bln_456", map[string]interface{}{"id": "3"})
	batch.AddDependency("balances", "bln_123", map[string]interface{}{"id": "4"}) // Duplicate

	assert.Len(t, batch.Dependencies, 4)

	batch.Deduplicate()

	assert.Len(t, batch.Dependencies, 2)
	// First occurrence should be kept
	assert.Equal(t, "bln_123", batch.Dependencies[0].DocumentID)
	assert.Equal(t, "bln_456", batch.Dependencies[1].DocumentID)
}

func TestIndexBatch_DeduplicateDifferentCollections(t *testing.T) {
	batch := NewIndexBatch("test-batch")

	// Same document ID but different collections
	batch.AddDependency("balances", "id_123", map[string]interface{}{"type": "balance"})
	batch.AddDependency("ledgers", "id_123", map[string]interface{}{"type": "ledger"})

	batch.Deduplicate()

	assert.Len(t, batch.Dependencies, 2) // Should keep both since they're different collections
}

func TestToMap_AlreadyMap(t *testing.T) {
	input := map[string]interface{}{"key": "value"}

	result, err := toMap(input)

	assert.NoError(t, err)
	assert.Equal(t, input, result)
}

func TestToMap_Struct(t *testing.T) {
	input := struct {
		Name string `json:"name"`
		Age  int    `json:"age"`
	}{
		Name: "test",
		Age:  25,
	}

	result, err := toMap(input)

	assert.NoError(t, err)
	assert.Equal(t, "test", result["name"])
	assert.Equal(t, float64(25), result["age"]) // JSON unmarshals numbers as float64
}

func TestGetDefaultValue(t *testing.T) {
	tests := []struct {
		fieldType string
		expected  interface{}
	}{
		{"string", ""},
		{"int32", int64(0)},
		{"int64", int64(0)},
		{"float", float64(0)},
		{"bool", false},
		{"string[]", []string{}},
		{"unknown", nil},
	}

	for _, tt := range tests {
		t.Run(tt.fieldType, func(t *testing.T) {
			result := getDefaultValue(tt.fieldType)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestCompareSchemas_NewFields(t *testing.T) {
	oldSchema := &api.CollectionSchema{
		Name: "test",
		Fields: []api.Field{
			{Name: "field1", Type: "string"},
		},
	}

	newSchema := &api.CollectionSchema{
		Name: "test",
		Fields: []api.Field{
			{Name: "field1", Type: "string"},
			{Name: "field2", Type: "int64"},
		},
	}

	newFields, removedFields := compareSchemas(oldSchema, newSchema)

	assert.Len(t, newFields, 1)
	assert.Equal(t, "field2", newFields[0].Name)
	assert.Len(t, removedFields, 0)
}

func TestCompareSchemas_RemovedFields(t *testing.T) {
	oldSchema := &api.CollectionSchema{
		Name: "test",
		Fields: []api.Field{
			{Name: "field1", Type: "string"},
			{Name: "field2", Type: "int64"},
		},
	}

	newSchema := &api.CollectionSchema{
		Name: "test",
		Fields: []api.Field{
			{Name: "field1", Type: "string"},
		},
	}

	newFields, removedFields := compareSchemas(oldSchema, newSchema)

	assert.Len(t, newFields, 0)
	assert.Len(t, removedFields, 1)
	assert.Equal(t, "field2", removedFields[0])
}

func TestCompareSchemas_BothNewAndRemoved(t *testing.T) {
	oldSchema := &api.CollectionSchema{
		Name: "test",
		Fields: []api.Field{
			{Name: "field1", Type: "string"},
			{Name: "old_field", Type: "string"},
		},
	}

	newSchema := &api.CollectionSchema{
		Name: "test",
		Fields: []api.Field{
			{Name: "field1", Type: "string"},
			{Name: "new_field", Type: "int64"},
		},
	}

	newFields, removedFields := compareSchemas(oldSchema, newSchema)

	assert.Len(t, newFields, 1)
	assert.Equal(t, "new_field", newFields[0].Name)
	assert.Len(t, removedFields, 1)
	assert.Equal(t, "old_field", removedFields[0])
}

func TestCompareSchemas_NoChanges(t *testing.T) {
	schema := &api.CollectionSchema{
		Name: "test",
		Fields: []api.Field{
			{Name: "field1", Type: "string"},
			{Name: "field2", Type: "int64"},
		},
	}

	newFields, removedFields := compareSchemas(schema, schema)

	assert.Len(t, newFields, 0)
	assert.Len(t, removedFields, 0)
}

func TestTypesenseClient_ConvertNumberField_BigInt(t *testing.T) {
	client := &TypesenseClient{}

	data := map[string]interface{}{
		"amount": big.NewInt(1234567890),
	}

	client.convertNumberField(data, "amount")

	assert.Equal(t, "1234567890", data["amount"])
}

func TestTypesenseClient_ConvertNumberField_Float64(t *testing.T) {
	client := &TypesenseClient{}

	data := map[string]interface{}{
		"amount": float64(1.234e10),
	}

	client.convertNumberField(data, "amount")

	assert.Equal(t, "12340000000", data["amount"])
}

func TestTypesenseClient_ConvertNumberField_MissingField(t *testing.T) {
	client := &TypesenseClient{}

	data := map[string]interface{}{
		"other": "value",
	}

	client.convertNumberField(data, "amount")

	// Should not add the field
	_, exists := data["amount"]
	assert.False(t, exists)
}

func TestTypesenseClient_ProcessMetadata_Nil(t *testing.T) {
	client := &TypesenseClient{}

	data := map[string]interface{}{
		"meta_data": nil,
	}

	err := client.processMetadata(data)

	assert.NoError(t, err)
	assert.NotNil(t, data["meta_data"])
	assert.IsType(t, map[string]interface{}{}, data["meta_data"])
}

func TestTypesenseClient_ProcessMetadata_Map(t *testing.T) {
	client := &TypesenseClient{}

	metaData := map[string]interface{}{"key": "value"}
	data := map[string]interface{}{
		"meta_data": metaData,
	}

	err := client.processMetadata(data)

	assert.NoError(t, err)
	assert.Equal(t, metaData, data["meta_data"])
}

func TestTypesenseClient_ProcessMetadata_NoMetaData(t *testing.T) {
	client := &TypesenseClient{}

	data := map[string]interface{}{
		"other_field": "value",
	}

	err := client.processMetadata(data)

	assert.NoError(t, err)
	_, exists := data["meta_data"]
	assert.False(t, exists)
}

func TestTypesenseClient_NormalizeTimeFields_TimeType(t *testing.T) {
	client := &TypesenseClient{}
	config := CollectionConfig{
		TimeFields: []string{"created_at"},
	}

	testTime := time.Date(2024, 1, 15, 10, 30, 0, 0, time.UTC)
	data := map[string]interface{}{
		"created_at": testTime,
	}

	client.normalizeTimeFields(config, data)

	assert.Equal(t, testTime.Unix(), data["created_at"])
}

func TestTypesenseClient_NormalizeTimeFields_Int64(t *testing.T) {
	client := &TypesenseClient{}
	config := CollectionConfig{
		TimeFields: []string{"created_at"},
	}

	unixTime := int64(1705318200)
	data := map[string]interface{}{
		"created_at": unixTime,
	}

	client.normalizeTimeFields(config, data)

	assert.Equal(t, unixTime, data["created_at"])
}

func TestTypesenseClient_NormalizeTimeFields_RFC3339String(t *testing.T) {
	client := &TypesenseClient{}
	config := CollectionConfig{
		TimeFields: []string{"created_at"},
	}

	data := map[string]interface{}{
		"created_at": "2024-01-15T10:30:00Z",
	}

	client.normalizeTimeFields(config, data)

	expectedTime, _ := time.Parse(time.RFC3339, "2024-01-15T10:30:00Z")
	assert.Equal(t, expectedTime.Unix(), data["created_at"])
}

func TestTypesenseClient_NormalizeTimeFields_EmptyString(t *testing.T) {
	client := &TypesenseClient{}
	config := CollectionConfig{
		TimeFields: []string{"created_at"},
	}

	data := map[string]interface{}{
		"created_at": "",
	}

	client.normalizeTimeFields(config, data)

	_, exists := data["created_at"]
	assert.False(t, exists, "Empty string should be removed")
}

func TestTypesenseClient_NormalizeTimeFields_Float64(t *testing.T) {
	client := &TypesenseClient{}
	config := CollectionConfig{
		TimeFields: []string{"created_at"},
	}

	data := map[string]interface{}{
		"created_at": float64(1705318200),
	}

	client.normalizeTimeFields(config, data)

	assert.Equal(t, int64(1705318200), data["created_at"])
}

func TestTypesenseClient_GetIDField(t *testing.T) {
	client := &TypesenseClient{}

	tests := []struct {
		table    string
		expected string
	}{
		{CollectionLedgers, "ledger_id"},
		{CollectionBalances, "balance_id"},
		{CollectionTransactions, "transaction_id"},
		{CollectionReconciliations, "reconciliation_id"},
		{CollectionIdentities, "identity_id"},
		{"unknown", ""},
	}

	for _, tt := range tests {
		t.Run(tt.table, func(t *testing.T) {
			result := client.getIDField(tt.table)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func TestAllSchemas(t *testing.T) {
	schemas := []struct {
		name   string
		getter func() *api.CollectionSchema
	}{
		{"ledgers", getLedgerSchema},
		{"balances", getBalanceSchema},
		{"transactions", getTransactionSchema},
		{"reconciliations", getReconciliationSchema},
		{"identities", getIdentitySchema},
	}

	for _, schema := range schemas {
		t.Run(schema.name, func(t *testing.T) {
			s := schema.getter()
			assert.NotNil(t, s)
			assert.Equal(t, schema.name, s.Name)
			assert.NotEmpty(t, s.Fields)
		})
	}
}

func TestCollectionConfigsInitialized(t *testing.T) {
	expectedCollections := []string{
		CollectionLedgers,
		CollectionBalances,
		CollectionTransactions,
		CollectionReconciliations,
		CollectionIdentities,
	}

	for _, collection := range expectedCollections {
		t.Run(collection, func(t *testing.T) {
			config, ok := collectionConfigs[collection]
			assert.True(t, ok, "Collection config should exist for %s", collection)
			assert.NotNil(t, config.Schema)
			assert.NotEmpty(t, config.IDField)
		})
	}
}

func TestNotificationPayload_Struct(t *testing.T) {
	payload := NotificationPayload{
		Table: "balances",
		Data: map[string]interface{}{
			"balance_id": "bln_123",
		},
	}

	assert.Equal(t, "balances", payload.Table)
	assert.NotNil(t, payload.Data)
}

func TestIndexItem_Struct(t *testing.T) {
	item := IndexItem{
		Collection: "transactions",
		DocumentID: "txn_123",
		Data:       map[string]interface{}{"key": "value"},
	}

	assert.Equal(t, "transactions", item.Collection)
	assert.Equal(t, "txn_123", item.DocumentID)
	assert.NotNil(t, item.Data)
}
