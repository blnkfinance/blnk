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
	"testing"

	"github.com/stretchr/testify/assert"
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
