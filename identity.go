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
	"reflect"
	"strings"

	"github.com/jerry-enebeli/blnk/internal/notification"
	"github.com/jerry-enebeli/blnk/internal/tokenization"
	"github.com/jerry-enebeli/blnk/model"
)

// postIdentityActions performs actions after an identity has been created.
// It sends the newly created identity to the search index queue and sends a webhook notification.
func (l *Blnk) postIdentityActions(_ context.Context, identity *model.Identity) {
	go func() {
		err := l.queue.queueIndexData(identity.IdentityID, "identities", identity)
		if err != nil {
			notification.NotifyError(err)
		}
		err = SendWebhook(NewWebhook{
			Event:   "identity.created",
			Payload: identity,
		})
		if err != nil {
			notification.NotifyError(err)
		}
	}()
}

// CreateIdentity creates a new identity in the database.
//
// Parameters:
// - identity model.Identity: The Identity model to be created.
//
// Returns:
// - model.Identity: The created Identity model.
// - error: An error if the identity could not be created.
func (l *Blnk) CreateIdentity(identity model.Identity) (model.Identity, error) {
	identity, err := l.datasource.CreateIdentity(identity)
	if err != nil {
		return model.Identity{}, err
	}
	l.postIdentityActions(context.Background(), &identity)
	return identity, nil
}

// GetIdentity retrieves an identity by its ID.
//
// Parameters:
// - id string: The ID of the identity to retrieve.
//
// Returns:
// - *model.Identity: A pointer to the Identity model if found.
// - error: An error if the identity could not be retrieved.
func (l *Blnk) GetIdentity(id string) (*model.Identity, error) {
	return l.datasource.GetIdentityByID(id)
}

// GetAllIdentities retrieves all identities from the database.
//
// Returns:
// - []model.Identity: A slice of Identity models.
// - error: An error if the identities could not be retrieved.
func (l *Blnk) GetAllIdentities() ([]model.Identity, error) {
	return l.datasource.GetAllIdentities()
}

// UpdateIdentity updates an existing identity in the database.
//
// Parameters:
// - identity *model.Identity: A pointer to the Identity model to be updated.
//
// Returns:
// - error: An error if the identity could not be updated.
func (l *Blnk) UpdateIdentity(identity *model.Identity) error {
	return l.datasource.UpdateIdentity(identity)
}

// DeleteIdentity deletes an identity by its ID.
//
// Parameters:
// - id string: The ID of the identity to delete.
//
// Returns:
// - error: An error if the identity could not be deleted.
func (l *Blnk) DeleteIdentity(id string) error {
	return l.datasource.DeleteIdentity(id)
}

// TokenizeIdentityField tokenizes a specific field in an identity.
//
// Parameters:
// - identityID string: The ID of the identity.
// - fieldName string: The name of the field to tokenize.
//
// Returns:
// - error: An error if the field could not be tokenized.
func (l *Blnk) TokenizeIdentityField(identityID, fieldName string) error {
	// Convert field name to struct field format for reflection
	structFieldName := convertToStructFieldName(fieldName)

	// Check if field is tokenizable
	validField := false
	for _, field := range tokenization.TokenizableFields {
		if field == structFieldName {
			validField = true
			break
		}
	}

	if !validField {
		return fmt.Errorf("field %s is not tokenizable", fieldName)
	}

	// Get the identity
	identity, err := l.GetIdentity(identityID)
	if err != nil {
		return err
	}

	// Check if field is already tokenized using the original field name
	// as IsFieldTokenized will handle the conversion internally
	if identity.IsFieldTokenized(fieldName) {
		return fmt.Errorf("field %s is already tokenized", fieldName)
	}

	// Get the field value using reflection with struct field name
	val := reflect.ValueOf(identity).Elem()
	fieldVal := val.FieldByName(structFieldName)

	if !fieldVal.IsValid() || !fieldVal.CanSet() {
		return fmt.Errorf("field %s not found or cannot be set", fieldName)
	}

	// Get the string value
	strVal := fieldVal.String()

	// Tokenize the value
	token, err := l.tokenizer.TokenizeWithMode(strVal, tokenization.FormatPreservingMode)
	if err != nil {
		return err
	}

	// Set the tokenized value
	fieldVal.SetString(token)

	// Mark the field as tokenized using the original field name
	// as MarkFieldAsTokenized will handle the conversion internally
	identity.MarkFieldAsTokenized(fieldName)

	// Update the identity
	return l.UpdateIdentity(identity)
}

// DetokenizeIdentityField detokenizes a specific field in an identity.
//
// Parameters:
// - identityID string: The ID of the identity.
// - fieldName string: The name of the field to detokenize.
//
// Returns:
// - string: The detokenized field value.
// - error: An error if the field could not be detokenized.
func (l *Blnk) DetokenizeIdentityField(identityID, fieldName string) (string, error) {
	// Get the identity
	identity, err := l.GetIdentity(identityID)
	if err != nil {
		return "", err
	}

	// Try both original and capitalized field name
	structFieldName := convertToStructFieldName(fieldName)

	// Check if field is tokenized
	if !identity.IsFieldTokenized(fieldName) && !identity.IsFieldTokenized(structFieldName) {
		// Debug info
		if identity.MetaData != nil {
			metaStr, _ := json.Marshal(identity.MetaData)
			return "", fmt.Errorf("field %s is not tokenized. Metadata: %s", fieldName, metaStr)
		}
		return "", fmt.Errorf("field %s is not tokenized", fieldName)
	}

	// Get the field value using reflection - try both field name versions
	val := reflect.ValueOf(identity).Elem()
	fieldVal := val.FieldByName(structFieldName)

	if !fieldVal.IsValid() {
		fieldVal = val.FieldByName(fieldName)
		if !fieldVal.IsValid() {
			return "", fmt.Errorf("field %s not found", fieldName)
		}
	}

	// Get the tokenized value
	tokenVal := fieldVal.String()

	// Detokenize the value
	originalValue, err := l.tokenizer.Detokenize(tokenVal)
	if err != nil {
		return "", err
	}

	return originalValue, nil
}

// TokenizeIdentity tokenizes all specified fields in an identity.
//
// Parameters:
// - identityID string: The ID of the identity.
// - fields []string: The names of the fields to tokenize.
//
// Returns:
// - error: An error if any field could not be tokenized.
func (l *Blnk) TokenizeIdentity(identityID string, fields []string) error {
	for _, field := range fields {
		err := l.TokenizeIdentityField(identityID, field)
		if err != nil {
			return err
		}
	}
	return nil
}

// DetokenizeIdentity detokenizes and returns all tokenized fields in an identity.
//
// Parameters:
// - identityID string: The ID of the identity.
//
// Returns:
// - map[string]string: A map of field names to their detokenized values.
// - error: An error if any field could not be detokenized.
func (l *Blnk) DetokenizeIdentity(identityID string) (map[string]string, error) {
	// Get the identity
	identity, err := l.GetIdentity(identityID)
	if err != nil {
		return nil, err
	}

	result := make(map[string]string)

	// Check each tokenized field in metadata
	if identity.MetaData != nil {
		tokenizedFields, ok := identity.MetaData["tokenized_fields"].(map[string]bool)
		if ok {
			for fieldName, isTokenized := range tokenizedFields {
				if isTokenized {
					originalValue, err := l.DetokenizeIdentityField(identityID, fieldName)
					if err != nil {
						return nil, err
					}
					result[fieldName] = originalValue
				}
			}
		}
	}

	return result, nil
}

// TokenizeAllPII tokenizes all eligible PII fields in an identity.
//
// Parameters:
// - identityID string: The ID of the identity.
//
// Returns:
// - error: An error if any field could not be tokenized.
func (l *Blnk) TokenizeAllPII(identityID string) error {
	for _, field := range tokenization.TokenizableFields {
		// Ignore errors for fields that might already be tokenized
		_ = l.TokenizeIdentityField(identityID, field)
	}
	return nil
}

// GetDetokenizedIdentity returns a copy of the identity with all fields detokenized.
// Note: This does not modify the stored identity.
//
// Parameters:
// - identityID string: The ID of the identity.
//
// Returns:
// - *model.Identity: A pointer to the detokenized Identity model.
// - error: An error if the identity could not be detokenized.
func (l *Blnk) GetDetokenizedIdentity(identityID string) (*model.Identity, error) {
	// Get the identity
	identity, err := l.GetIdentity(identityID)
	if err != nil {
		return nil, err
	}

	// Create a copy
	detokenizedIdentity := *identity

	// Detokenize all tokenized fields
	if identity.MetaData != nil {
		tokenizedFields, ok := identity.MetaData["tokenized_fields"].(map[string]bool)
		if ok {
			for field, isTokenized := range tokenizedFields {
				if isTokenized {
					originalValue, err := l.DetokenizeIdentityField(identityID, field)
					if err != nil {
						return nil, err
					}

					// Set the original value in the copy
					val := reflect.ValueOf(&detokenizedIdentity).Elem()
					fieldVal := val.FieldByName(field)
					if fieldVal.IsValid() && fieldVal.CanSet() {
						fieldVal.SetString(originalValue)
					}
				}
			}
		}
	}

	// Create a clean copy of metadata without tokenized_fields
	if detokenizedIdentity.MetaData != nil {
		newMetaData := make(map[string]interface{})
		for k, v := range detokenizedIdentity.MetaData {
			if k != "tokenized_fields" {
				newMetaData[k] = v
			}
		}
		detokenizedIdentity.MetaData = newMetaData
	}

	return &detokenizedIdentity, nil
}

// convertToStructFieldName ensures consistent field name format by returning
// the Go struct field name (typically capitalized) for the given input
func convertToStructFieldName(fieldName string) string {
	// For simple cases, just capitalize the first letter
	if len(fieldName) > 0 {
		return strings.ToUpper(fieldName[0:1]) + fieldName[1:]
	}
	return fieldName
}
