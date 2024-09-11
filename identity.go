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

import "github.com/jerry-enebeli/blnk/model"

// CreateIdentity creates a new identity in the database.
//
// Parameters:
// - identity model.Identity: The Identity model to be created.
//
// Returns:
// - model.Identity: The created Identity model.
// - error: An error if the identity could not be created.
func (l *Blnk) CreateIdentity(identity model.Identity) (model.Identity, error) {
	return l.datasource.CreateIdentity(identity)
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
