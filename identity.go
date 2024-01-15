package blnk

import "github.com/jerry-enebeli/blnk/model"

// CreateIdentity creates a new identity in the database.
func (l Blnk) CreateIdentity(identity model.Identity) (model.Identity, error) {
	return l.datasource.CreateIdentity(identity)
}

// GetIdentity retrieves an identity from the database by ID.
func (l Blnk) GetIdentity(id string) (*model.Identity, error) {
	return l.datasource.GetIdentityByID(id)
}

// GetAllIdentities retrieves all identities from the database.
func (l Blnk) GetAllIdentities() ([]model.Identity, error) {
	return l.datasource.GetAllIdentities()
}

// UpdateIdentity updates an identity in the database.
func (l Blnk) UpdateIdentity(identity *model.Identity) error {
	return l.datasource.UpdateIdentity(identity)
}

// DeleteIdentity deletes an identity from the database by ID.
func (l Blnk) DeleteIdentity(id string) error {
	return l.datasource.DeleteIdentity(id)
}
