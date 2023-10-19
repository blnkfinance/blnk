package pkg

import (
	"github.com/jerry-enebeli/blnk"
)

// CreateIdentity creates a new identity in the database.
func (l Blnk) CreateIdentity(identity blnk.Identity) (blnk.Identity, error) {
	return l.datasource.CreateIdentity(identity)
}

// GetIdentity retrieves an identity from the database by ID.
func (l Blnk) GetIdentity(id string) (*blnk.Identity, error) {
	return l.datasource.GetIdentityByID(id)
}

// GetAllIdentities retrieves all identities from the database.
func (l Blnk) GetAllIdentities() ([]blnk.Identity, error) {
	return l.datasource.GetAllIdentities()
}

// UpdateIdentity updates an identity in the database.
func (l Blnk) UpdateIdentity(identity *blnk.Identity) error {
	return l.datasource.UpdateIdentity(identity)
}

// DeleteIdentity deletes an identity from the database by ID.
func (l Blnk) DeleteIdentity(id string) error {
	return l.datasource.DeleteIdentity(id)
}
