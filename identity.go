package blnk

import "github.com/jerry-enebeli/blnk/model"

func (l Blnk) CreateIdentity(identity model.Identity) (model.Identity, error) {
	return l.datasource.CreateIdentity(identity)
}

func (l Blnk) GetIdentity(id string) (*model.Identity, error) {
	return l.datasource.GetIdentityByID(id)
}

func (l Blnk) GetAllIdentities() ([]model.Identity, error) {
	return l.datasource.GetAllIdentities()
}

func (l Blnk) UpdateIdentity(identity *model.Identity) error {
	return l.datasource.UpdateIdentity(identity)
}

func (l Blnk) DeleteIdentity(id string) error {
	return l.datasource.DeleteIdentity(id)
}
