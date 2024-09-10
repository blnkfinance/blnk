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

func (l *Blnk) CreateIdentity(identity model.Identity) (model.Identity, error) {
	return l.datasource.CreateIdentity(identity)
}

func (l *Blnk) GetIdentity(id string) (*model.Identity, error) {
	return l.datasource.GetIdentityByID(id)
}

func (l *Blnk) GetAllIdentities() ([]model.Identity, error) {
	return l.datasource.GetAllIdentities()
}

func (l *Blnk) UpdateIdentity(identity *model.Identity) error {
	return l.datasource.UpdateIdentity(identity)
}

func (l *Blnk) DeleteIdentity(id string) error {
	return l.datasource.DeleteIdentity(id)
}
