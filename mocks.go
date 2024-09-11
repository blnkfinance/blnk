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

	"github.com/jerry-enebeli/blnk/model"
)

// MockBlnk is a mock implementation of the Blnk struct for testing purposes.
type MockBlnk struct {
	Blnk
	mockGetTransaction func(string) (*model.Transaction, error)
}

// GetTransaction retrieves a transaction by its ID using the mock implementation if provided.
// If no mock implementation is provided, it falls back to the actual GetTransaction method.
//
// Parameters:
// - id string: The ID of the transaction to retrieve.
//
// Returns:
// - *model.Transaction: A pointer to the Transaction model if found.
// - error: An error if the transaction could not be retrieved.
func (m *MockBlnk) GetTransaction(id string) (*model.Transaction, error) {
	if m.mockGetTransaction != nil {
		return m.mockGetTransaction(id)
	}
	return m.Blnk.GetTransaction(context.Background(), id)
}
