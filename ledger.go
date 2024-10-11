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

	"github.com/jerry-enebeli/blnk/internal/notification"
	"github.com/jerry-enebeli/blnk/model"
)

// postLedgerActions performs some actions after a ledger has been created.
// It sends the newly created ledger to the search index queue, which indexes the ledger in Typesense.
// It also sends a webhook notification.
//
// Parameters:
// - _ context.Context: The context for the operation (not used in this function).
// - ledger *model.Ledger: A pointer to the newly created Ledger model.
func (l *Blnk) postLedgerActions(_ context.Context, ledger *model.Ledger) {
	go func() {
		err := l.queue.queueIndexData(ledger.LedgerID, "ledgers", ledger)
		if err != nil {
			notification.NotifyError(err)
		}
		err = SendWebhook(NewWebhook{
			Event:   "ledger.created",
			Payload: ledger,
		})
		if err != nil {
			notification.NotifyError(err)
		}
	}()
}

// CreateLedger creates a new ledger.
// It calls postLedgerActions after a successful creation.
//
// Parameters:
// - ledger: A Ledger model representing the ledger to be created.
//
// Returns:
// - model.Ledger: The created Ledger model.
// - error: An error if the ledger could not be created.
func (l *Blnk) CreateLedger(ledger model.Ledger) (model.Ledger, error) {
	ledger, err := l.datasource.CreateLedger(ledger)
	if err != nil {
		return model.Ledger{}, err
	}
	l.postLedgerActions(context.Background(), &ledger)
	return ledger, nil
}

// GetAllLedgers retrieves all ledgers from the datasource.
// It returns a slice of Ledger models and an error if the operation fails.
//
// Returns:
// - []model.Ledger: A slice of Ledger models.
// - error: An error if the ledgers could not be retrieved.
func (l *Blnk) GetAllLedgers(limit, offset int) ([]model.Ledger, error) {
	return l.datasource.GetAllLedgers(limit, offset)
}

// GetLedgerByID retrieves a ledger by its ID from the datasource.
// It returns a pointer to the Ledger model and an error if the operation fails.
//
// Parameters:
// - id: A string representing the ID of the ledger to retrieve.
//
// Returns:
// - *model.Ledger: A pointer to the Ledger model if found.
// - error: An error if the ledger could not be retrieved.
func (l *Blnk) GetLedgerByID(id string) (*model.Ledger, error) {
	return l.datasource.GetLedgerByID(id)
}
