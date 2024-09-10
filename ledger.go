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

func (l *Blnk) CreateLedger(ledger model.Ledger) (model.Ledger, error) {
	ledger, err := l.datasource.CreateLedger(ledger)
	if err != nil {
		return model.Ledger{}, err
	}
	l.postLedgerActions(context.Background(), &ledger)
	return ledger, nil
}

func (l *Blnk) GetAllLedgers() ([]model.Ledger, error) {
	return l.datasource.GetAllLedgers()
}

func (l *Blnk) GetLedgerByID(id string) (*model.Ledger, error) {
	return l.datasource.GetLedgerByID(id)
}
