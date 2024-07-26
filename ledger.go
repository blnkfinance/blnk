package blnk

import (
	"context"

	"github.com/jerry-enebeli/blnk/internal/notification"
	"github.com/jerry-enebeli/blnk/model"
)

func (l *Blnk) postLedgerActions(_ context.Context, ledger *model.Ledger) {
	go func() {
		l.queue.queueIndexData(ledger.LedgerID, "ledgers", ledger)
		err := SendWebhook(NewWebhook{
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
