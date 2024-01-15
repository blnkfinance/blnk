package blnk

import "github.com/jerry-enebeli/blnk/model"

func (l Blnk) CreateLedger(ledger model.Ledger) (model.Ledger, error) {
	return l.datasource.CreateLedger(ledger)
}

func (l Blnk) GetAllLedgers() ([]model.Ledger, error) {
	return l.datasource.GetAllLedgers()
}

func (l Blnk) GetLedgerByID(id string) (*model.Ledger, error) {
	return l.datasource.GetLedgerByID(id)
}
