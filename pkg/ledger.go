package pkg

import (
	"github.com/jerry-enebeli/blnk"
)

func (l Blnk) CreateLedger(ledger blnk.Ledger) (blnk.Ledger, error) {
	return l.datasource.CreateLedger(ledger)
}

func (l Blnk) GetAllLedgers() ([]blnk.Ledger, error) {
	return l.datasource.GetAllLedgers()
}

func (l Blnk) GetLedgerByID(id string) (*blnk.Ledger, error) {
	return l.datasource.GetLedgerByID(id)
}
