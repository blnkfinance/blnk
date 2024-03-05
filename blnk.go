package blnk

import (
	"embed"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/database"
	"github.com/jerry-enebeli/blnk/model"
)

type Blnk struct {
	queue      *Queue
	datasource database.IDataSource
	bt         *model.BalanceTracker
}

const (
	GeneralLedgerID = "general_ledger_id"
)

//go:embed sql/*.sql
var SQLFiles embed.FS

func NewBlnk(db database.IDataSource) (*Blnk, error) {
	configuration, err := config.Fetch()
	if err != nil {
		return nil, err
	}

	bt := NewBalanceTracker()
	newQueue := NewQueue(configuration)
	newBlnk := &Blnk{datasource: db, bt: bt, queue: newQueue}
	return newBlnk, nil
}
