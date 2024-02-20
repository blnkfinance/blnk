package blnk

import (
	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/database"
	"github.com/jerry-enebeli/blnk/model"
)

type Blnk struct {
	queue      *Queue
	datasource database.IDataSource
	config     *config.Configuration
	bt         *model.BalanceTracker
}

func NewBlnk(db database.IDataSource) (*Blnk, error) {
	configuration, err := config.Fetch()
	if err != nil {
		return nil, err
	}

	bt := NewBalanceTracker()
	newQueue := NewQueue(configuration)
	newBlnk := &Blnk{datasource: db, config: configuration, bt: bt, queue: newQueue}
	return newBlnk, nil
}
