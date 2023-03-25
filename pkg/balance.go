package pkg

import (
	"github.com/jerry-enebeli/blnk"
)

func (l Blnk) CreateBalance(balance blnk.Balance) (blnk.Balance, error) {
	return l.datasource.CreateBalance(balance)
}

func (l Blnk) GetBalance(id int64) (*blnk.Balance, error) {
	return l.datasource.GetBalanceByID(id)
}

func (l Blnk) GetAllBalances() ([]blnk.Balance, error) {
	return l.datasource.GetAllBalances()
}
