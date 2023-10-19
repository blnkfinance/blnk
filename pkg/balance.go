package pkg

import (
	"github.com/jerry-enebeli/blnk"
)

func (l Blnk) CreateBalance(balance blnk.Balance) (blnk.Balance, error) {
	return l.datasource.CreateBalance(balance)
}

func (l Blnk) GetBalanceByID(id string, include []string) (*blnk.Balance, error) {
	return l.datasource.GetBalanceByID(id, include)
}

func (l Blnk) GetAllBalances() ([]blnk.Balance, error) {
	return l.datasource.GetAllBalances()
}
