package database

import (
	"github.com/jerry-enebeli/blnk/model"
)

type IDataSource interface {
	transaction
	ledger
	balance
	identity
	balanceMonitor
	eventMapper
	account
}

type transaction interface {
	RecordTransaction(txn model.Transaction) (model.Transaction, error)
	GetTransaction(id string) (model.Transaction, error)
	GetTransactionByRef(reference string) (model.Transaction, error)
	UpdateTransactionStatus(id string, status string) error
	GroupTransactionsByCurrency() (map[string]struct {
		TotalAmount int64 `json:"total_amount"`
	}, error)
	GetAllTransactions() ([]model.Transaction, error)
	GetNextQueuedTransaction() (*model.Transaction, error)
}

type ledger interface {
	CreateLedger(ledger model.Ledger) (model.Ledger, error)
	GetAllLedgers() ([]model.Ledger, error)
	GetLedgerByID(id string) (*model.Ledger, error)
}

type balance interface {
	CreateBalance(balance model.Balance) (model.Balance, error)
	GetBalanceByID(id string, include []string) (*model.Balance, error)
	GetAllBalances() ([]model.Balance, error)
	UpdateBalance(balance *model.Balance) error
}

type account interface {
	CreateAccount(account model.Account) (model.Account, error)
	GetAccountByID(id string, include []string) (*model.Account, error)
	GetAllAccounts() ([]model.Account, error)
	GetAccountByNumber(number string) (*model.Account, error)
	UpdateAccount(account *model.Account) error
	DeleteAccount(id string) error
}

type balanceMonitor interface {
	CreateMonitor(monitor model.BalanceMonitor) (model.BalanceMonitor, error)
	GetMonitorByID(id string) (*model.BalanceMonitor, error)
	GetAllMonitors() ([]model.BalanceMonitor, error)
	GetBalanceMonitors(balanceID string) ([]model.BalanceMonitor, error)
	UpdateMonitor(monitor *model.BalanceMonitor) error
	DeleteMonitor(id string) error
}

type identity interface {
	CreateIdentity(identity model.Identity) (model.Identity, error)
	GetIdentityByID(id string) (*model.Identity, error)
	GetAllIdentities() ([]model.Identity, error)
	UpdateIdentity(identity *model.Identity) error
	DeleteIdentity(id string) error
}

type eventMapper interface {
	CreateEventMapper(mapper model.EventMapper) (model.EventMapper, error)
	GetAllEventMappers() ([]model.EventMapper, error)
	GetEventMapperByID(id string) (*model.EventMapper, error)
	UpdateEventMapper(mapper model.EventMapper) error
	DeleteEventMapper(id string) error
}
