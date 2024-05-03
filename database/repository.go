package database

import (
	"context"

	"github.com/jerry-enebeli/blnk/model"
)

type IDataSource interface {
	transaction
	ledger
	balance
	identity
	balanceMonitor
	account
}

type transaction interface {
	RecordTransaction(cxt context.Context, txn *model.Transaction) (*model.Transaction, error)
	GetTransaction(id string) (*model.Transaction, error)
	GetTransactionByRef(cxt context.Context, reference string) (model.Transaction, error)
	TransactionExistsByRef(ctx context.Context, reference string) (bool, error)
	UpdateTransactionStatus(id string, status string) error
	GetAllTransactions() ([]model.Transaction, error)
	GetTotalCommittedTransactions(parentID string) (int64, error)
}

type ledger interface {
	CreateLedger(ledger model.Ledger) (model.Ledger, error)
	GetAllLedgers() ([]model.Ledger, error)
	GetLedgerByID(id string) (*model.Ledger, error)
}

type balance interface {
	CreateBalance(balance model.Balance) (model.Balance, error)
	GetBalanceByID(id string, include []string) (*model.Balance, error)
	GetBalanceByIDLite(id string) (*model.Balance, error)
	GetAllBalances() ([]model.Balance, error)
	UpdateBalance(balance *model.Balance) error
	GetBalanceByIndicator(indicator, currency string) (*model.Balance, error)
	UpdateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error
	GetSourceDestination(sourceId, destinationId string) ([]*model.Balance, error)
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
