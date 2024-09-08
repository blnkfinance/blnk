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
	reconciliation
}

type transaction interface {
	RecordTransaction(cxt context.Context, txn *model.Transaction) (*model.Transaction, error)
	GetTransaction(cxt context.Context, id string) (*model.Transaction, error)
	IsParentTransactionVoid(cxt context.Context, parentID string) (bool, error)
	GetTransactionByRef(cxt context.Context, reference string) (model.Transaction, error)
	TransactionExistsByRef(ctx context.Context, reference string) (bool, error)
	UpdateTransactionStatus(cxt context.Context, id string, status string) error
	GetAllTransactions(cxt context.Context) ([]model.Transaction, error)
	GetTotalCommittedTransactions(cxt context.Context, parentID string) (int64, error)
	GetTransactionsPaginated(ctx context.Context, id string, batchSize int, offset int64) ([]*model.Transaction, error)
	GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error)
	GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error)
	GroupTransactions(ctx context.Context, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error)
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

type reconciliation interface {
	RecordReconciliation(ctx context.Context, rec *model.Reconciliation) error
	GetReconciliation(ctx context.Context, id string) (*model.Reconciliation, error)
	UpdateReconciliationStatus(ctx context.Context, id string, status string, matchedCount, unmatchedCount int) error
	GetReconciliationsByUploadID(ctx context.Context, uploadID string) ([]*model.Reconciliation, error)
	RecordMatch(ctx context.Context, match *model.Match) error
	GetMatchesByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.Match, error)
	GetExternalTransactionsPaginated(ctx context.Context, uploadID string, batchSize int, offset int64) ([]*model.ExternalTransaction, error)
	RecordExternalTransaction(ctx context.Context, tx *model.ExternalTransaction, reconciliationID string) error
	RecordMatchingRule(ctx context.Context, rule *model.MatchingRule) error
	GetMatchingRules(ctx context.Context) ([]*model.MatchingRule, error)
	GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error)
	UpdateMatchingRule(ctx context.Context, rule *model.MatchingRule) error
	DeleteMatchingRule(ctx context.Context, id string) error
	SaveReconciliationProgress(ctx context.Context, reconciliationID string, progress model.ReconciliationProgress) error
	LoadReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error)
	RecordMatches(ctx context.Context, reconciliationID string, matches []model.Match) error
	RecordUnmatched(ctx context.Context, reconciliationID string, results []string) error
	FetchAndGroupExternalTransactions(ctx context.Context, uploadID string, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error)
}
