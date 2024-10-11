/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package database

import (
	"context"

	"github.com/jerry-enebeli/blnk/model"
)

// IDataSource defines the interface for data source operations, grouping related functionalities.
type IDataSource interface {
	transaction    // Interface for transaction-related operations
	ledger         // Interface for ledger-related operations
	balance        // Interface for balance-related operations
	identity       // Interface for identity-related operations
	balanceMonitor // Interface for balance monitoring operations
	account        // Interface for account-related operations
	reconciliation // Interface for reconciliation-related operations
}

// transaction defines methods for handling transactions.
type transaction interface {
	RecordTransaction(cxt context.Context, txn *model.Transaction) (*model.Transaction, error)                                                      // Records a new transaction
	GetTransaction(cxt context.Context, id string) (*model.Transaction, error)                                                                      // Retrieves a transaction by ID
	IsParentTransactionVoid(cxt context.Context, parentID string) (bool, error)                                                                     // Checks if a parent transaction is void
	GetTransactionByRef(cxt context.Context, reference string) (model.Transaction, error)                                                           // Retrieves a transaction by reference
	TransactionExistsByRef(ctx context.Context, reference string) (bool, error)                                                                     // Checks if a transaction exists by reference
	UpdateTransactionStatus(cxt context.Context, id string, status string) error                                                                    // Updates the status of a transaction
	GetAllTransactions(cxt context.Context, limit, offset int) ([]model.Transaction, error)                                                         // Retrieves all transactions
	GetTotalCommittedTransactions(cxt context.Context, parentID string) (int64, error)                                                              // Gets the total count of committed transactions for a parent
	GetTransactionsPaginated(ctx context.Context, id string, batchSize int, offset int64) ([]*model.Transaction, error)                             // Retrieves transactions in a paginated manner
	GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error)   // Retrieves inflight transactions by parent ID
	GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) // Retrieves refundable transactions by parent ID
	GroupTransactions(ctx context.Context, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error)              // Groups transactions based on specified criteria
}

// ledger defines methods for handling ledgers.
type ledger interface {
	CreateLedger(ledger model.Ledger) (model.Ledger, error) // Creates a new ledger
	GetAllLedgers(limit, offset int) ([]model.Ledger, error)
	GetLedgerByID(id string) (*model.Ledger, error) // Retrieves a ledger by ID
}

// balance defines methods for handling balances.
type balance interface {
	CreateBalance(balance model.Balance) (model.Balance, error)                                 // Creates a new balance
	GetBalanceByID(id string, include []string) (*model.Balance, error)                         // Retrieves a balance by ID with additional data
	GetBalanceByIDLite(id string) (*model.Balance, error)                                       // Retrieves a balance by ID with minimal data
	GetAllBalances(limit, offset int) ([]model.Balance, error)                                  // Retrieves all balances
	UpdateBalance(balance *model.Balance) error                                                 // Updates a balance
	GetBalanceByIndicator(indicator, currency string) (*model.Balance, error)                   // Retrieves a balance by indicator and currency
	UpdateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error // Updates multiple balances
	GetSourceDestination(sourceId, destinationId string) ([]*model.Balance, error)              // Retrieves balances between source and destination
}

// account defines methods for handling accounts.
type account interface {
	CreateAccount(account model.Account) (model.Account, error)         // Creates a new account
	GetAccountByID(id string, include []string) (*model.Account, error) // Retrieves an account by ID with additional data
	GetAllAccounts() ([]model.Account, error)                           // Retrieves all accounts
	GetAccountByNumber(number string) (*model.Account, error)           // Retrieves an account by its number
	UpdateAccount(account *model.Account) error                         // Updates an account
	DeleteAccount(id string) error                                      // Deletes an account
}

// balanceMonitor defines methods for monitoring balances.
type balanceMonitor interface {
	CreateMonitor(monitor model.BalanceMonitor) (model.BalanceMonitor, error) // Creates a new balance monitor
	GetMonitorByID(id string) (*model.BalanceMonitor, error)                  // Retrieves a balance monitor by ID
	GetAllMonitors() ([]model.BalanceMonitor, error)                          // Retrieves all balance monitors
	GetBalanceMonitors(balanceID string) ([]model.BalanceMonitor, error)      // Retrieves monitors for a specific balance
	UpdateMonitor(monitor *model.BalanceMonitor) error                        // Updates a balance monitor
	DeleteMonitor(id string) error                                            // Deletes a balance monitor
}

// identity defines methods for handling identities.
type identity interface {
	CreateIdentity(identity model.Identity) (model.Identity, error) // Creates a new identity
	GetIdentityByID(id string) (*model.Identity, error)             // Retrieves an identity by ID
	GetAllIdentities() ([]model.Identity, error)                    // Retrieves all identities
	UpdateIdentity(identity *model.Identity) error                  // Updates an identity
	DeleteIdentity(id string) error                                 // Deletes an identity
}

// reconciliation defines methods for handling reconciliation processes.
type reconciliation interface {
	RecordReconciliation(ctx context.Context, rec *model.Reconciliation) error                                                                                          // Records a new reconciliation
	GetReconciliation(ctx context.Context, id string) (*model.Reconciliation, error)                                                                                    // Retrieves a reconciliation by ID
	UpdateReconciliationStatus(ctx context.Context, id string, status string, matchedCount, unmatchedCount int) error                                                   // Updates the status of a reconciliation
	GetReconciliationsByUploadID(ctx context.Context, uploadID string) ([]*model.Reconciliation, error)                                                                 // Retrieves reconciliations by upload ID
	RecordMatch(ctx context.Context, match *model.Match) error                                                                                                          // Records a match in reconciliation
	GetMatchesByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.Match, error)                                                                  // Retrieves matches by reconciliation ID
	GetExternalTransactionsPaginated(ctx context.Context, uploadID string, batchSize int, offset int64) ([]*model.ExternalTransaction, error)                           // Retrieves external transactions in a paginated manner
	RecordExternalTransaction(ctx context.Context, tx *model.ExternalTransaction, reconciliationID string) error                                                        // Records an external transaction
	RecordMatchingRule(ctx context.Context, rule *model.MatchingRule) error                                                                                             // Records a matching rule
	GetMatchingRules(ctx context.Context) ([]*model.MatchingRule, error)                                                                                                // Retrieves all matching rules
	GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error)                                                                                        // Retrieves a matching rule by ID
	UpdateMatchingRule(ctx context.Context, rule *model.MatchingRule) error                                                                                             // Updates a matching rule
	DeleteMatchingRule(ctx context.Context, id string) error                                                                                                            // Deletes a matching rule
	SaveReconciliationProgress(ctx context.Context, reconciliationID string, progress model.ReconciliationProgress) error                                               // Saves reconciliation progress
	LoadReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error)                                                      // Loads reconciliation progress
	RecordMatches(ctx context.Context, reconciliationID string, matches []model.Match) error                                                                            // Records matches for a reconciliation
	RecordUnmatched(ctx context.Context, reconciliationID string, results []string) error                                                                               // Records unmatched results for a reconciliation
	FetchAndGroupExternalTransactions(ctx context.Context, uploadID string, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) // Fetches and groups external transactions based on criteria
}
