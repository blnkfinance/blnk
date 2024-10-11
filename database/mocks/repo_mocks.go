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
package mocks

import (
	"context"

	"github.com/jerry-enebeli/blnk/model"
	"github.com/stretchr/testify/mock"
)

// MockDataSource is a mock implementation of the IDataSource interface
type MockDataSource struct {
	mock.Mock
}

// Transaction methods

func (m *MockDataSource) RecordTransaction(ctx context.Context, txn *model.Transaction) (*model.Transaction, error) {
	args := m.Called(ctx, txn)
	return args.Get(0).(*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetTransaction(ctx context.Context, id string) (*model.Transaction, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(*model.Transaction), args.Error(1)
}

func (m *MockDataSource) IsParentTransactionVoid(ctx context.Context, parentID string) (bool, error) {
	args := m.Called(ctx, parentID)
	return args.Bool(0), args.Error(1)
}

func (m *MockDataSource) GetTransactionByRef(ctx context.Context, reference string) (model.Transaction, error) {
	args := m.Called(ctx, reference)
	return args.Get(0).(model.Transaction), args.Error(1)
}

func (m *MockDataSource) TransactionExistsByRef(ctx context.Context, reference string) (bool, error) {
	args := m.Called(ctx, reference)
	return args.Bool(0), args.Error(1)
}

func (m *MockDataSource) UpdateTransactionStatus(ctx context.Context, id string, status string) error {
	args := m.Called(ctx, id, status)
	return args.Error(0)
}

func (m *MockDataSource) GetAllTransactions(ctx context.Context, limit, offset int) ([]model.Transaction, error) {
	args := m.Called(limit, offset)
	return args.Get(0).([]model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetTotalCommittedTransactions(ctx context.Context, parentID string) (int64, error) {
	args := m.Called(ctx, parentID)
	return args.Get(0).(int64), args.Error(1)
}

func (m *MockDataSource) GetTransactionsPaginated(ctx context.Context, id string, batchSize int, offset int64) ([]*model.Transaction, error) {
	args := m.Called(ctx, id, batchSize, offset)
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	args := m.Called(ctx, parentTransactionID, batchSize, offset)
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	args := m.Called(ctx, parentTransactionID, batchSize, offset)
	return args.Get(0).([]*model.Transaction), args.Error(1)
}

func (m *MockDataSource) GroupTransactions(ctx context.Context, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	args := m.Called(ctx, groupCriteria, batchSize, offset)
	return args.Get(0).(map[string][]*model.Transaction), args.Error(1)
}

// Ledger methods

func (m *MockDataSource) CreateLedger(ledger model.Ledger) (model.Ledger, error) {
	args := m.Called(ledger)
	return args.Get(0).(model.Ledger), args.Error(1)
}

func (m *MockDataSource) GetAllLedgers(limit, offset int) ([]model.Ledger, error) {
	args := m.Called(limit, offset)
	return args.Get(0).([]model.Ledger), args.Error(1)
}

func (m *MockDataSource) GetLedgerByID(id string) (*model.Ledger, error) {
	args := m.Called(id)
	return args.Get(0).(*model.Ledger), args.Error(1)
}

// Balance methods

func (m *MockDataSource) CreateBalance(balance model.Balance) (model.Balance, error) {
	args := m.Called(balance)
	return args.Get(0).(model.Balance), args.Error(1)
}

func (m *MockDataSource) GetBalanceByID(id string, include []string) (*model.Balance, error) {
	args := m.Called(id, include)
	return args.Get(0).(*model.Balance), args.Error(1)
}

func (m *MockDataSource) GetBalanceByIDLite(id string) (*model.Balance, error) {
	args := m.Called(id)
	return args.Get(0).(*model.Balance), args.Error(1)
}

func (m *MockDataSource) GetAllBalances(limit, offset int) ([]model.Balance, error) {
	args := m.Called(limit, offset)
	return args.Get(0).([]model.Balance), args.Error(1)
}

func (m *MockDataSource) UpdateBalance(balance *model.Balance) error {
	args := m.Called(balance)
	return args.Error(0)
}

func (m *MockDataSource) GetBalanceByIndicator(indicator, currency string) (*model.Balance, error) {
	args := m.Called(indicator, currency)
	return args.Get(0).(*model.Balance), args.Error(1)
}

func (m *MockDataSource) UpdateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	args := m.Called(ctx, sourceBalance, destinationBalance)
	return args.Error(0)
}

func (m *MockDataSource) GetSourceDestination(sourceId, destinationId string) ([]*model.Balance, error) {
	args := m.Called(sourceId, destinationId)
	return args.Get(0).([]*model.Balance), args.Error(1)
}

// Account methods

func (m *MockDataSource) CreateAccount(account model.Account) (model.Account, error) {
	args := m.Called(account)
	return args.Get(0).(model.Account), args.Error(1)
}

func (m *MockDataSource) GetAccountByID(id string, include []string) (*model.Account, error) {
	args := m.Called(id, include)
	return args.Get(0).(*model.Account), args.Error(1)
}

func (m *MockDataSource) GetAllAccounts() ([]model.Account, error) {
	args := m.Called()
	return args.Get(0).([]model.Account), args.Error(1)
}

func (m *MockDataSource) GetAccountByNumber(number string) (*model.Account, error) {
	args := m.Called(number)
	return args.Get(0).(*model.Account), args.Error(1)
}

func (m *MockDataSource) UpdateAccount(account *model.Account) error {
	args := m.Called(account)
	return args.Error(0)
}

func (m *MockDataSource) DeleteAccount(id string) error {
	args := m.Called(id)
	return args.Error(0)
}

// BalanceMonitor methods

func (m *MockDataSource) CreateMonitor(monitor model.BalanceMonitor) (model.BalanceMonitor, error) {
	args := m.Called(monitor)
	return args.Get(0).(model.BalanceMonitor), args.Error(1)
}

func (m *MockDataSource) GetMonitorByID(id string) (*model.BalanceMonitor, error) {
	args := m.Called(id)
	return args.Get(0).(*model.BalanceMonitor), args.Error(1)
}

func (m *MockDataSource) GetAllMonitors() ([]model.BalanceMonitor, error) {
	args := m.Called()
	return args.Get(0).([]model.BalanceMonitor), args.Error(1)
}

func (m *MockDataSource) GetBalanceMonitors(balanceID string) ([]model.BalanceMonitor, error) {
	args := m.Called(balanceID)
	return args.Get(0).([]model.BalanceMonitor), args.Error(1)
}

func (m *MockDataSource) UpdateMonitor(monitor *model.BalanceMonitor) error {
	args := m.Called(monitor)
	return args.Error(0)
}

func (m *MockDataSource) DeleteMonitor(id string) error {
	args := m.Called(id)
	return args.Error(0)
}

// Identity methods

func (m *MockDataSource) CreateIdentity(identity model.Identity) (model.Identity, error) {
	args := m.Called(identity)
	return args.Get(0).(model.Identity), args.Error(1)
}

func (m *MockDataSource) GetIdentityByID(id string) (*model.Identity, error) {
	args := m.Called(id)
	return args.Get(0).(*model.Identity), args.Error(1)
}

func (m *MockDataSource) GetAllIdentities() ([]model.Identity, error) {
	args := m.Called()
	return args.Get(0).([]model.Identity), args.Error(1)
}

func (m *MockDataSource) UpdateIdentity(identity *model.Identity) error {
	args := m.Called(identity)
	return args.Error(0)
}

func (m *MockDataSource) DeleteIdentity(id string) error {
	args := m.Called(id)
	return args.Error(0)
}

// Reconciliation methods

func (m *MockDataSource) RecordReconciliation(ctx context.Context, rec *model.Reconciliation) error {
	args := m.Called(ctx, rec)
	return args.Error(0)
}

func (m *MockDataSource) GetReconciliation(ctx context.Context, id string) (*model.Reconciliation, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(*model.Reconciliation), args.Error(1)
}

func (m *MockDataSource) UpdateReconciliationStatus(ctx context.Context, id string, status string, matchedCount, unmatchedCount int) error {
	args := m.Called(ctx, id, status, matchedCount, unmatchedCount)
	return args.Error(0)
}

func (m *MockDataSource) GetReconciliationsByUploadID(ctx context.Context, uploadID string) ([]*model.Reconciliation, error) {
	args := m.Called(ctx, uploadID)
	return args.Get(0).([]*model.Reconciliation), args.Error(1)
}

func (m *MockDataSource) RecordMatch(ctx context.Context, match *model.Match) error {
	args := m.Called(ctx, match)
	return args.Error(0)
}

func (m *MockDataSource) RecordMatches(ctx context.Context, reconciliationID string, match []model.Match) error {
	args := m.Called(ctx, reconciliationID, match)
	return args.Error(0)
}

func (m *MockDataSource) RecordUnmatched(ctx context.Context, reconciliationID string, unmatched []string) error {
	args := m.Called(ctx, reconciliationID, unmatched)
	return args.Error(0)
}

func (m *MockDataSource) GetMatchesByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.Match, error) {
	args := m.Called(ctx, reconciliationID)
	return args.Get(0).([]*model.Match), args.Error(1)
}

func (m *MockDataSource) GetExternalTransactionsPaginated(ctx context.Context, uploadID string, batchSize int, offset int64) ([]*model.ExternalTransaction, error) {
	args := m.Called(ctx, uploadID, batchSize, offset)
	return args.Get(0).([]*model.ExternalTransaction), args.Error(1)
}

func (m *MockDataSource) RecordExternalTransaction(ctx context.Context, tx *model.ExternalTransaction, reconciliationID string) error {
	args := m.Called(ctx, tx, reconciliationID)
	return args.Error(0)
}

func (m *MockDataSource) RecordMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	args := m.Called(ctx, rule)
	return args.Error(0)
}

func (m *MockDataSource) GetMatchingRules(ctx context.Context) ([]*model.MatchingRule, error) {
	args := m.Called(ctx)
	return args.Get(0).([]*model.MatchingRule), args.Error(1)
}

func (m *MockDataSource) GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error) {
	args := m.Called(ctx, id)
	return args.Get(0).(*model.MatchingRule), args.Error(1)
}

func (m *MockDataSource) UpdateMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	args := m.Called(ctx, rule)
	return args.Error(0)
}

func (m *MockDataSource) DeleteMatchingRule(ctx context.Context, id string) error {
	args := m.Called(ctx, id)
	return args.Error(0)
}

func (m *MockDataSource) SaveReconciliationProgress(ctx context.Context, reconciliationID string, progress model.ReconciliationProgress) error {
	args := m.Called(ctx, reconciliationID, progress)
	return args.Error(0)
}

func (m *MockDataSource) LoadReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error) {
	args := m.Called(ctx, reconciliationID)
	return args.Get(0).(model.ReconciliationProgress), args.Error(1)
}

func (m *MockDataSource) FetchAndGroupExternalTransactions(ctx context.Context, uploadID string, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	args := m.Called(ctx, uploadID, groupCriteria, batchSize, offset)
	return args.Get(0).(map[string][]*model.Transaction), args.Error(1)
}
