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
	"database/sql"
	"errors"
	"fmt"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/stretchr/testify/assert"
)

type mockCacheRecon struct {
	data map[string]interface{}
}

func newMockCacheRecon() *mockCacheRecon {
	return &mockCacheRecon{data: make(map[string]interface{})}
}

func (m *mockCacheRecon) Set(ctx context.Context, key string, value interface{}, ttl time.Duration) error {
	m.data[key] = value
	return nil
}

func (m *mockCacheRecon) Get(ctx context.Context, key string, data interface{}) error {
	if _, ok := m.data[key]; ok {
		return nil
	}
	return errors.New("cache miss")
}

func (m *mockCacheRecon) Delete(ctx context.Context, key string) error {
	delete(m.data, key)
	return nil
}

func TestRecordReconciliation_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()
	completedAt := time.Now()

	rec := &model.Reconciliation{
		ReconciliationID:      "rec123",
		UploadID:              "upl123",
		Status:                "completed",
		MatchedTransactions:   10,
		UnmatchedTransactions: 5,
		StartedAt:             time.Now(),
		CompletedAt:           &completedAt,
	}

	mock.ExpectExec("INSERT INTO blnk.reconciliations").
		WithArgs(rec.ReconciliationID, rec.UploadID, rec.Status, rec.MatchedTransactions,
			rec.UnmatchedTransactions, rec.StartedAt, rec.CompletedAt).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.RecordReconciliation(ctx, rec)
	assert.NoError(t, err)
}

func TestRecordReconciliation_Fail(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	completedAt := time.Now()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	rec := &model.Reconciliation{
		ReconciliationID:      "rec123",
		UploadID:              "upl123",
		Status:                "completed",
		MatchedTransactions:   10,
		UnmatchedTransactions: 5,
		StartedAt:             time.Now(),
		CompletedAt:           &completedAt,
	}

	mock.ExpectExec("INSERT INTO blnk.reconciliations").
		WithArgs(rec.ReconciliationID, rec.UploadID, rec.Status, rec.MatchedTransactions,
			rec.UnmatchedTransactions, rec.StartedAt, rec.CompletedAt).
		WillReturnError(fmt.Errorf("failed to insert"))

	err = ds.RecordReconciliation(ctx, rec)
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestGetReconciliation_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()
	completedAt := time.Now()
	ds := Datasource{Conn: db}
	ctx := context.TODO()

	expectedRec := &model.Reconciliation{
		ID:                    1,
		ReconciliationID:      "rec123",
		UploadID:              "upl123",
		Status:                "completed",
		MatchedTransactions:   10,
		UnmatchedTransactions: 5,
		StartedAt:             time.Now(),
		CompletedAt:           &completedAt,
	}

	mock.ExpectQuery("SELECT id, reconciliation_id, upload_id, status").
		WithArgs("rec123").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "reconciliation_id", "upload_id", "status", "matched_transactions", "unmatched_transactions", "started_at", "completed_at",
		}).AddRow(expectedRec.ID, expectedRec.ReconciliationID, expectedRec.UploadID, expectedRec.Status, expectedRec.MatchedTransactions,
			expectedRec.UnmatchedTransactions, expectedRec.StartedAt, expectedRec.CompletedAt))

	rec, err := ds.GetReconciliation(ctx, "rec123")
	assert.NoError(t, err)
	assert.Equal(t, expectedRec.ReconciliationID, rec.ReconciliationID)
}

func TestGetReconciliation_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT id, reconciliation_id, upload_id, status").
		WithArgs("rec123").
		WillReturnError(sql.ErrNoRows)

	_, err = ds.GetReconciliation(ctx, "rec123")
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrNotFound, err.(apierror.APIError).Code)
}

func TestUpdateReconciliationStatus_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectExec("UPDATE blnk.reconciliations").
		WithArgs("rec123", "completed", 10, 5, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.UpdateReconciliationStatus(ctx, "rec123", "completed", 10, 5)
	assert.NoError(t, err)
}

func TestUpdateReconciliationStatus_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectExec("UPDATE blnk.reconciliations").
		WithArgs("rec123", "completed", 10, 5, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 0))

	err = ds.UpdateReconciliationStatus(ctx, "rec123", "completed", 10, 5)
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrNotFound, err.(apierror.APIError).Code)
}

func TestUpdateReconciliationStatus_Fail(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectExec("UPDATE blnk.reconciliations").
		WithArgs("rec123", "completed", 10, 5, sqlmock.AnyArg()).
		WillReturnError(fmt.Errorf("failed to update"))

	err = ds.UpdateReconciliationStatus(ctx, "rec123", "completed", 10, 5)
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestRecordMatches_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	matches := []model.Match{
		{
			ExternalTransactionID: "ext1",
			InternalTransactionID: "int1",
			ReconciliationID:      "rec123",
			Amount:                1000,
			Date:                  time.Now(),
		},
		{
			ExternalTransactionID: "ext2",
			InternalTransactionID: "int2",
			ReconciliationID:      "rec123",
			Amount:                2000,
			Date:                  time.Now(),
		},
	}

	mock.ExpectBegin()

	for _, match := range matches {
		mock.ExpectExec("INSERT INTO blnk.matches").
			WithArgs(match.ExternalTransactionID, match.InternalTransactionID, match.ReconciliationID, match.Amount, match.Date).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	mock.ExpectCommit()

	err = ds.RecordMatches(ctx, "rec123", matches)
	assert.NoError(t, err)
}

func TestRecordMatches_Fail(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	matches := []model.Match{
		{
			ExternalTransactionID: "ext1",
			InternalTransactionID: "int1",
			ReconciliationID:      "rec123",
			Amount:                1000,
			Date:                  time.Now(),
		},
	}

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO blnk.matches").
		WithArgs(matches[0].ExternalTransactionID, matches[0].InternalTransactionID, matches[0].ReconciliationID, matches[0].Amount, matches[0].Date).
		WillReturnError(fmt.Errorf("failed to insert match"))
	mock.ExpectRollback()

	err = ds.RecordMatches(ctx, "rec123", matches)
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestGetReconciliationsByUploadID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()
	completedAt := time.Now()

	mock.ExpectQuery("SELECT id, reconciliation_id, upload_id, status, matched_transactions, unmatched_transactions, started_at, completed_at FROM blnk.reconciliations WHERE upload_id").
		WithArgs("upl123").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "reconciliation_id", "upload_id", "status", "matched_transactions", "unmatched_transactions", "started_at", "completed_at",
		}).
			AddRow(1, "rec1", "upl123", "completed", 10, 5, time.Now(), &completedAt).
			AddRow(2, "rec2", "upl123", "pending", 0, 0, time.Now(), nil))

	recs, err := ds.GetReconciliationsByUploadID(ctx, "upl123")
	assert.NoError(t, err)
	assert.Len(t, recs, 2)
	assert.Equal(t, "rec1", recs[0].ReconciliationID)
	assert.Equal(t, "rec2", recs[1].ReconciliationID)
}

func TestGetReconciliationsByUploadID_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT id, reconciliation_id, upload_id, status, matched_transactions, unmatched_transactions, started_at, completed_at FROM blnk.reconciliations WHERE upload_id").
		WithArgs("upl123").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "reconciliation_id", "upload_id", "status", "matched_transactions", "unmatched_transactions", "started_at", "completed_at",
		}))

	recs, err := ds.GetReconciliationsByUploadID(ctx, "upl123")
	assert.NoError(t, err)
	assert.Len(t, recs, 0)
}

func TestGetReconciliationsByUploadID_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT id, reconciliation_id, upload_id, status").
		WithArgs("upl123").
		WillReturnError(fmt.Errorf("database error"))

	recs, err := ds.GetReconciliationsByUploadID(ctx, "upl123")
	assert.Error(t, err)
	assert.Nil(t, recs)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestRecordUnmatched_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	externalIDs := []string{"ext1", "ext2", "ext3"}

	mock.ExpectBegin()
	for _, extID := range externalIDs {
		mock.ExpectExec("INSERT INTO blnk.unmatched").
			WithArgs(extID, "rec123", sqlmock.AnyArg()).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}
	mock.ExpectCommit()

	err = ds.RecordUnmatched(ctx, "rec123", externalIDs)
	assert.NoError(t, err)
}

func TestRecordUnmatched_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectBegin()
	mock.ExpectExec("INSERT INTO blnk.unmatched").
		WithArgs("ext1", "rec123", sqlmock.AnyArg()).
		WillReturnError(fmt.Errorf("insert error"))
	mock.ExpectRollback()

	err = ds.RecordUnmatched(ctx, "rec123", []string{"ext1"})
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestRecordMatch_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	match := &model.Match{
		ExternalTransactionID: "ext1",
		InternalTransactionID: "int1",
		ReconciliationID:      "rec123",
		Amount:                1000,
		Date:                  time.Now(),
	}

	mock.ExpectExec("INSERT INTO blnk.matches").
		WithArgs(match.ExternalTransactionID, match.InternalTransactionID, match.ReconciliationID, match.Amount, match.Date).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.RecordMatch(ctx, match)
	assert.NoError(t, err)
}

func TestRecordMatch_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	match := &model.Match{
		ExternalTransactionID: "ext1",
		InternalTransactionID: "int1",
		ReconciliationID:      "rec123",
		Amount:                1000,
		Date:                  time.Now(),
	}

	mock.ExpectExec("INSERT INTO blnk.matches").
		WithArgs(match.ExternalTransactionID, match.InternalTransactionID, match.ReconciliationID, match.Amount, match.Date).
		WillReturnError(fmt.Errorf("insert error"))

	err = ds.RecordMatch(ctx, match)
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestGetMatchesByReconciliationID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT m.external_transaction_id, m.internal_transaction_id, m.amount, m.date FROM blnk.matches m").
		WithArgs("rec123").
		WillReturnRows(sqlmock.NewRows([]string{
			"external_transaction_id", "internal_transaction_id", "amount", "date",
		}).
			AddRow("ext1", "int1", 1000.0, time.Now()).
			AddRow("ext2", "int2", 2000.0, time.Now()))

	matches, err := ds.GetMatchesByReconciliationID(ctx, "rec123")
	assert.NoError(t, err)
	assert.Len(t, matches, 2)
	assert.Equal(t, "ext1", matches[0].ExternalTransactionID)
}

func TestGetMatchesByReconciliationID_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT m.external_transaction_id, m.internal_transaction_id, m.amount, m.date FROM blnk.matches m").
		WithArgs("rec123").
		WillReturnError(fmt.Errorf("query error"))

	matches, err := ds.GetMatchesByReconciliationID(ctx, "rec123")
	assert.Error(t, err)
	assert.Nil(t, matches)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestRecordExternalTransaction_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	extTxn := &model.ExternalTransaction{
		ID:          "ext1",
		Amount:      1000.0,
		Reference:   "ref123",
		Currency:    "USD",
		Description: "Test transaction",
		Date:        time.Now(),
		Source:      "bank",
	}
	uploadID := "upl123"

	mock.ExpectExec("INSERT INTO blnk.external_transactions").
		WithArgs(extTxn.ID, extTxn.Amount, extTxn.Reference, extTxn.Currency, extTxn.Description, extTxn.Date, extTxn.Source, uploadID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.RecordExternalTransaction(ctx, extTxn, uploadID)
	assert.NoError(t, err)
}

func TestRecordExternalTransaction_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	extTxn := &model.ExternalTransaction{
		ID:          "ext1",
		Amount:      1000.0,
		Reference:   "ref123",
		Currency:    "USD",
		Description: "Test transaction",
		Date:        time.Now(),
		Source:      "bank",
	}
	uploadID := "upl123"

	mock.ExpectExec("INSERT INTO blnk.external_transactions").
		WithArgs(extTxn.ID, extTxn.Amount, extTxn.Reference, extTxn.Currency, extTxn.Description, extTxn.Date, extTxn.Source, uploadID).
		WillReturnError(fmt.Errorf("insert error"))

	err = ds.RecordExternalTransaction(ctx, extTxn, uploadID)
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestRecordMatchingRule_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	rule := &model.MatchingRule{
		RuleID:      "rule1",
		Name:        "Test Rule",
		Description: "A test matching rule",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Criteria:    []model.MatchingCriteria{{Field: "amount", Operator: "equals", Value: "100"}},
	}

	mock.ExpectExec("INSERT INTO blnk.matching_rules").
		WithArgs(rule.RuleID, sqlmock.AnyArg(), sqlmock.AnyArg(), rule.Name, rule.Description, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.RecordMatchingRule(ctx, rule)
	assert.NoError(t, err)
}

func TestRecordMatchingRule_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	rule := &model.MatchingRule{
		RuleID:      "rule1",
		Name:        "Test Rule",
		Description: "A test matching rule",
		CreatedAt:   time.Now(),
		UpdatedAt:   time.Now(),
		Criteria:    []model.MatchingCriteria{{Field: "amount", Operator: "equals", Value: "100"}},
	}

	mock.ExpectExec("INSERT INTO blnk.matching_rules").
		WithArgs(rule.RuleID, sqlmock.AnyArg(), sqlmock.AnyArg(), rule.Name, rule.Description, sqlmock.AnyArg()).
		WillReturnError(fmt.Errorf("insert error"))

	err = ds.RecordMatchingRule(ctx, rule)
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestGetMatchingRules_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT id, rule_id, created_at, updated_at, name, description, criteria FROM blnk.matching_rules").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "rule_id", "created_at", "updated_at", "name", "description", "criteria",
		}).
			AddRow(1, "rule1", time.Now(), time.Now(), "Rule 1", "Description 1", []byte(`[]`)).
			AddRow(2, "rule2", time.Now(), time.Now(), "Rule 2", "Description 2", []byte(`[]`)))

	rules, err := ds.GetMatchingRules(ctx)
	assert.NoError(t, err)
	assert.Len(t, rules, 2)
	assert.Equal(t, "rule1", rules[0].RuleID)
}

func TestGetMatchingRules_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT id, rule_id, created_at, updated_at, name, description, criteria FROM blnk.matching_rules").
		WillReturnError(fmt.Errorf("query error"))

	rules, err := ds.GetMatchingRules(ctx)
	assert.Error(t, err)
	assert.Nil(t, rules)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestGetMatchingRule_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT id, rule_id, created_at, updated_at, name, description, criteria FROM blnk.matching_rules WHERE rule_id").
		WithArgs("rule1").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "rule_id", "created_at", "updated_at", "name", "description", "criteria",
		}).AddRow(1, "rule1", time.Now(), time.Now(), "Rule 1", "Description 1", []byte(`[]`)))

	rule, err := ds.GetMatchingRule(ctx, "rule1")
	assert.NoError(t, err)
	assert.Equal(t, "rule1", rule.RuleID)
}

func TestGetMatchingRule_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT id, rule_id, created_at, updated_at, name, description, criteria FROM blnk.matching_rules WHERE rule_id").
		WithArgs("rule_notfound").
		WillReturnError(sql.ErrNoRows)

	rule, err := ds.GetMatchingRule(ctx, "rule_notfound")
	assert.Error(t, err)
	assert.Nil(t, rule)
	assert.Equal(t, apierror.ErrNotFound, err.(apierror.APIError).Code)
}

func TestDeleteMatchingRule_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectExec("DELETE FROM blnk.matching_rules WHERE rule_id").
		WithArgs("rule1").
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = ds.DeleteMatchingRule(ctx, "rule1")
	assert.NoError(t, err)
}

func TestDeleteMatchingRule_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectExec("DELETE FROM blnk.matching_rules WHERE rule_id").
		WithArgs("rule_notfound").
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = ds.DeleteMatchingRule(ctx, "rule_notfound")
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrNotFound, err.(apierror.APIError).Code)
}

func TestUpdateMatchingRule_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	rule := &model.MatchingRule{
		RuleID:      "rule1",
		Name:        "Updated Rule",
		Description: "Updated description",
		Criteria:    []model.MatchingCriteria{{Field: "amount", Operator: "equals", Value: "200"}},
	}

	mock.ExpectExec("UPDATE blnk.matching_rules SET name").
		WithArgs(rule.RuleID, rule.Name, rule.Description, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 1))

	err = ds.UpdateMatchingRule(ctx, rule)
	assert.NoError(t, err)
}

func TestUpdateMatchingRule_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	rule := &model.MatchingRule{
		RuleID:      "rule_notfound",
		Name:        "Updated Rule",
		Description: "Updated description",
		Criteria:    []model.MatchingCriteria{},
	}

	mock.ExpectExec("UPDATE blnk.matching_rules SET name").
		WithArgs(rule.RuleID, rule.Name, rule.Description, sqlmock.AnyArg()).
		WillReturnResult(sqlmock.NewResult(0, 0))

	err = ds.UpdateMatchingRule(ctx, rule)
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrNotFound, err.(apierror.APIError).Code)
}

func TestGetExternalTransactionsByReconciliationID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT id, amount, reference, currency, description, date, source FROM blnk.external_transactions WHERE reconciliation_id").
		WithArgs("rec123").
		WillReturnRows(sqlmock.NewRows([]string{
			"id", "amount", "reference", "currency", "description", "date", "source",
		}).
			AddRow("ext1", 1000.0, "ref1", "USD", "Test 1", time.Now(), "bank").
			AddRow("ext2", 2000.0, "ref2", "USD", "Test 2", time.Now(), "bank"))

	txns, err := ds.GetExternalTransactionsByReconciliationID(ctx, "rec123")
	assert.NoError(t, err)
	assert.Len(t, txns, 2)
	assert.Equal(t, "ext1", txns[0].ID)
}

func TestGetExternalTransactionsByReconciliationID_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT id, amount, reference, currency, description, date, source FROM blnk.external_transactions").
		WithArgs("rec123").
		WillReturnError(fmt.Errorf("database error"))

	txns, err := ds.GetExternalTransactionsByReconciliationID(ctx, "rec123")
	assert.Error(t, err)
	assert.Nil(t, txns)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestSaveReconciliationProgress_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	progress := model.ReconciliationProgress{
		ProcessedCount:             100,
		LastProcessedExternalTxnID: "ext100",
	}

	mock.ExpectExec("INSERT INTO blnk.reconciliation_progress").
		WithArgs("rec123", progress.ProcessedCount, progress.LastProcessedExternalTxnID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.SaveReconciliationProgress(ctx, "rec123", progress)
	assert.NoError(t, err)
}

func TestSaveReconciliationProgress_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	progress := model.ReconciliationProgress{
		ProcessedCount:             100,
		LastProcessedExternalTxnID: "ext100",
	}

	mock.ExpectExec("INSERT INTO blnk.reconciliation_progress").
		WithArgs("rec123", progress.ProcessedCount, progress.LastProcessedExternalTxnID).
		WillReturnError(fmt.Errorf("database error"))

	err = ds.SaveReconciliationProgress(ctx, "rec123", progress)
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestLoadReconciliationProgress_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT processed_count, last_processed_external_txn_id FROM blnk.reconciliation_progress WHERE reconciliation_id").
		WithArgs("rec123").
		WillReturnRows(sqlmock.NewRows([]string{"processed_count", "last_processed_external_txn_id"}).
			AddRow(100, "ext100"))

	progress, err := ds.LoadReconciliationProgress(ctx, "rec123")
	assert.NoError(t, err)
	assert.Equal(t, 100, progress.ProcessedCount)
	assert.Equal(t, "ext100", progress.LastProcessedExternalTxnID)
}

func TestLoadReconciliationProgress_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT processed_count, last_processed_external_txn_id FROM blnk.reconciliation_progress WHERE reconciliation_id").
		WithArgs("rec_notfound").
		WillReturnError(sql.ErrNoRows)

	progress, err := ds.LoadReconciliationProgress(ctx, "rec_notfound")
	assert.NoError(t, err)
	assert.Equal(t, 0, progress.ProcessedCount)
	assert.Equal(t, "", progress.LastProcessedExternalTxnID)
}

func TestLoadReconciliationProgress_Error(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	ds := Datasource{Conn: db}
	ctx := context.TODO()

	mock.ExpectQuery("SELECT processed_count, last_processed_external_txn_id FROM blnk.reconciliation_progress WHERE reconciliation_id").
		WithArgs("rec123").
		WillReturnError(fmt.Errorf("database error"))

	_, err = ds.LoadReconciliationProgress(ctx, "rec123")
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestGetExternalTransactionsPaginated_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCacheRecon()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()

	uploadID := "upload123"
	txDate := time.Now()

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, amount, reference, currency, description, date, source
		FROM blnk.external_transactions
		WHERE upload_id = $1
		ORDER BY date DESC
		LIMIT $2 OFFSET $3`)).
		WithArgs(uploadID, 10, int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "amount", "reference", "currency", "description", "date", "source"}).
			AddRow("ext_123", 100.0, "ref_123", "USD", "test desc", txDate, "external_source"))

	txns, err := ds.GetExternalTransactionsPaginated(ctx, uploadID, 10, 0)
	assert.NoError(t, err)
	assert.Len(t, txns, 1)
	assert.Equal(t, "ext_123", txns[0].ID)
	assert.Equal(t, 100.0, txns[0].Amount)
}

func TestGetExternalTransactionsPaginated_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCacheRecon()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()

	uploadID := "upload123"

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, amount, reference, currency, description, date, source
		FROM blnk.external_transactions
		WHERE upload_id = $1
		ORDER BY date DESC
		LIMIT $2 OFFSET $3`)).
		WithArgs(uploadID, 10, int64(0)).
		WillReturnError(sql.ErrConnDone)

	txns, err := ds.GetExternalTransactionsPaginated(ctx, uploadID, 10, 0)
	assert.Error(t, err)
	assert.Nil(t, txns)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestGetExternalTransactionsPaginated_EmptyResults(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCacheRecon()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()

	uploadID := "upload123"

	mock.ExpectQuery(regexp.QuoteMeta(`SELECT id, amount, reference, currency, description, date, source
		FROM blnk.external_transactions
		WHERE upload_id = $1
		ORDER BY date DESC
		LIMIT $2 OFFSET $3`)).
		WithArgs(uploadID, 10, int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"id", "amount", "reference", "currency", "description", "date", "source"}))

	txns, err := ds.GetExternalTransactionsPaginated(ctx, uploadID, 10, 0)
	assert.NoError(t, err)
	assert.Len(t, txns, 0)
}

func TestFetchAndGroupExternalTransactions_InvalidGroupCriteria(t *testing.T) {
	db, _, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCacheRecon()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()

	_, err = ds.FetchAndGroupExternalTransactions(ctx, "upload123", "invalid_field", 10, 0)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrBadRequest, apiErr.Code)
}

func TestFetchAndGroupExternalTransactions_QueryError(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCacheRecon()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()

	mock.ExpectQuery("SELECT").
		WillReturnError(sql.ErrConnDone)

	_, err = ds.FetchAndGroupExternalTransactions(ctx, "upload123", "amount", 10, 0)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok)
	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
}

func TestFetchAndGroupExternalTransactions_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer func() { _ = db.Close() }()

	mc := newMockCacheRecon()
	ds := Datasource{Conn: db, Cache: mc}
	ctx := context.Background()

	txDate := time.Now()

	mock.ExpectQuery("SELECT").
		WithArgs("amount", "upload123", 10, int64(0)).
		WillReturnRows(sqlmock.NewRows([]string{"group_key", "id", "amount", "reference", "currency", "description", "date", "source"}).
			AddRow("100.00", "ext_123", 100.0, "ref_123", "USD", "test desc", txDate, "external_source"))

	result, err := ds.FetchAndGroupExternalTransactions(ctx, "upload123", "amount", 10, 0)
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Len(t, result, 1)
}
