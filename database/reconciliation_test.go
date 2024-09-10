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
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/stretchr/testify/assert"
)

func TestRecordReconciliation_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

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
	defer db.Close()

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
	defer db.Close()
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
	defer db.Close()

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
	defer db.Close()

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
	defer db.Close()

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
	defer db.Close()

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
	defer db.Close()

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
	defer db.Close()

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
