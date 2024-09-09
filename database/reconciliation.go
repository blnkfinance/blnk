package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

func (d Datasource) RecordReconciliation(ctx context.Context, rec *model.Reconciliation) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Saving reconciliation to db")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.reconciliations(
			reconciliation_id, upload_id, status, matched_transactions, 
			unmatched_transactions, started_at, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		rec.ReconciliationID, rec.UploadID, rec.Status, rec.MatchedTransactions,
		rec.UnmatchedTransactions, rec.StartedAt, rec.CompletedAt,
	)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record reconciliation", err)
	}

	return nil
}

func (d Datasource) GetReconciliation(ctx context.Context, id string) (*model.Reconciliation, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Fetching reconciliation from db")
	defer span.End()

	rec := &model.Reconciliation{}
	err := d.Conn.QueryRowContext(ctx, `
		SELECT id, reconciliation_id, upload_id, status, matched_transactions, 
			unmatched_transactions, started_at, completed_at
		FROM blnk.reconciliations
		WHERE reconciliation_id = $1
	`, id).Scan(
		&rec.ID, &rec.ReconciliationID, &rec.UploadID, &rec.Status,
		&rec.MatchedTransactions, &rec.UnmatchedTransactions,
		&rec.StartedAt, &rec.CompletedAt,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Reconciliation with ID '%s' not found", id), err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve reconciliation", err)
	}

	return rec, nil
}

func (d Datasource) UpdateReconciliationStatus(ctx context.Context, id string, status string, matchedCount, unmatchedCount int) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Updating reconciliation status")
	defer span.End()

	completedAt := sql.NullTime{Time: time.Now(), Valid: status == "completed"}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.reconciliations
		SET status = $2, matched_transactions = $3, unmatched_transactions = $4, completed_at = $5
		WHERE reconciliation_id = $1
	`, id, status, matchedCount, unmatchedCount, completedAt)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update reconciliation status", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Reconciliation with ID '%s' not found", id), nil)
	}

	return nil
}

func (d Datasource) GetReconciliationsByUploadID(ctx context.Context, uploadID string) ([]*model.Reconciliation, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Fetching reconciliations by upload ID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, reconciliation_id, upload_id, status, matched_transactions, 
			unmatched_transactions, started_at, completed_at
		FROM blnk.reconciliations
		WHERE upload_id = $1
		ORDER BY started_at DESC
	`, uploadID)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve reconciliations", err)
	}
	defer rows.Close()

	var reconciliations []*model.Reconciliation

	for rows.Next() {
		rec := &model.Reconciliation{}
		err = rows.Scan(
			&rec.ID, &rec.ReconciliationID, &rec.UploadID, &rec.Status,
			&rec.MatchedTransactions, &rec.UnmatchedTransactions,
			&rec.StartedAt, &rec.CompletedAt,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan reconciliation data", err)
		}

		reconciliations = append(reconciliations, rec)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over reconciliations", err)
	}

	return reconciliations, nil
}
func (d Datasource) RecordMatches(ctx context.Context, reconciliationID string, matches []model.Match) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Batch saving matches to db")
	defer span.End()

	txn, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to start transaction", err)
	}
	defer func() {
		if err := txn.Rollback(); err != nil && err != sql.ErrTxDone {
			span.RecordError(fmt.Errorf("error rolling back transaction: %w", err))
		}
	}()

	for _, match := range matches {
		match.ReconciliationID = reconciliationID
		err := d.recordMatchInTransaction(ctx, txn, &match)
		if err != nil {
			return err // The error is already wrapped in an APIError by recordMatchInTransaction
		}
	}

	if err := txn.Commit(); err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	return nil
}

func (d Datasource) RecordUnmatched(ctx context.Context, reconciliationID string, results []string) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Batch saving unmatched to db")
	defer span.End()

	txn, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to start transaction", err)
	}
	defer func() {
		if err := txn.Rollback(); err != nil && err != sql.ErrTxDone {
			span.RecordError(fmt.Errorf("error rolling back transaction: %w", err))
		}
	}()

	for _, externalId := range results {
		_, err := txn.ExecContext(ctx,
			`INSERT INTO blnk.unmatched(
					external_transaction_id, reconciliation_id, date
				) VALUES ($1, $2, $3)`,
			externalId, reconciliationID, time.Now())
		if err != nil {
			return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record unmatched match", err)
		}

	}

	if err := txn.Commit(); err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	return nil
}

func (d Datasource) recordMatchInTransaction(ctx context.Context, tx *sql.Tx, match *model.Match) error {
	_, err := tx.ExecContext(ctx,
		`INSERT INTO blnk.matches(
			external_transaction_id, internal_transaction_id, reconciliation_id, amount, date
		) VALUES ($1, $2, $3, $4, $5)`,
		match.ExternalTransactionID, match.InternalTransactionID, match.ReconciliationID, match.Amount, match.Date,
	)

	if err != nil {
		// For other errors, return the original error
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record match", err)
	}

	return nil
}

func (d Datasource) RecordMatch(ctx context.Context, match *model.Match) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Saving match to db")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.matches(
			external_transaction_id, internal_transaction_id, reconciliation_id, amount, date
		) VALUES ($1, $2, $3, $4, $5)`,
		match.ExternalTransactionID, match.InternalTransactionID, match.ReconciliationID, match.Amount, match.Date,
	)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record match", err)
	}

	return nil
}

func (d Datasource) GetMatchesByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.Match, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Fetching matches by reconciliation ID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT m.external_transaction_id, m.internal_transaction_id, m.amount, m.date
		FROM blnk.matches m
		JOIN blnk.external_transactions et ON m.external_transaction_id = et.id
		WHERE et.reconciliation_id = $1
	`, reconciliationID)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve matches", err)
	}
	defer rows.Close()

	var matches []*model.Match

	for rows.Next() {
		match := &model.Match{}
		err = rows.Scan(
			&match.ExternalTransactionID, &match.InternalTransactionID,
			&match.Amount, &match.Date,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan match data", err)
		}

		matches = append(matches, match)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over matches", err)
	}

	return matches, nil
}

func (d Datasource) RecordExternalTransaction(ctx context.Context, tx *model.ExternalTransaction, uploadID string) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Saving external transaction to db")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.external_transactions(
			id, amount, reference, currency, description, date, source, upload_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		tx.ID, tx.Amount, tx.Reference, tx.Currency, tx.Description, tx.Date, tx.Source, uploadID,
	)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record external transaction", err)
	}

	return nil
}

func (d Datasource) GetExternalTransactionsByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.ExternalTransaction, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Fetching external transactions by reconciliation ID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, amount, reference, currency, description, date, source
		FROM blnk.external_transactions
		WHERE reconciliation_id = $1
	`, reconciliationID)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve external transactions", err)
	}
	defer rows.Close()

	var transactions []*model.ExternalTransaction

	for rows.Next() {
		tx := &model.ExternalTransaction{}
		err = rows.Scan(
			&tx.ID, &tx.Amount, &tx.Reference, &tx.Currency,
			&tx.Description, &tx.Date, &tx.Source,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan external transaction data", err)
		}

		transactions = append(transactions, tx)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over external transactions", err)
	}

	return transactions, nil
}

func (d Datasource) RecordMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Saving matching rule to db")
	defer span.End()

	criteriaJSON, err := json.Marshal(rule.Criteria)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal matching rule criteria", err)
	}

	_, err = d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.matching_rules(
			rule_id, created_at, updated_at, name, description, criteria
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		rule.RuleID, rule.CreatedAt, rule.UpdatedAt, rule.Name, rule.Description, criteriaJSON,
	)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record matching rule", err)
	}

	return nil
}

func (d Datasource) GetMatchingRules(ctx context.Context) ([]*model.MatchingRule, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Fetching matching rules")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, rule_id, created_at, updated_at, name, description, criteria
		FROM blnk.matching_rules
	`)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve matching rules", err)
	}
	defer rows.Close()

	var rules []*model.MatchingRule

	for rows.Next() {
		rule := &model.MatchingRule{}
		var criteriaJSON []byte
		err = rows.Scan(
			&rule.ID, &rule.RuleID, &rule.CreatedAt, &rule.UpdatedAt,
			&rule.Name, &rule.Description, &criteriaJSON,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan matching rule data", err)
		}

		err = json.Unmarshal(criteriaJSON, &rule.Criteria)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal matching rule criteria", err)
		}

		rules = append(rules, rule)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over matching rules", err)
	}

	return rules, nil
}

func (d Datasource) UpdateMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Updating matching rule")
	defer span.End()

	criteriaJSON, err := json.Marshal(rule.Criteria)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal matching rule criteria", err)
	}

	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.matching_rules
		SET name = $2, description = $3, criteria = $4
		WHERE rule_id = $1
	`, rule.RuleID, rule.Name, rule.Description, criteriaJSON)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update matching rule", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Matching rule with ID '%s' not found", rule.RuleID), nil)
	}

	return nil
}

func (d Datasource) DeleteMatchingRule(ctx context.Context, id string) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Deleting matching rule")
	defer span.End()

	result, err := d.Conn.ExecContext(ctx, `
		DELETE FROM blnk.matching_rules
		WHERE rule_id = $1
	`, id)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to delete matching rule", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Matching rule with ID '%s' not found", id), nil)
	}

	return nil
}

func (d Datasource) GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Fetching matching rule")
	defer span.End()

	var rule model.MatchingRule
	var criteriaJSON []byte

	err := d.Conn.QueryRowContext(ctx, `
		SELECT id, rule_id, created_at, updated_at, name, description, criteria
		FROM blnk.matching_rules
		WHERE rule_id = $1
	`, id).Scan(
		&rule.ID, &rule.RuleID, &rule.CreatedAt, &rule.UpdatedAt,
		&rule.Name, &rule.Description, &criteriaJSON,
	)

	if err != nil {
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Matching rule with ID '%s' not found", id), err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve matching rule", err)
	}

	err = json.Unmarshal(criteriaJSON, &rule.Criteria)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal matching rule criteria", err)
	}

	return &rule, nil
}

func (d Datasource) GetExternalTransactionsPaginated(ctx context.Context, uploadID string, batchSize int, offset int64) ([]*model.ExternalTransaction, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Fetching external transactions with pagination")
	defer span.End()

	// Create a cache key based on the pagination parameters
	cacheKey := fmt.Sprintf("transactions:external:paginated:%d:%d", batchSize, offset)
	var transactions []*model.ExternalTransaction
	err := d.Cache.Get(ctx, cacheKey, &transactions)
	if err == nil && len(transactions) > 0 {
		return transactions, nil
	}

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, amount, reference, currency, description, date, source
		FROM blnk.external_transactions
		WHERE upload_id = $1
		ORDER BY date DESC
		LIMIT $2 OFFSET $3
	`, uploadID, batchSize, offset)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve external transactions", err)
	}
	defer rows.Close()

	for rows.Next() {
		tx := &model.ExternalTransaction{}
		err = rows.Scan(
			&tx.ID, &tx.Amount, &tx.Reference, &tx.Currency,
			&tx.Description, &tx.Date, &tx.Source,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan external transaction data", err)
		}

		transactions = append(transactions, tx)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over external transactions", err)
	}

	// Cache the fetched data
	if len(transactions) > 0 {
		err = d.Cache.Set(ctx, cacheKey, transactions, 5*time.Minute) // Cache for 5 minutes
		if err != nil {
			// Log the error, but don't return it as the main operation succeeded
			log.Printf("Failed to cache transactions: %v", err)
		}
	}
	return transactions, nil
}

func (d Datasource) SaveReconciliationProgress(ctx context.Context, reconciliationID string, progress model.ReconciliationProgress) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Saving reconciliation progress to db")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx, `
		INSERT INTO blnk.reconciliation_progress (reconciliation_id, processed_count, last_processed_external_txn_id)
		VALUES ($1, $2,$3)
		ON CONFLICT (reconciliation_id) DO UPDATE
		SET processed_count = $2, last_processed_external_txn_id = $3
	`, reconciliationID, progress.ProcessedCount, progress.LastProcessedExternalTxnID)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to save reconciliation progress", err)
	}

	return nil
}

func (d Datasource) LoadReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Loading reconciliation progress from db")
	defer span.End()

	var progressJSON []byte
	err := d.Conn.QueryRowContext(ctx, `
		SELECT processed_count, last_processed_external_txn_id
		FROM blnk.reconciliation_progress
		WHERE reconciliation_id = $1
	`, reconciliationID).Scan(&progressJSON)

	if err != nil {
		if err == sql.ErrNoRows {
			return model.ReconciliationProgress{}, nil // Return empty progress if not found
		}
		return model.ReconciliationProgress{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to load reconciliation progress", err)
	}

	var progress model.ReconciliationProgress
	err = json.Unmarshal(progressJSON, &progress)
	if err != nil {
		return model.ReconciliationProgress{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal reconciliation progress", err)
	}

	return progress, nil
}

func (d Datasource) FetchAndGroupExternalTransactions(ctx context.Context, uploadID string, groupCriteria string, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "FetchAndGroupExternalTransactions")
	defer span.End()

	validColumns := map[string]bool{
		"id": true, "amount": true, "reference": true, "currency": true,
		"description": true, "date": true, "source": true,
	}
	if !validColumns[groupCriteria] {
		span.RecordError(fmt.Errorf("invalid group criteria: %s", groupCriteria))
		return nil, apierror.NewAPIError(apierror.ErrBadRequest, fmt.Sprintf("Invalid group criteria: %s", groupCriteria), nil)
	}

	// Create a cache key based on the grouping and pagination parameters
	cacheKey := fmt.Sprintf("external_transactions:grouped:%s:%s:%d:%d", uploadID, groupCriteria, batchSize, offset)

	var groupedTransactions map[string][]*model.Transaction
	err := d.Cache.Get(ctx, cacheKey, &groupedTransactions)
	if err == nil && len(groupedTransactions) > 0 {
		span.AddEvent("Grouped external transactions retrieved from cache", trace.WithAttributes(
			attribute.Int("group.count", len(groupedTransactions)),
		))
		return groupedTransactions, nil
	}

	// If not in cache or error occurred, fetch from database
	query := `
        SELECT $1::text AS group_key, id, amount, reference, currency, description, date, source
        FROM blnk.external_transactions
        WHERE upload_id = $2 AND $1::text IS NOT NULL AND $1::text != ''
        ORDER BY $1::text
        LIMIT $3 OFFSET $4
    `

	rows, err := d.Conn.QueryContext(ctx, query, groupCriteria, uploadID, batchSize, offset)
	if err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve grouped external transactions", err)
	}
	defer rows.Close()

	groupedTransactions = make(map[string][]*model.Transaction)

	for rows.Next() {
		var groupKey string
		tx := &model.ExternalTransaction{}
		err = rows.Scan(
			&groupKey,
			&tx.ID, &tx.Amount, &tx.Reference, &tx.Currency,
			&tx.Description, &tx.Date, &tx.Source,
		)
		if err != nil {
			span.RecordError(err)
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan external transaction data", err)
		}

		// Convert ExternalTransaction to Transaction
		transaction := tx.ToInternalTransaction()
		groupedTransactions[groupKey] = append(groupedTransactions[groupKey], transaction)
	}

	if err = rows.Err(); err != nil {
		span.RecordError(err)
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over external transactions", err)
	}

	// Cache the fetched data if not empty
	if len(groupedTransactions) > 0 {
		if err := d.Cache.Set(ctx, cacheKey, groupedTransactions, 5*time.Minute); err != nil {
			log.Printf("Failed to cache grouped external transactions: %v", err)
		}
	}

	span.AddEvent("Grouped external transactions retrieved", trace.WithAttributes(
		attribute.Int("group.count", len(groupedTransactions)),
	))
	return groupedTransactions, nil
}
