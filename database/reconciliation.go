package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jerry-enebeli/blnk/model"
	"go.opentelemetry.io/otel"
)

// RecordReconciliation inserts a new reconciliation record into the database
func (d Datasource) RecordReconciliation(ctx context.Context, rec *model.Reconciliation) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Saving reconciliation to db")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.reconciliations(
			reconciliation_id, upload_id, status, matched_transactions, 
			unmatched_transactions, started_at, completed_at
		) VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		rec.ReconciliationID, rec.UploadID, rec.Status, rec.MatchedTransactions,
		rec.UnmatchedTransactions, rec.StartedAt, rec.CompletedAt,
	)

	return err
}

// GetReconciliation retrieves a reconciliation record by its ID
func (d Datasource) GetReconciliation(ctx context.Context, id string) (*model.Reconciliation, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching reconciliation from db")
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
		return nil, err
	}

	return rec, nil
}

// UpdateReconciliationStatus updates the status of a reconciliation record
func (d Datasource) UpdateReconciliationStatus(ctx context.Context, id string, status string, matchedCount, unmatchedCount int) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Updating reconciliation status")
	defer span.End()

	completedAt := sql.NullTime{Time: time.Now(), Valid: status == "completed"}

	_, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.reconciliations
		SET status = $2, matched_transactions = $3, unmatched_transactions = $4, completed_at = $5
		WHERE reconciliation_id = $1
	`, id, status, matchedCount, unmatchedCount, completedAt)

	return err
}

// GetReconciliationsByUploadID retrieves all reconciliations for a given upload
func (d Datasource) GetReconciliationsByUploadID(ctx context.Context, uploadID string) ([]*model.Reconciliation, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching reconciliations by upload ID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, reconciliation_id, upload_id, status, matched_transactions, 
			unmatched_transactions, started_at, completed_at
		FROM blnk.reconciliations
		WHERE upload_id = $1
		ORDER BY started_at DESC
	`, uploadID)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		reconciliations = append(reconciliations, rec)
	}

	return reconciliations, nil
}

// RecordMatch inserts a new match record into the database
func (d Datasource) RecordMatch(ctx context.Context, match *model.Match) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Saving match to db")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.matches(
			external_transaction_id, internal_transaction_id, reconciliation_id, amount, date
		) VALUES ($1, $2, $3, $4, $5)`,
		match.ExternalTransactionID, match.InternalTransactionID, match.ReconciliationID, match.Amount, match.Date,
	)

	return err
}

// GetMatchesByReconciliationID retrieves all matches for a given reconciliation
func (d Datasource) GetMatchesByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.Match, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching matches by reconciliation ID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT m.external_transaction_id, m.internal_transaction_id, m.amount, m.date
		FROM blnk.matches m
		JOIN blnk.external_transactions et ON m.external_transaction_id = et.id
		WHERE et.reconciliation_id = $1
	`, reconciliationID)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		matches = append(matches, match)
	}

	return matches, nil
}

// RecordExternalTransaction inserts a new external transaction record into the database
func (d Datasource) RecordExternalTransaction(ctx context.Context, tx *model.ExternalTransaction, uploadID string) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Saving external transaction to db")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.external_transactions(
			id, amount, reference, currency, description, date, source, upload_id
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		tx.ID, tx.Amount, tx.Reference, tx.Currency, tx.Description, tx.Date, tx.Source, uploadID,
	)

	return err
}

// GetExternalTransactionsByReconciliationID retrieves all external transactions for a given reconciliation
func (d Datasource) GetExternalTransactionsByReconciliationID(ctx context.Context, reconciliationID string) ([]*model.ExternalTransaction, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching external transactions by reconciliation ID")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, amount, reference, currency, description, date, source
		FROM blnk.external_transactions
		WHERE reconciliation_id = $1
	`, reconciliationID)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		transactions = append(transactions, tx)
	}

	return transactions, nil
}

// RecordMatchingRule inserts a new matching rule into the database
func (d Datasource) RecordMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Saving matching rule to db")
	defer span.End()

	criteriaJSON, err := json.Marshal(rule.Criteria)
	if err != nil {
		return err
	}

	_, err = d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.matching_rules(
			rule_id, created_at, updated_at, name, description, criteria
		) VALUES ($1, $2, $3, $4, $5, $6)`,
		rule.RuleID, rule.CreatedAt, rule.UpdatedAt, rule.Name, rule.Description, criteriaJSON,
	)

	return err
}

// GetMatchingRules retrieves all matching rules
func (d Datasource) GetMatchingRules(ctx context.Context) ([]*model.MatchingRule, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching matching rules")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, rule_id, created_at, updated_at, name, description, criteria
		FROM blnk.matching_rules
	`)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		err = json.Unmarshal(criteriaJSON, &rule.Criteria)
		if err != nil {
			return nil, err
		}

		rules = append(rules, rule)
	}

	return rules, nil
}
func (d Datasource) UpdateMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Updating matching rule")
	defer span.End()

	criteriaJSON, err := json.Marshal(rule.Criteria)
	if err != nil {
		return err
	}

	_, err = d.Conn.ExecContext(ctx, `
		UPDATE blnk.matching_rules
		SET name = $2, description = $3, criteria = $4
		WHERE rule_id = $1
	`, rule.RuleID, rule.Name, rule.Description, criteriaJSON)

	return err
}

func (d Datasource) DeleteMatchingRule(ctx context.Context, id string) error {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Deleting matching rule")
	defer span.End()

	_, err := d.Conn.ExecContext(ctx, `
		DELETE FROM blnk.matching_rules
		WHERE rule_id = $1
	`, id)

	return err
}

// GetMatchingRules retrieves all matching rules
func (d Datasource) GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error) {
	ctx, span := otel.Tracer("Reconciliation").Start(ctx, "Fetching matching rule")
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
			return nil, fmt.Errorf("no matching rule found with id: %s", id)
		}
		return nil, fmt.Errorf("error fetching matching rule: %w", err)
	}

	err = json.Unmarshal(criteriaJSON, &rule.Criteria)
	if err != nil {
		return nil, fmt.Errorf("error unmarshaling criteria JSON: %w", err)
	}

	return &rule, nil
}

func (d Datasource) GetExternalTransactionsPaginated(ctx context.Context, uploadID string, batchSize int, offset int64) ([]*model.ExternalTransaction, error) {
	ctx, span := otel.Tracer("Queue transaction").Start(ctx, "Fetching transactions by parent ID with pagination")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, amount, reference, currency, description, date, source
		FROM blnk.external_transactions
		WHERE upload_id = $1
		ORDER BY date DESC
		LIMIT $2 OFFSET $3
	`, uploadID, batchSize, offset)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		transactions = append(transactions, tx)
	}

	fmt.Println("external transactions", transactions)

	return transactions, nil
}
