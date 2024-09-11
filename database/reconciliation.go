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

// RecordReconciliation saves a reconciliation record to the database.
// Parameters:
// - ctx: Context for managing request and tracing.
// - rec: The reconciliation data to be stored.
// Returns:
// - An error if the reconciliation record fails to save, otherwise nil.
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

// GetReconciliation fetches a reconciliation record from the database based on its ID.
// Parameters:
// - ctx: Context for managing request and tracing.
// - id: The reconciliation ID to search for.
// Returns:
// - A pointer to the reconciliation record if found, or an error if not found or if a failure occurs.
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

// UpdateReconciliationStatus updates the status, matched transactions, unmatched transactions,
// and completed_at timestamp of a reconciliation in the database.
// Parameters:
// - ctx: Context for managing request and tracing.
// - id: The reconciliation ID to update.
// - status: The new status of the reconciliation.
// - matchedCount: The number of matched transactions.
// - unmatchedCount: The number of unmatched transactions.
// Returns:
// - An error if the update fails or the reconciliation is not found.
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

// GetReconciliationsByUploadID retrieves all reconciliations associated with a specific upload ID, ordered by the start date in descending order.
// Parameters:
// - ctx: Context for managing request and tracing.
// - uploadID: The upload ID to filter the reconciliations.
// Returns:
// - A slice of Reconciliation pointers and an error if the query fails.
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

// RecordMatches batches the saving of match records associated with a specific reconciliation ID.
// It uses a database transaction to ensure atomicity and consistency of the batch insert operation.
// Parameters:
// - ctx: Context for managing request and tracing.
// - reconciliationID: The ID of the reconciliation the matches are associated with.
// - matches: A slice of Match objects to be recorded.
// Returns:
// - An error if the operation fails, wrapped in an APIError for consistency.
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

// RecordUnmatched batches the saving of unmatched external transactions related to a specific reconciliation.
// It uses a database transaction to ensure atomicity and consistency of the batch insert operation.
// Parameters:
// - ctx: Context for managing request and tracing.
// - reconciliationID: The ID of the reconciliation the unmatched transactions are associated with.
// - results: A slice of external transaction IDs (as strings) representing unmatched transactions.
// Returns:
// - An error if the operation fails, wrapped in an APIError for consistency.
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

// recordMatchInTransaction inserts a match into the database within a transaction context.
// It is used to save matches during a reconciliation process, ensuring atomicity in batch operations.
// Parameters:
// - ctx: Context for managing request and tracing.
// - tx: The current database transaction in which the match is recorded.
// - match: A pointer to the Match struct containing details of the external and internal transactions, and their match status.
// Returns:
// - An error if the operation fails, wrapped in an APIError for consistency.
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

// RecordMatch inserts a single match into the database.
// It is used to record the match between external and internal transactions during reconciliation.
// Parameters:
// - ctx: Context for managing request and tracing.
// - match: A pointer to the Match struct containing details of the external and internal transactions, and their match status.
// Returns:
// - An error if the operation fails, wrapped in an APIError for consistency.
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

// GetMatchesByReconciliationID retrieves all matches associated with a given reconciliation ID.
// It joins the matches table with external transactions to filter matches by reconciliation ID.
// Parameters:
// - ctx: Context for managing request and tracing.
// - reconciliationID: The ID of the reconciliation to filter matches.
// Returns:
// - A slice of Match structs representing the matched transactions.
// - An error if the operation fails, wrapped in an APIError for consistency.
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

// RecordExternalTransaction inserts a new external transaction into the database.
// It associates the transaction with an upload ID and records its details.
// Parameters:
// - ctx: Context for managing request and tracing.
// - tx: Pointer to the ExternalTransaction struct containing transaction data.
// - uploadID: The ID of the upload batch this transaction belongs to.
// Returns:
// - An error if the operation fails, wrapped in an APIError for consistency.
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

// GetExternalTransactionsByReconciliationID fetches all external transactions associated with a given reconciliation ID.
// Parameters:
// - ctx: Context for managing request and tracing.
// - reconciliationID: The ID of the reconciliation to fetch external transactions for.
// Returns:
// - A slice of ExternalTransaction pointers or an error wrapped in an APIError if the operation fails.
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

// RecordMatchingRule saves a new matching rule to the database.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - rule: A pointer to the MatchingRule object containing rule details.
// Returns:
// - An error wrapped in an APIError if the operation fails.
func (d Datasource) RecordMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Saving matching rule to db")
	defer span.End()

	// Marshal the matching rule criteria into JSON format
	criteriaJSON, err := json.Marshal(rule.Criteria)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal matching rule criteria", err)
	}

	// Execute the SQL insert query to record the rule in the database
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

// GetMatchingRules retrieves all matching rules from the database.
// Parameters:
// - ctx: Context for managing the request and tracing.
// Returns:
// - A slice of MatchingRule pointers or an error wrapped in an APIError if the operation fails.
func (d Datasource) GetMatchingRules(ctx context.Context) ([]*model.MatchingRule, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Fetching matching rules")
	defer span.End()

	// Execute the query to fetch all matching rules
	rows, err := d.Conn.QueryContext(ctx, `
		SELECT id, rule_id, created_at, updated_at, name, description, criteria
		FROM blnk.matching_rules
	`)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve matching rules", err)
	}
	defer rows.Close()

	var rules []*model.MatchingRule

	// Iterate over the rows to scan each rule into a MatchingRule object
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

		// Unmarshal the criteria JSON into the MatchingRule's Criteria field
		err = json.Unmarshal(criteriaJSON, &rule.Criteria)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal matching rule criteria", err)
		}

		// Append the rule to the rules slice
		rules = append(rules, rule)
	}

	// Check for any errors that occurred during the iteration
	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over matching rules", err)
	}

	return rules, nil
}

// UpdateMatchingRule updates a specific matching rule in the database.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - rule: The matching rule to be updated, including the RuleID, name, description, and criteria.
// Returns:
// - An error wrapped in an APIError if the operation fails, or nil if the update is successful.
func (d Datasource) UpdateMatchingRule(ctx context.Context, rule *model.MatchingRule) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Updating matching rule")
	defer span.End()

	// Marshal the Criteria field into JSON
	criteriaJSON, err := json.Marshal(rule.Criteria)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal matching rule criteria", err)
	}

	// Execute the SQL update statement
	result, err := d.Conn.ExecContext(ctx, `
		UPDATE blnk.matching_rules
		SET name = $2, description = $3, criteria = $4
		WHERE rule_id = $1
	`, rule.RuleID, rule.Name, rule.Description, criteriaJSON)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update matching rule", err)
	}

	// Check how many rows were affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	// If no rows were affected, return a NotFound error
	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Matching rule with ID '%s' not found", rule.RuleID), nil)
	}

	return nil
}

// DeleteMatchingRule deletes a specific matching rule from the database.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - id: The ID of the matching rule to be deleted.
// Returns:
// - An error wrapped in an APIError if the operation fails, or nil if the deletion is successful.
func (d Datasource) DeleteMatchingRule(ctx context.Context, id string) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Deleting matching rule")
	defer span.End()

	// Execute the SQL delete statement
	result, err := d.Conn.ExecContext(ctx, `
		DELETE FROM blnk.matching_rules
		WHERE rule_id = $1
	`, id)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to delete matching rule", err)
	}

	// Check how many rows were affected
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	// If no rows were affected, return a NotFound error
	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Matching rule with ID '%s' not found", id), nil)
	}

	return nil
}

// GetMatchingRule retrieves a specific matching rule from the database by its rule ID.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - id: The ID of the matching rule to be retrieved.
// Returns:
// - A pointer to the MatchingRule object if found, or an APIError if the operation fails.
func (d Datasource) GetMatchingRule(ctx context.Context, id string) (*model.MatchingRule, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Fetching matching rule")
	defer span.End()

	var rule model.MatchingRule
	var criteriaJSON []byte

	// Execute SQL query to retrieve the matching rule by rule_id
	err := d.Conn.QueryRowContext(ctx, `
		SELECT id, rule_id, created_at, updated_at, name, description, criteria
		FROM blnk.matching_rules
		WHERE rule_id = $1
	`, id).Scan(
		&rule.ID, &rule.RuleID, &rule.CreatedAt, &rule.UpdatedAt,
		&rule.Name, &rule.Description, &criteriaJSON,
	)

	if err != nil {
		// Return NotFound error if no rows are returned
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Matching rule with ID '%s' not found", id), err)
		}
		// Return InternalServer error for any other failure
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve matching rule", err)
	}

	// Unmarshal JSON criteria into the MatchingRule object
	err = json.Unmarshal(criteriaJSON, &rule.Criteria)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal matching rule criteria", err)
	}

	return &rule, nil
}

// GetExternalTransactionsPaginated retrieves external transactions based on the provided upload ID
// with pagination. It first checks the cache, and if the data is not available, it fetches from the
// database and caches the result for 5 minutes.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - uploadID: The ID of the upload to filter external transactions.
// - batchSize: The number of transactions to retrieve per batch.
// - offset: The starting point to retrieve transactions from.
// Returns:
// - A slice of ExternalTransaction objects or an APIError if the operation fails.
func (d Datasource) GetExternalTransactionsPaginated(ctx context.Context, uploadID string, batchSize int, offset int64) ([]*model.ExternalTransaction, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Fetching external transactions with pagination")
	defer span.End()

	// Create a cache key based on the pagination parameters
	cacheKey := fmt.Sprintf("transactions:external:paginated:%d:%d", batchSize, offset)
	var transactions []*model.ExternalTransaction

	// Check if the data exists in the cache
	err := d.Cache.Get(ctx, cacheKey, &transactions)
	if err == nil && len(transactions) > 0 {
		return transactions, nil
	}

	// Query the database for external transactions if not found in cache
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

	// Iterate through the rows and scan data into the transaction slice
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

	// Handle any error encountered during row iteration
	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over external transactions", err)
	}

	// Cache the fetched data for future queries
	if len(transactions) > 0 {
		err = d.Cache.Set(ctx, cacheKey, transactions, 5*time.Minute) // Cache for 5 minutes
		if err != nil {
			// Log the error, but don't return it as the main operation succeeded
			log.Printf("Failed to cache transactions: %v", err)
		}
	}
	return transactions, nil
}

// SaveReconciliationProgress saves the progress of a reconciliation process to the database. If a record with the given reconciliation ID
// already exists, it updates the progress information. The function ensures that the reconciliation progress is stored and updated properly.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - reconciliationID: The ID of the reconciliation to track progress.
// - progress: A ReconciliationProgress object containing the number of processed transactions and the last processed external transaction ID.
// Returns:
// - An error if the operation fails, wrapped in an APIError if needed.
func (d Datasource) SaveReconciliationProgress(ctx context.Context, reconciliationID string, progress model.ReconciliationProgress) error {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Saving reconciliation progress to db")
	defer span.End()

	// Execute the query, with conflict handling to update if the record already exists
	_, err := d.Conn.ExecContext(ctx, `
		INSERT INTO blnk.reconciliation_progress (reconciliation_id, processed_count, last_processed_external_txn_id)
		VALUES ($1, $2, $3)
		ON CONFLICT (reconciliation_id) DO UPDATE
		SET processed_count = $2, last_processed_external_txn_id = $3
	`, reconciliationID, progress.ProcessedCount, progress.LastProcessedExternalTxnID)

	// Return an API error if the query fails
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to save reconciliation progress", err)
	}

	return nil
}

// LoadReconciliationProgress retrieves the progress of a reconciliation process from the database. If the reconciliation progress
// is not found, it returns an empty ReconciliationProgress object.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - reconciliationID: The ID of the reconciliation whose progress is being retrieved.
// Returns:
// - A ReconciliationProgress object containing the current progress, or an error wrapped in an APIError if any issues occur.
func (d Datasource) LoadReconciliationProgress(ctx context.Context, reconciliationID string) (model.ReconciliationProgress, error) {
	ctx, span := otel.Tracer("reconciliation.database").Start(ctx, "Loading reconciliation progress from db")
	defer span.End()

	var progress model.ReconciliationProgress
	err := d.Conn.QueryRowContext(ctx, `
		SELECT processed_count, last_processed_external_txn_id
		FROM blnk.reconciliation_progress
		WHERE reconciliation_id = $1
	`, reconciliationID).Scan(&progress.ProcessedCount, &progress.LastProcessedExternalTxnID)

	// Handle potential errors
	if err != nil {
		if err == sql.ErrNoRows {
			return model.ReconciliationProgress{}, nil // Return empty progress if not found
		}
		return model.ReconciliationProgress{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to load reconciliation progress", err)
	}

	return progress, nil
}

// FetchAndGroupExternalTransactions retrieves external transactions from the database based on a specific grouping criterion and paginates the results.
// The function first checks if the results are available in cache, and if not, fetches the data from the database, groups it by the specified criterion, and stores the result in cache.
// Parameters:
// - ctx: Context for managing the request and tracing.
// - uploadID: The ID of the upload to filter external transactions.
// - groupCriteria: The field by which to group the transactions (e.g., "amount", "currency").
// - batchSize: The number of transactions to retrieve.
// - offset: The pagination offset.
// Returns:
// - A map of grouped transactions where the key is the group criterion value and the value is a slice of transactions, or an error wrapped in an APIError if any issues occur.
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
