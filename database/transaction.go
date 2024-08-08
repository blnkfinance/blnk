package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"reflect"
	"strings"
	"time"

	"go.opentelemetry.io/otel"

	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func (d Datasource) RecordTransaction(ctx context.Context, txn *model.Transaction) (*model.Transaction, error) {
	ctx, span := otel.Tracer("Queue transaction").Start(ctx, "Saving transaction to db")
	defer span.End()

	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	_, err = d.Conn.ExecContext(ctx,
		`INSERT INTO blnk.transactions(transaction_id,parent_transaction,source,reference,amount,precise_amount,precision,rate,currency,destination,description,status,created_at,meta_data,scheduled_for,hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)`,
		txn.TransactionID, txn.ParentTransaction, txn.Source, txn.Reference, txn.Amount, txn.PreciseAmount, txn.Precision, txn.Rate, txn.Currency, txn.Destination, txn.Description, txn.Status, txn.CreatedAt, metaDataJSON, txn.ScheduledFor, txn.Hash,
	)

	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to record transaction", err)
	}

	return txn, nil
}

func (d Datasource) GetTransaction(id string) (*model.Transaction, error) {
	row := d.Conn.QueryRow(`
		SELECT transaction_id, source, reference, amount, precise_amount, precision, currency, destination, description, status, created_at, meta_data
		FROM blnk.transactions
		WHERE transaction_id = $1
	`, id)

	txn := &model.Transaction{}
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &txn.PreciseAmount, &txn.Precision, &txn.Currency, &txn.Destination, &txn.Description, &txn.Status, &txn.CreatedAt, &metaDataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Transaction with ID '%s' not found", id), err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transaction", err)
	}

	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
	}

	return txn, nil
}

func (d Datasource) IsParentTransactionVoid(parentID string) (bool, error) {
	var exists bool
	err := d.Conn.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM blnk.transactions
			WHERE parent_transaction = $1
			AND status = 'VOID'
		)
	`, parentID).Scan(&exists)

	if err != nil {
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to check if parent transaction is void", err)
	}

	return exists, nil
}

func (d Datasource) TransactionExistsByRef(ctx context.Context, reference string) (bool, error) {
	ctx, span := otel.Tracer("Queue transaction").Start(ctx, "Getting transaction from db by reference")
	defer span.End()

	var exists bool
	err := d.Conn.QueryRowContext(ctx, `
		SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
	`, reference).Scan(&exists)

	if err != nil {
		return false, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to check if transaction exists", err)
	}

	return exists, nil
}

func (d Datasource) GetTransactionByRef(ctx context.Context, reference string) (model.Transaction, error) {
	row := d.Conn.QueryRowContext(ctx, `
		SELECT transaction_id, source, reference, amount, precise_amount, currency, destination, description, status, created_at, meta_data
		FROM blnk.transactions
		WHERE reference = $1
	`, reference)

	txn := model.Transaction{}
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &txn.PreciseAmount, &txn.Currency, &txn.Destination, &txn.Description, &txn.Status, &txn.CreatedAt, &metaDataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return model.Transaction{}, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Transaction with reference '%s' not found", reference), err)
		}
		return model.Transaction{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transaction", err)
	}

	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		return model.Transaction{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
	}

	return txn, nil
}

func (d Datasource) UpdateTransactionStatus(id string, status string) error {
	result, err := d.Conn.Exec(`
		UPDATE blnk.transactions
		SET status = $2
		WHERE transaction_id = $1
	`, id, status)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update transaction status", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Transaction with ID '%s' not found", id), nil)
	}

	return nil
}

func (d Datasource) GetAllTransactions() ([]model.Transaction, error) {
	rows, err := d.Conn.Query(`
		SELECT transaction_id, source, reference, amount, currency, destination, description, status, hash, created_at, meta_data
		FROM blnk.transactions
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve transactions", err)
	}
	defer rows.Close()

	var transactions []model.Transaction

	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.Currency,
			&transaction.Destination,
			&transaction.Description,
			&transaction.Status,
			&transaction.Hash,
			&transaction.CreatedAt,
			&metaDataJSON,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan transaction data", err)
		}

		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		transactions = append(transactions, transaction)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	return transactions, nil
}

func (d Datasource) GetTotalCommittedTransactions(parentID string) (int64, error) {
	query := `
		SELECT SUM(precise_amount) AS total_amount
		FROM blnk.transactions
		WHERE parent_transaction = $1
		GROUP BY parent_transaction;
	`

	var totalAmount int64
	err := d.Conn.QueryRow(query, parentID).Scan(&totalAmount)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get total committed transactions", err)
	}

	return totalAmount, nil
}

func (d Datasource) GetTransactionsPaginated(ctx context.Context, _ string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("Queue transaction").Start(ctx, "Fetching transactions by parent ID with pagination")
	defer span.End()

	// Create a cache key based on the pagination parameters
	cacheKey := fmt.Sprintf("transactions:paginated:%d:%d", batchSize, offset)

	var transactions []*model.Transaction
	err := d.Cache.Get(ctx, cacheKey, &transactions)
	if err == nil && len(transactions) > 0 {
		return transactions, nil
	}

	// If not in cache or error occurred, fetch from database
	rows, err := d.Conn.QueryContext(ctx, `
        SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        ORDER BY created_at ASC
        LIMIT $1 OFFSET $2
    `, batchSize, offset)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve paginated transactions", err)
	}
	defer rows.Close()

	transactions = []*model.Transaction{}

	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.ParentTransaction,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.PreciseAmount,
			&transaction.Precision,
			&transaction.Rate,
			&transaction.Currency,
			&transaction.Destination,
			&transaction.Description,
			&transaction.Status,
			&transaction.CreatedAt,
			&metaDataJSON,
			&transaction.ScheduledFor,
			&transaction.Hash,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan transaction data", err)
		}

		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		transactions = append(transactions, &transaction)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
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

func (d Datasource) GroupTransactions(ctx context.Context, groupCriteria map[string]interface{}, batchSize int, offset int64) (map[string][]*model.Transaction, error) {
	query := `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE 1=1
	`
	var args []interface{}
	var groupByColumns []string
	var whereClauses []string

	argIndex := 1

	for field, value := range groupCriteria {
		groupByColumns = append(groupByColumns, field)

		if value != nil {
			whereClauses = append(whereClauses, fmt.Sprintf("%s = $%d", field, argIndex))
			args = append(args, value)
			argIndex++
		}
	}

	if len(whereClauses) > 0 {
		query += " AND " + strings.Join(whereClauses, " AND ")
	}

	allColumns := []string{
		"transaction_id", "parent_transaction", "source", "reference", "amount", "precise_amount",
		"precision", "rate", "currency", "destination", "description", "status", "created_at",
		"meta_data", "scheduled_for", "hash",
	}

	groupByColumns = append(groupByColumns, allColumns...)
	query += " GROUP BY " + strings.Join(groupByColumns, ", ")

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argIndex, argIndex+1)
	args = append(args, batchSize, offset)

	rows, err := d.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve grouped transactions", err)
	}
	defer rows.Close()

	groupedTransactions := make(map[string][]*model.Transaction)

	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.ParentTransaction,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.PreciseAmount,
			&transaction.Precision,
			&transaction.Rate,
			&transaction.Currency,
			&transaction.Destination,
			&transaction.Description,
			&transaction.Status,
			&transaction.CreatedAt,
			&metaDataJSON,
			&transaction.ScheduledFor,
			&transaction.Hash,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan transaction data", err)
		}

		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		var groupKeyParts []string
		for field := range groupCriteria {
			groupKeyParts = append(groupKeyParts, fmt.Sprintf("%v", reflect.ValueOf(transaction).FieldByName(field)))
		}
		groupKey := strings.Join(groupKeyParts, "-")

		groupedTransactions[groupKey] = append(groupedTransactions[groupKey], &transaction)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	return groupedTransactions, nil
}

func (d Datasource) GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("Queue transaction").Start(ctx, "Fetching inflight transactions by parent ID with pagination")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE transaction_id = $1 OR parent_transaction = $1 AND status = 'INFLIGHT'
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, parentTransactionID, batchSize, offset)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve inflight transactions", err)
	}
	defer rows.Close()

	var transactions []*model.Transaction

	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.ParentTransaction,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.PreciseAmount,
			&transaction.Precision,
			&transaction.Rate,
			&transaction.Currency,
			&transaction.Destination,
			&transaction.Description,
			&transaction.Status,
			&transaction.CreatedAt,
			&metaDataJSON,
			&transaction.ScheduledFor,
			&transaction.Hash,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan transaction data", err)
		}

		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		transactions = append(transactions, &transaction)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	return transactions, nil
}

func (d Datasource) GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("Queue transaction").Start(ctx, "Fetching refundable transactions by parent ID with pagination")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE transaction_id = $1 AND status = 'APPLIED' OR parent_transaction = $1 AND (status = 'VOID' OR status = 'APPLIED')
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, parentTransactionID, batchSize, offset)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve refundable transactions", err)
	}
	defer rows.Close()

	var transactions []*model.Transaction

	for rows.Next() {
		transaction := model.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.ParentTransaction,
			&transaction.Source,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.PreciseAmount,
			&transaction.Precision,
			&transaction.Rate,
			&transaction.Currency,
			&transaction.Destination,
			&transaction.Description,
			&transaction.Status,
			&transaction.CreatedAt,
			&metaDataJSON,
			&transaction.ScheduledFor,
			&transaction.Hash,
		)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan transaction data", err)
		}

		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		transactions = append(transactions, &transaction)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over transactions", err)
	}

	return transactions, nil
}
