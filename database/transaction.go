package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"

	"go.opentelemetry.io/otel"

	"github.com/jerry-enebeli/blnk/model"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func (d Datasource) RecordTransaction(cxt context.Context, txn *model.Transaction) (*model.Transaction, error) {
	cxt, span := otel.Tracer("Queue transaction").Start(cxt, "Saving transaction to db")
	defer span.End()
	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		return txn, err
	}

	_, err = d.Conn.ExecContext(cxt,
		`
		INSERT INTO blnk.transactions(transaction_id,parent_transaction,source,reference,amount,precise_amount,precision,rate,currency,destination,description,status,created_at,meta_data,scheduled_for,hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15,$16)
	`,
		txn.TransactionID,
		txn.ParentTransaction,
		txn.Source,
		txn.Reference,
		txn.Amount,
		txn.PreciseAmount,
		txn.Precision,
		txn.Rate,
		txn.Currency,
		txn.Destination,
		txn.Description,
		txn.Status,
		txn.CreatedAt,
		metaDataJSON,
		txn.ScheduledFor,
		txn.Hash,
	)

	if err != nil {
		return txn, err
	}

	return txn, nil
}

func (d Datasource) GetTransaction(id string) (*model.Transaction, error) {
	row := d.Conn.QueryRow(`
			SELECT transaction_id, source, reference, amount, precise_amount, precision, currency,destination, description, status,created_at, meta_data
						FROM blnk.transactions
					WHERE transaction_id = $1
				`, id)

	txn := &model.Transaction{}

	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &txn.PreciseAmount, &txn.Precision, &txn.Currency, &txn.Destination, &txn.Description,
		&txn.Status,
		&txn.CreatedAt, &metaDataJSON)
	if err != nil {
		return &model.Transaction{}, err
	}

	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		return &model.Transaction{}, err
	}

	return txn, nil
}

func (d Datasource) IsParentTransactionVoid(parentID string) (bool, error) {
	row := d.Conn.QueryRow(`
		SELECT EXISTS (
			SELECT 1
			FROM blnk.transactions
			WHERE parent_transaction = $1
			AND status = 'VOID'
		)
	`, parentID)

	var exists bool
	err := row.Scan(&exists)
	if err != nil {
		return false, err
	}

	return exists, nil
}

func (d Datasource) TransactionExistsByRef(ctx context.Context, reference string) (bool, error) {
	cxt, span := otel.Tracer("Queue transaction").Start(ctx, "Getting transaction from db by reference")
	defer span.End()
	var exists bool
	err := d.Conn.QueryRowContext(cxt, `
        SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)
    `, reference).Scan(&exists)
	return exists, err
}

func (d Datasource) GetTransactionByRef(ctx context.Context, reference string) (model.Transaction, error) {
	// retrieve from database
	row := d.Conn.QueryRowContext(ctx, `
		SELECT transaction_id, source, reference, amount, precise_amount, currency,destination, description, status,created_at, meta_data
		FROM blnk.transactions
		WHERE reference = $1
	`, reference)

	txn := &model.Transaction{}

	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &txn.PreciseAmount, &txn.Currency, &txn.Destination, &txn.Description,
		&txn.Status,
		&txn.CreatedAt, &metaDataJSON)
	if err != nil {
		return model.Transaction{}, err
	}

	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		return model.Transaction{}, err
	}

	return *txn, nil
}

func (d Datasource) UpdateTransactionStatus(id string, status string) error {
	// retrieve from database
	_, err := d.Conn.Exec(`
		UPDATE blnk.transactions
		SET status = $2
		WHERE transaction_id = $1
	`, id, status)

	return err
}

func (d Datasource) GetAllTransactions() ([]model.Transaction, error) {
	rows, err := d.Conn.Query(`
		SELECT transaction_id, source, reference, amount, currency,destination, description, status, hash, created_at, meta_data
		FROM blnk.transactions
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		// convert metadata from JSON to map
		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			return nil, err
		}

		transactions = append(transactions, transaction)
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

	row := d.Conn.QueryRow(query, parentID)
	var totalAmount int64

	err := row.Scan(&totalAmount)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}

	return totalAmount, nil
}

func (d Datasource) GetTransactionsPaginated(ctx context.Context, _ string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("Queue transaction").Start(ctx, "Fetching transactions by parent ID with pagination")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
        SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
    `, batchSize, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var transactions []*model.Transaction
	rowCount := 0

	for rows.Next() {
		rowCount++
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
			return nil, err
		}

		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			return nil, err
		}

		transactions = append(transactions, &transaction)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return transactions, nil
}

func (d Datasource) GroupTransactions(ctx context.Context, groupingCriteria map[string]interface{}, batchSize int, offset int64) (map[string][]model.Transaction, error) {
	ctx, span := otel.Tracer("Group transactions").Start(ctx, "Grouping transactions based on criteria")
	defer span.End()

	// Build the SQL query based on the grouping criteria
	query := `
        SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
        FROM blnk.transactions
        WHERE 1=1
    `
	var args []interface{}
	var groupByColumns []string

	argIndex := 1
	for key, value := range groupingCriteria {
		query += fmt.Sprintf(" AND %s = $%d", key, argIndex)
		args = append(args, value)
		groupByColumns = append(groupByColumns, key)
		argIndex++
	}

	if len(groupByColumns) > 0 {
		query += " GROUP BY " + strings.Join(groupByColumns, ", ")
	}

	query += fmt.Sprintf(" ORDER BY created_at DESC LIMIT $%d OFFSET $%d", argIndex, argIndex+1)
	args = append(args, batchSize, offset)

	rows, err := d.Conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	groupedTransactions := make(map[string][]model.Transaction)
	rowCount := 0

	for rows.Next() {
		rowCount++
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
			return nil, err
		}

		// Convert metadata from JSON to map
		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			return nil, err
		}

		// Create a group key based on the grouping criteria
		var groupKeyParts []string
		for _, col := range groupByColumns {
			groupKeyParts = append(groupKeyParts, fmt.Sprintf("%v", reflect.ValueOf(transaction).FieldByName(col)))
		}
		groupKey := strings.Join(groupKeyParts, "-")

		groupedTransactions[groupKey] = append(groupedTransactions[groupKey], transaction)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return groupedTransactions, nil
}

func (d Datasource) GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("Queue transaction").Start(ctx, "Fetching transactions by parent ID with pagination")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE transaction_id = $1 OR parent_transaction = $1 AND status = 'INFLIGHT'
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, parentTransactionID, batchSize, offset)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		// Convert metadata from JSON to map
		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			return nil, err
		}

		transactions = append(transactions, &transaction)
	}

	return transactions, nil
}

func (d Datasource) GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	ctx, span := otel.Tracer("Queue transaction").Start(ctx, "Fetching transactions by parent ID with pagination")
	defer span.End()

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT transaction_id, parent_transaction, source, reference, amount, precise_amount, precision, rate, currency, destination, description, status, created_at, meta_data, scheduled_for, hash
		FROM blnk.transactions
		WHERE transaction_id = $1 AND status = 'APPLIED' OR parent_transaction = $1 AND (status = 'VOID' OR status = 'APPLIED')
		ORDER BY created_at DESC
		LIMIT $2 OFFSET $3
	`, parentTransactionID, batchSize, offset)
	if err != nil {
		return nil, err
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
			return nil, err
		}

		// Convert metadata from JSON to map
		err = json.Unmarshal(metaDataJSON, &transaction.MetaData)
		if err != nil {
			return nil, err
		}

		transactions = append(transactions, &transaction)
	}
	return transactions, nil
}
