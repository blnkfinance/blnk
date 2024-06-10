package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

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
