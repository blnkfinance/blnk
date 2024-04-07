package database

import (
	"context"
	"encoding/json"
	"time"

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

	txn.CreatedAt = time.Now()
	// insert into database
	_, err = d.Conn.ExecContext(cxt,
		`
		INSERT INTO blnk.transactions(transaction_id,source,reference,amount,currency,destination,description,status,created_at,meta_data,scheduled_for,hash) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
	`,
		txn.TransactionID,
		txn.Source,
		txn.Reference,
		txn.Amount,
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
	// retrieve from database
	row := d.Conn.QueryRow(`
			SELECT transaction_id, source, reference, amount, currency,destination, description, status,created_at, meta_data
						FROM blnk.transactions
					WHERE transaction_id = $1
				`, id)

	// create a transaction instance
	txn := &model.Transaction{}

	// scan database row into transaction instance
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &txn.Currency, &txn.Destination, &txn.Description,
		&txn.Status,
		&txn.CreatedAt, &metaDataJSON)
	if err != nil {
		return &model.Transaction{}, err
	}

	// convert metadata from JSONB to map
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
		SELECT transaction_id, source, reference, amount, currency,destination, description, status,created_at, meta_data
		FROM blnk.transactions
		WHERE reference = $1
	`, reference)

	// create a transaction instance
	txn := &model.Transaction{}

	// scan database row into transaction instance
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Source, &txn.Reference, &txn.Amount, &txn.Currency, &txn.Destination, &txn.Description,
		&txn.Status,
		&txn.CreatedAt, &metaDataJSON)
	if err != nil {
		return model.Transaction{}, err
	}

	// convert metadata from JSONB to map
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
	// select all transactions from database
	rows, err := d.Conn.Query(`
		SELECT transaction_id, source, reference, amount, currency,destination, description, status, hash, created_at, meta_data
		FROM blnk.transactions
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// create slice to store transactions
	var transactions []model.Transaction

	// iterate through result set and parse metadata from JSON
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
