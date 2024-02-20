package database

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/jerry-enebeli/blnk/model"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func (d Datasource) RecordTransaction(txn model.Transaction) (model.Transaction, error) {
	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		return txn, err
	}

	txn.TransactionID = GenerateUUIDWithSuffix("txn")
	txn.CreatedAt = time.Now()
	// insert into database
	_, err = d.Conn.Exec(
		`
		INSERT INTO public.transactions (
			transaction_id, tag, reference, amount, currency,payment_method, description, drcr, status, ledger_id, balance_id,
			credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
			balance_before, balance_after, created_at, meta_data,scheduled_for
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,$18,$19,$20)
	`,
		txn.TransactionID,
		txn.Tag,
		txn.Reference,
		txn.Amount,
		txn.Currency,
		txn.PaymentMethod,
		txn.Description,
		txn.DRCR,
		txn.Status,
		txn.LedgerID,
		txn.BalanceID,
		txn.CreditBalanceBefore,
		txn.DebitBalanceBefore,
		txn.CreditBalanceAfter,
		txn.DebitBalanceAfter,
		txn.BalanceBefore,
		txn.BalanceAfter,
		txn.CreatedAt,
		metaDataJSON,
		txn.ScheduledFor,
	)

	if err != nil {
		return txn, err
	}

	return txn, nil
}

func (d Datasource) GetTransaction(id string) (model.Transaction, error) {
	// retrieve from database
	row := d.Conn.QueryRow(`
		SELECT transaction_id, tag, reference, amount, currency,payment_method, description, drcr, status, ledger_id, balance_id,
			credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
			balance_before, balance_after, created_at, meta_data
		FROM transactions
		WHERE transaction_id = $1
	`, id)

	// create a transaction instance
	txn := &model.Transaction{}

	// scan database row into transaction instance
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Tag, &txn.Reference, &txn.Amount, &txn.Currency, &txn.PaymentMethod, &txn.Description, &txn.DRCR,
		&txn.Status, &txn.LedgerID, &txn.BalanceID, &txn.CreditBalanceBefore, &txn.DebitBalanceBefore,
		&txn.CreditBalanceAfter, &txn.DebitBalanceAfter, &txn.BalanceBefore, &txn.BalanceAfter,
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

func (d Datasource) GetTransactionByRef(reference string) (model.Transaction, error) {
	// retrieve from database
	row := d.Conn.QueryRow(`
		SELECT transaction_id, tag, reference, amount, currency,payment_method, description, drcr, status, ledger_id, balance_id,
			credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
			balance_before, balance_after, created_at, meta_data
		FROM transactions
		WHERE reference = $1
	`, reference)

	// create a transaction instance
	txn := &model.Transaction{}

	// scan database row into transaction instance
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Tag, &txn.Reference, &txn.Amount, &txn.Currency, &txn.PaymentMethod, &txn.Description, &txn.DRCR,
		&txn.Status, &txn.LedgerID, &txn.BalanceID, &txn.CreditBalanceBefore, &txn.DebitBalanceBefore,
		&txn.CreditBalanceAfter, &txn.DebitBalanceAfter, &txn.BalanceBefore, &txn.BalanceAfter,
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
		UPDATE transactions
		SET status = $2
		WHERE transaction_id = $1
	`, id, status)

	return err
}

// GroupTransactionsByCurrency groups transactions by currency
func (d Datasource) GroupTransactionsByCurrency() (map[string]struct {
	TotalAmount int64 `json:"total_amount"`
}, error) {
	// select transactions grouped by currency with total amount
	rows, err := d.Conn.Query(`
		SELECT currency, SUM(amount) AS total_amount
		FROM transactions
		GROUP BY currency
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// create a map to store transactions by currency
	transactionsByCurrency := make(map[string]struct {
		TotalAmount int64 `json:"total_amount"`
	})

	// iterate through result set and parse JSON into Transaction structs
	for rows.Next() {
		var currency string
		var totalAmount int64
		err = rows.Scan(&currency, &totalAmount)
		if err != nil {
			return nil, err
		}

		// add transactions and total amount to map
		transactionsByCurrency[currency] = struct {
			TotalAmount int64 `json:"total_amount"`
		}{
			TotalAmount: totalAmount,
		}
	}

	return transactionsByCurrency, nil
}

// GetAllTransactions retrieves all transactions from the database
func (d Datasource) GetAllTransactions() ([]model.Transaction, error) {
	// select all transactions from database
	rows, err := d.Conn.Query(`
		SELECT transaction_id, tag, reference, amount, currency,payment_method, description, drcr, status, ledger_id, balance_id, credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after, balance_before, balance_after, created_at, meta_data
		FROM transactions
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
			&transaction.Tag,
			&transaction.Reference,
			&transaction.Amount,
			&transaction.Currency,
			&transaction.PaymentMethod,
			&transaction.Currency,
			&transaction.DRCR,
			&transaction.Status,
			&transaction.LedgerID,
			&transaction.BalanceID,
			&transaction.CreditBalanceBefore,
			&transaction.DebitBalanceBefore,
			&transaction.CreditBalanceAfter,
			&transaction.DebitBalanceAfter,
			&transaction.BalanceBefore,
			&transaction.BalanceAfter,
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

func (d Datasource) GetNextQueuedTransaction() (*model.Transaction, error) {
	// Start a transaction
	tx, err := d.Conn.Begin()
	if err != nil {
		return nil, err
	}

	// Query to select the earliest transaction from the queue
	row := tx.QueryRow(`
        SELECT transaction_id, tag, reference, amount, currency,payment_method, description, drcr, status, ledger_id, balance_id,
               credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
               balance_before, balance_after, created_at, meta_data, scheduled_for
        FROM transactions
        WHERE status = 'QUEUED'
        ORDER BY created_at ASC
        FOR UPDATE SKIP LOCKED
        LIMIT 1
    `)

	txn := &model.Transaction{}
	var metaDataJSON []byte

	// Scan the database row into the transaction instance
	err = row.Scan(&txn.TransactionID, &txn.Tag, &txn.Reference, &txn.Amount, &txn.Currency, &txn.PaymentMethod, &txn.Description, &txn.DRCR,
		&txn.Status, &txn.LedgerID, &txn.BalanceID, &txn.CreditBalanceBefore, &txn.DebitBalanceBefore,
		&txn.CreditBalanceAfter, &txn.DebitBalanceAfter, &txn.BalanceBefore, &txn.BalanceAfter,
		&txn.CreatedAt, &metaDataJSON, &txn.ScheduledFor)

	if err != nil {
		err := tx.Rollback() // Roll back in case of any error
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, err
	}
	// Convert metadata from JSONB to map
	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		err := tx.Rollback()
		return nil, err
	}

	// Update the status of the transaction to "PROCESSING"
	_, err = tx.Exec(`UPDATE transactions SET status = 'PROCESSING' WHERE transaction_id = $1`, txn.TransactionID)
	if err != nil {
		err := tx.Rollback()
		return nil, err
	}

	// Commit the transaction to release the lock
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return txn, nil
}
