package datasources

import (
	"encoding/json"
	"time"

	"github.com/jerry-enebeli/blnk"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func (d datasource) RecordTransaction(txn blnk.Transaction) (blnk.Transaction, error) {
	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		return txn, err
	}

	txn.TransactionID = GenerateUUIDWithSuffix("txn")
	txn.CreatedAt = time.Now()
	// insert into database
	_, err = d.conn.Exec(
		`
		INSERT INTO public.transactions (
			transaction_id, tag, reference, amount, currency, drcr, status, ledger_id, balance_id,
			credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
			balance_before, balance_after, created_at, meta_data,scheduled_for
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17,$18)
	`,
		txn.TransactionID,
		txn.Tag,
		txn.Reference,
		txn.Amount,
		txn.Currency,
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

func (d datasource) GetTransaction(id string) (blnk.Transaction, error) {
	// retrieve from database
	row := d.conn.QueryRow(`
		SELECT transaction_id, tag, reference, amount, currency, drcr, status, ledger_id, balance_id,
			credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
			balance_before, balance_after, created_at, meta_data
		FROM transactions
		WHERE transaction_id = $1
	`, id)

	// create a transaction instance
	txn := &blnk.Transaction{}

	// scan database row into transaction instance
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Tag, &txn.Reference, &txn.Amount, &txn.Currency, &txn.DRCR,
		&txn.Status, &txn.LedgerID, &txn.BalanceID, &txn.CreditBalanceBefore, &txn.DebitBalanceBefore,
		&txn.CreditBalanceAfter, &txn.DebitBalanceAfter, &txn.BalanceBefore, &txn.BalanceAfter,
		&txn.CreatedAt, &metaDataJSON)

	if err != nil {
		return blnk.Transaction{}, err
	}

	// convert metadata from JSONB to map
	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		return blnk.Transaction{}, err
	}

	return *txn, nil
}

func (d datasource) GetTransactionByRef(reference string) (blnk.Transaction, error) {
	// retrieve from database
	row := d.conn.QueryRow(`
		SELECT transaction_id, tag, reference, amount, currency, drcr, status, ledger_id, balance_id,
			credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
			balance_before, balance_after, created_at, meta_data
		FROM transactions
		WHERE reference = $1
	`, reference)

	// create a transaction instance
	txn := &blnk.Transaction{}

	// scan database row into transaction instance
	var metaDataJSON []byte
	err := row.Scan(&txn.TransactionID, &txn.Tag, &txn.Reference, &txn.Amount, &txn.Currency, &txn.DRCR,
		&txn.Status, &txn.LedgerID, &txn.BalanceID, &txn.CreditBalanceBefore, &txn.DebitBalanceBefore,
		&txn.CreditBalanceAfter, &txn.DebitBalanceAfter, &txn.BalanceBefore, &txn.BalanceAfter,
		&txn.CreatedAt, &metaDataJSON)
	if err != nil {
		return blnk.Transaction{}, err
	}

	// convert metadata from JSONB to map
	err = json.Unmarshal(metaDataJSON, &txn.MetaData)
	if err != nil {
		return blnk.Transaction{}, err
	}

	return *txn, nil
}

func (d datasource) UpdateTransactionStatus(id string, status string) error {
	// retrieve from database
	_, err := d.conn.Exec(`
		UPDATE transactions
		SET status = $2
		WHERE transaction_id = $1
	`, id, status)

	return err
}

// GroupTransactionsByCurrency groups transactions by currency
func (d datasource) GroupTransactionsByCurrency() (map[string]struct {
	TotalAmount int64 `json:"total_amount"`
}, error) {
	// select transactions grouped by currency with total amount
	rows, err := d.conn.Query(`
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
func (d datasource) GetAllTransactions() ([]blnk.Transaction, error) {
	// select all transactions from database
	rows, err := d.conn.Query(`
		SELECT transaction_id, tag, reference, amount, currency, drcr, status, ledger_id, balance_id, credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after, balance_before, balance_after, created_at, meta_data
		FROM transactions
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// create slice to store transactions
	var transactions []blnk.Transaction

	// iterate through result set and parse metadata from JSON
	for rows.Next() {
		transaction := blnk.Transaction{}
		var metaDataJSON []byte
		err = rows.Scan(
			&transaction.TransactionID,
			&transaction.Tag,
			&transaction.Reference,
			&transaction.Amount,
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

func (d datasource) GetScheduledTransactions() ([]blnk.Transaction, error) {
	// Get the current time in the database's timezone (assuming the database uses UTC)
	currentTime := time.Now()

	// Query transactions with scheduled_for equal to the current time
	rows, err := d.conn.Query(`
		SELECT transaction_id, tag, reference, amount, currency, drcr, status, ledger_id, balance_id,
			credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
			balance_before, balance_after, created_at, meta_data,scheduled_for
		FROM transactions
		WHERE scheduled_for <= $1 AND status = $2
	`, currentTime, "SCHEDULED")

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Create a slice to store scheduled transactions
	var scheduledTransactions []blnk.Transaction

	// Iterate through the result set
	for rows.Next() {
		// Create a transaction instance
		txn := &blnk.Transaction{}

		// Scan the database row into the transaction instance (similar to your GetTransaction code)
		var metaDataJSON []byte
		err := rows.Scan(&txn.TransactionID, &txn.Tag, &txn.Reference, &txn.Amount, &txn.Currency, &txn.DRCR,
			&txn.Status, &txn.LedgerID, &txn.BalanceID, &txn.CreditBalanceBefore, &txn.DebitBalanceBefore,
			&txn.CreditBalanceAfter, &txn.DebitBalanceAfter, &txn.BalanceBefore, &txn.BalanceAfter,
			&txn.CreatedAt, &metaDataJSON, &txn.ScheduledFor)

		if err != nil {
			return nil, err
		}

		// Convert metadata from JSONB to map
		err = json.Unmarshal(metaDataJSON, &txn.MetaData)
		if err != nil {
			return nil, err
		}

		// Append the scheduled transaction to the slice
		scheduledTransactions = append(scheduledTransactions, *txn)
	}

	return scheduledTransactions, nil
}
