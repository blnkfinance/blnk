package datasources

import (
	"encoding/json"

	"github.com/jerry-enebeli/blnk"

	_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"
)

func (d datasource) RecordTransaction(txn blnk.Transaction) (blnk.Transaction, error) {
	metaDataJSON, err := json.Marshal(txn.MetaData)
	if err != nil {
		return txn, err
	}
	// insert into database
	_, err = d.conn.Exec(
		`
		INSERT INTO public.transactions (
			id, tag, reference, amount, currency, drcr, status, ledger_id, balance_id,
			credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
			balance_before, balance_after, created, meta_data
		) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)
	`,
		txn.ID,
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
		txn.Created,
		metaDataJSON,
	)

	if err != nil {
		return txn, err
	}

	return txn, nil
}

func (d datasource) GetTransaction(id string) (blnk.Transaction, error) {
	// retrieve from database
	row := d.conn.QueryRow(`
		SELECT id, tag, reference, amount, currency, drcr, status, ledger_id, balance_id,
			credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
			balance_before, balance_after, created, meta_data
		FROM transactions
		WHERE id = $1
	`, id)

	// create a transaction instance
	txn := &blnk.Transaction{}

	// scan database row into transaction instance
	var metaDataJSON []byte
	err := row.Scan(&txn.ID, &txn.Tag, &txn.Reference, &txn.Amount, &txn.Currency, &txn.DRCR,
		&txn.Status, &txn.LedgerID, &txn.BalanceID, &txn.CreditBalanceBefore, &txn.DebitBalanceBefore,
		&txn.CreditBalanceAfter, &txn.DebitBalanceAfter, &txn.BalanceBefore, &txn.BalanceAfter,
		&txn.Created, &metaDataJSON)

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
		SELECT id, tag, reference, amount, currency, drcr, status, ledger_id, balance_id,
			credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after,
			balance_before, balance_after, created, meta_data
		FROM transactions
		WHERE reference = $1
	`, reference)

	// create a transaction instance
	txn := &blnk.Transaction{}

	// scan database row into transaction instance
	var metaDataJSON []byte
	err := row.Scan(&txn.ID, &txn.Tag, &txn.Reference, &txn.Amount, &txn.Currency, &txn.DRCR,
		&txn.Status, &txn.LedgerID, &txn.BalanceID, &txn.CreditBalanceBefore, &txn.DebitBalanceBefore,
		&txn.CreditBalanceAfter, &txn.DebitBalanceAfter, &txn.BalanceBefore, &txn.BalanceAfter,
		&txn.Created, &metaDataJSON)
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
		WHERE id = $1
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
		SELECT id, tag, reference, amount, currency, drcr, status, ledger_id, balance_id, credit_balance_before, debit_balance_before, credit_balance_after, debit_balance_after, balance_before, balance_after, created, meta_data
		FROM transactions
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
			&transaction.ID,
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
			&transaction.Created,
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
