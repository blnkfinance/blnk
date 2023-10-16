package datasources

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jerry-enebeli/blnk"
)

// CreateBalance inserts a new Balance into the database
func (d datasource) CreateBalance(balance blnk.Balance) (blnk.Balance, error) {
	// convert metadata to JSONB
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return balance, err
	}

	balance.BalanceID = GenerateUUIDWithSuffix("bln")
	balance.CreatedAt = time.Now()

	// insert into database
	_, err = d.conn.Exec(`
		INSERT INTO balances (balance_id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, meta_data)
		VALUES ($8,$1, $2, $3, $4, $5, $6, $7)
	`, balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, metaDataJSON, balance.BalanceID)

	return balance, err
}

// GetBalanceByID retrieves a balance from the database by ID
func (d datasource) GetBalanceByID(id string) (*blnk.Balance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel() // Ensure the context is canceled when the function exits

	// start a transaction
	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	// select balance from database by ID
	row := tx.QueryRow(`
		SELECT balance_id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, meta_data
		FROM balances
		WHERE balance_id = $1 FOR UPDATE SKIP LOCKED
	`, id)

	// parse metadata from JSON
	balance := &blnk.Balance{}
	var metaDataJSON []byte
	err = row.Scan(
		&balance.BalanceID,
		&balance.Balance,
		&balance.CreditBalance,
		&balance.DebitBalance,
		&balance.Currency,
		&balance.CurrencyMultiplier,
		&balance.LedgerID,
		&balance.CreatedAt,
		&metaDataJSON,
	)
	if err != nil {
		// if error occurs, roll back the transaction
		_ = tx.Rollback()
		return nil, err
	}

	// convert metadata from JSON to map
	err = json.Unmarshal(metaDataJSON, &balance.MetaData)
	if err != nil {
		// if error occurs, roll back the transaction
		_ = tx.Rollback()
		return nil, err
	}

	// commit the transaction
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return balance, nil
}

// GetAllBalances retrieves all balances from the database
func (d datasource) GetAllBalances() ([]blnk.Balance, error) {
	// select all balances from database
	rows, err := d.conn.Query(`
		SELECT id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, modification_ref, meta_data
		FROM balances
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// create slice to store balances
	var balances []blnk.Balance

	// iterate through result set and parse metadata from JSON
	for rows.Next() {
		balance := blnk.Balance{}
		var metaDataJSON []byte
		err = rows.Scan(
			&balance.BalanceID,
			&balance.Balance,
			&balance.CreditBalance,
			&balance.DebitBalance,
			&balance.Currency,
			&balance.CurrencyMultiplier,
			&balance.LedgerID,
			&balance.CreatedAt,
			&metaDataJSON,
		)
		if err != nil {
			return nil, err
		}

		// convert metadata from JSON to map
		err = json.Unmarshal(metaDataJSON, &balance.MetaData)
		if err != nil {
			return nil, err
		}

		balances = append(balances, balance)
	}

	return balances, nil
}

// UpdateBalance updates a balance in the database
func (d datasource) UpdateBalance(balance *blnk.Balance) error {
	// convert metadata to JSONB
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return err
	}

	// update balance in database
	_, err = d.conn.Exec(`
		UPDATE balances
		SET balance = $2, credit_balance = $3, debit_balance = $4, currency = $5, currency_multiplier = $6, ledger_id = $7, created_at = $8, meta_data = $9
		WHERE balance_id = $1
	`, balance.BalanceID, balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, metaDataJSON)

	return err
}
