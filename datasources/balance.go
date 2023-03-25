package datasources

import (
	"encoding/json"

	"github.com/jerry-enebeli/blnk"
)

// CreateBalance inserts a new Balance into the database
func (d datasource) CreateBalance(balance blnk.Balance) (blnk.Balance, error) {
	// convert metadata to JSONB
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return balance, err
	}
	// insert into database
	_, err = d.conn.Exec(`
		INSERT INTO balances (balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, modification_ref, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
	`, balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.ModificationRef, metaDataJSON)

	return balance, err
}

// GetBalanceByID retrieves a balance from the database by ID
func (d datasource) GetBalanceByID(id int64) (*blnk.Balance, error) {
	// select balance from database by ID
	row := d.conn.QueryRow(`
		SELECT id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created, modification_ref, meta_data
		FROM balances
		WHERE id = $1
	`, id)

	// parse metadata from JSON
	balance := &blnk.Balance{}
	var metaDataJSON []byte
	err := row.Scan(
		&balance.ID,
		&balance.Balance,
		&balance.CreditBalance,
		&balance.DebitBalance,
		&balance.Currency,
		&balance.CurrencyMultiplier,
		&balance.LedgerID,
		&balance.Created,
		&balance.ModificationRef,
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

	return balance, nil
}

// GetAllBalances retrieves all balances from the database
func (d datasource) GetAllBalances() ([]blnk.Balance, error) {
	// select all balances from database
	rows, err := d.conn.Query(`
		SELECT id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created, modification_ref, meta_data
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
			&balance.ID,
			&balance.Balance,
			&balance.CreditBalance,
			&balance.DebitBalance,
			&balance.Currency,
			&balance.CurrencyMultiplier,
			&balance.LedgerID,
			&balance.Created,
			&balance.ModificationRef,
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
		SET balance = $2, credit_balance = $3, debit_balance = $4, currency = $5, currency_multiplier = $6, ledger_id = $7, created = $8, modification_ref = $9, meta_data = $10
		WHERE id = $1
	`, balance.ID, balance.Balance, balance.CreditBalance, balance.DebitBalance, balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.Created, balance.ModificationRef, metaDataJSON)

	return err
}
