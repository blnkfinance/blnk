package datasources

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jerry-enebeli/blnk" // Replace with the actual path to your blnk package
)

// CreateAccount inserts a new Account into the database
func (d datasource) CreateAccount(account blnk.Account) (blnk.Account, error) {
	metaDataJSON, err := json.Marshal(account.MetaData)
	if err != nil {
		return account, err
	}

	account.AccountID = GenerateUUIDWithSuffix("acc")
	account.CreatedAt = time.Now()

	_, err = d.conn.Exec(`
		INSERT INTO accounts (account_id, name, number, bank_name, ledger_id, identity_id, balance_id, created_at, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6,$7,$8,$9)
	`, account.AccountID, account.Name, account.Number, account.BankName, account.LedgerID, account.IdentityID, account.BalanceID, account.CreatedAt, metaDataJSON)

	return account, err
}

// GetAccountByID retrieves an account based on its ID
func (d datasource) GetAccountByID(id string) (*blnk.Account, error) {
	row := d.conn.QueryRow(`
		SELECT account_id, name, number, bank_name, created_at, meta_data 
		FROM accounts WHERE account_id = $1
	`, id)

	account := &blnk.Account{}
	var metaDataJSON []byte

	err := row.Scan(&account.AccountID, &account.Name, &account.Number, &account.BankName, &account.CreatedAt, &metaDataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("account with ID '%s' not found", id)
		}
		return nil, err
	}

	err = json.Unmarshal(metaDataJSON, &account.MetaData)
	if err != nil {
		return nil, err
	}

	return account, nil
}

// GetAllAccounts retrieves all accounts
func (d datasource) GetAllAccounts() ([]blnk.Account, error) {
	rows, err := d.conn.Query(`
    SELECT account_id, name, number, bank_name, created_at, meta_data 
    FROM accounts
	ORDER BY created_at DESC

`)

	if err != nil {
		return nil, err
	}
	var accounts []blnk.Account
	for rows.Next() {
		account := blnk.Account{}
		var metaDataJSON []byte
		err := rows.Scan(&account.AccountID, &account.Name, &account.Number, &account.BankName, &account.CreatedAt, &metaDataJSON)
		if err != nil {
			if err == sql.ErrNoRows {
				return nil, err
			}
			return nil, err
		}
		err = json.Unmarshal(metaDataJSON, &account.MetaData)
		if err != nil {
			return nil, err
		}
		accounts = append(accounts, account)
	}

	return accounts, nil
}

// GetAccountByNumber retrieves an account based on its number
func (d datasource) GetAccountByNumber(number string) (*blnk.Account, error) {
	row := d.conn.QueryRow(`
		SELECT account_id, name, number, bank_name, created_at, meta_data 
		FROM accounts WHERE number = $1
	`, number)

	account := &blnk.Account{}
	var metaDataJSON []byte

	err := row.Scan(&account.AccountID, &account.Name, &account.Number, &account.BankName, &account.CreatedAt, &metaDataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("account with number '%s' not found", number)
		}
		return nil, err
	}

	err = json.Unmarshal(metaDataJSON, &account.MetaData)
	if err != nil {
		return nil, err
	}

	return account, nil
}

// UpdateAccount updates a specific account in the database
func (d datasource) UpdateAccount(account *blnk.Account) error {
	metaDataJSON, err := json.Marshal(account.MetaData)
	if err != nil {
		return err
	}

	_, err = d.conn.Exec(`
		UPDATE accounts
		SET name = $2, number = $3, bank_name = $4, meta_data = $5
		WHERE account_id = $1
	`, account.AccountID, account.Name, account.Number, account.BankName, metaDataJSON)

	return err
}

// DeleteAccount deletes a specific account from the database
func (d datasource) DeleteAccount(id string) error {
	_, err := d.conn.Exec(`
		DELETE FROM accounts WHERE account_id = $1
	`, id)
	return err
}
