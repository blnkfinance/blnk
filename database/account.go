package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/jerry-enebeli/blnk/model"
)

// CreateAccount inserts a new Account into the database
func (d Datasource) CreateAccount(account model.Account) (model.Account, error) {
	metaDataJSON, err := json.Marshal(account.MetaData)
	if err != nil {
		return account, err
	}

	account.AccountID = GenerateUUIDWithSuffix("acc")
	account.CreatedAt = time.Now()

	_, err = d.Conn.Exec(`
		INSERT INTO blnk.accounts (account_id, name, number, bank_name, currency, ledger_id, identity_id, balance_id, created_at, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6,$7,$8,$9,$10)
	`, account.AccountID, account.Name, account.Number, account.BankName, account.Currency, account.LedgerID, account.IdentityID, account.BalanceID, account.CreatedAt, metaDataJSON)

	return account, err
}

func (d Datasource) GetAccountByID(id string, include []string) (*model.Account, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	var queryBuilder strings.Builder
	query := prepareAccountQueries(queryBuilder, include)
	row := tx.QueryRow(query, id)

	account, err := scanAccountRow(row, tx, include)
	if err != nil {
		if err == sql.ErrNoRows {
			// Handle no rows error
			return nil, fmt.Errorf("account with ID '%s' not found", id)
		} else {
			// Handle other errors
			return nil, err
		}
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return account, nil
}

func prepareAccountQueries(queryBuilder strings.Builder, include []string) string {
	var selectFields []string
	selectFields = append(selectFields,
		"a.account_id", "a.name", "a.number", "a.bank_name",
		"a.currency", "a.ledger_id",
		"a.identity_id", "a.balance_id", "a.created_at", "a.meta_data")

	if contains(include, "balance") {
		selectFields = append(selectFields,
			"b.balance_id", "b.balance", "b.credit_balance", "b.debit_balance",
			"b.currency", "b.currency_multiplier", "b.ledger_id",
			"COALESCE(b.identity_id, '') as identity_id", "b.created_at", "b.meta_data")
	}

	if contains(include, "identity") {
		selectFields = append(selectFields,
			"i.identity_id", "i.first_name", "i.organization_name", "i.category", "i.last_name", "i.other_names",
			"i.gender", "i.dob", "i.email_address", "i.phone_number",
			"i.nationality", "i.street", "i.country", "i.state",
			"i.post_code", "i.city", "i.identity_type", "i.created_at", "i.meta_data")
	}

	if contains(include, "ledger") {
		selectFields = append(selectFields,
			"l.ledger_id", "l.name", "l.created_at")
	}

	// Construct the query
	queryBuilder.WriteString("SELECT ")
	queryBuilder.WriteString(strings.Join(selectFields, ", "))
	queryBuilder.WriteString(`
        FROM (
            SELECT * FROM blnk.accounts WHERE account_id = $1 FOR UPDATE
        ) AS a
    `)

	if contains(include, "identity") {
		queryBuilder.WriteString(`
            LEFT JOIN blnk.identity i ON a.identity_id = i.identity_id
        `)
	}
	if contains(include, "ledger") {
		queryBuilder.WriteString(`
            LEFT JOIN blnk.ledgers l ON a.ledger_id = l.ledger_id
        `)
	}
	if contains(include, "balance") {
		queryBuilder.WriteString(`
            LEFT JOIN blnk.balances b ON a.balance_id = b.balance_id
        `)
	}

	return queryBuilder.String()
}

func scanAccountRow(row *sql.Row, tx *sql.Tx, include []string) (*model.Account, error) {
	account := &model.Account{}
	balance := &model.Balance{}
	identity := &model.Identity{}
	ledger := &model.Ledger{}

	metaDataJSON := []byte{}
	var scanArgs []interface{}

	scanArgs = append(scanArgs, &account.AccountID, &account.Name, &account.Number, &account.BankName,
		&account.Currency,
		&account.LedgerID, &account.IdentityID, &account.BalanceID, &balance.CreatedAt, &metaDataJSON)

	if contains(include, "balance") {
		scanArgs = append(scanArgs, &balance.BalanceID, &balance.Balance, &balance.CreditBalance,
			&balance.DebitBalance, &balance.Currency, &balance.CurrencyMultiplier,
			&balance.LedgerID, &balance.IdentityID, &balance.CreatedAt, &metaDataJSON)
	}

	if contains(include, "identity") {
		scanArgs = append(scanArgs, &identity.IdentityID, &identity.FirstName, &identity.OrganizationName, &identity.Category, &identity.LastName,
			&identity.OtherNames, &identity.Gender, &identity.DOB, &identity.EmailAddress,
			&identity.PhoneNumber, &identity.Nationality, &identity.Street, &identity.Country,
			&identity.State, &identity.PostCode, &identity.City, &identity.IdentityType, &identity.CreatedAt, &metaDataJSON)
	}

	if contains(include, "ledger") {
		scanArgs = append(scanArgs, &ledger.LedgerID, &ledger.Name, &ledger.CreatedAt)
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		fmt.Println("Errror: ", err)
		_ = tx.Rollback()
		return nil, err
	}

	err = json.Unmarshal(metaDataJSON, &account.MetaData)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	if contains(include, "identity") {
		account.Identity = identity
	}
	if contains(include, "balance") {
		account.Balance = balance
	}
	if contains(include, "ledger") {
		account.Ledger = ledger
	}

	return account, nil
}

// GetAllAccounts retrieves all accounts
func (d Datasource) GetAllAccounts() ([]model.Account, error) {
	rows, err := d.Conn.Query(`
    SELECT account_id, name, number, bank_name, currency, created_at, meta_data 
    FROM accounts
	ORDER BY created_at DESC

`)

	if err != nil {
		return nil, err
	}
	var accounts []model.Account
	for rows.Next() {
		account := model.Account{}
		var metaDataJSON []byte
		err := rows.Scan(&account.AccountID, &account.Name, &account.Number, &account.BankName, &account.Currency, &account.CreatedAt, &metaDataJSON)
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
func (d Datasource) GetAccountByNumber(number string) (*model.Account, error) {
	row := d.Conn.QueryRow(`
		SELECT account_id, name, number, bank_name, created_at, meta_data 
		FROM blnk.accounts WHERE number = $1
	`, number)

	account := &model.Account{}
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
func (d Datasource) UpdateAccount(account *model.Account) error {
	metaDataJSON, err := json.Marshal(account.MetaData)
	if err != nil {
		return err
	}

	_, err = d.Conn.Exec(`
		UPDATE blnk.accounts
		SET name = $2, number = $3, bank_name = $4, meta_data = $5
		WHERE account_id = $1
	`, account.AccountID, account.Name, account.Number, account.BankName, metaDataJSON)

	return err
}

// DeleteAccount deletes a specific account from the database
func (d Datasource) DeleteAccount(id string) error {
	_, err := d.Conn.Exec(`
		DELETE FROM blnk.accounts WHERE account_id = $1
	`, id)
	return err
}
