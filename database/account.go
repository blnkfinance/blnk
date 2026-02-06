/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package database

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/model"
)

// CreateAccount inserts a new Account into the database.
// This function handles metadata serialization and database insertion.
// Parameters:
// - account: The account model containing fields such as name, number, bank name, currency, ledger ID, identity ID, and balance ID.
// Returns:
// - model.Account: The created account with the assigned account ID and creation timestamp.
// - error: Returns an error if any issue occurs while marshalling metadata or executing the database query.
func (d Datasource) CreateAccount(account model.Account) (model.Account, error) {
	// Serialize metadata into JSON
	metaDataJSON, err := json.Marshal(account.MetaData)
	if err != nil {
		return account, err // Return error if metadata marshalling fails
	}

	// Generate a unique account ID and assign the current time for the account creation
	account.AccountID = model.GenerateUUIDWithSuffix("acc")
	account.CreatedAt = time.Now()

	// Insert the new account into the database
	_, err = d.Conn.Exec(`
		INSERT INTO blnk.accounts (account_id, name, number, bank_name, currency, ledger_id, identity_id, balance_id, created_at, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, account.AccountID, account.Name, account.Number, account.BankName, account.Currency, account.LedgerID, account.IdentityID, account.BalanceID, account.CreatedAt, metaDataJSON)

	// Return the account object and any error that occurred during the database operation
	return account, err
}

// GetAccountByID retrieves an account by its ID from the database.
// It uses a transaction to ensure consistency and can include additional
// related entities like balance, identity, or ledger if specified in the `include` parameter.
// Parameters:
// - id: The ID of the account to retrieve.
// - include: A list of related entities to include in the query result.
// Returns:
// - A pointer to the retrieved Account or an error if something goes wrong.
func (d Datasource) GetAccountByID(id string, include []string) (*model.Account, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// Start a transaction
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	// Prepare the query with additional includes if needed
	var queryBuilder strings.Builder
	query := prepareAccountQueries(queryBuilder, include)

	// Execute the query
	row := tx.QueryRow(query, id)

	// Scan the result into the account object
	account, err := scanAccountRow(row, tx, include)
	if err != nil {
		if err == sql.ErrNoRows {
			// No account found for the given ID
			return nil, fmt.Errorf("account with ID '%s' not found", id)
		}
		return nil, err
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	// Return the account object
	return account, nil
}

// prepareAccountQueries constructs an SQL query for retrieving accounts, including
// optional related entities such as balance, identity, and ledger if specified in the `include` parameter.
// Parameters:
// - queryBuilder: A strings.Builder used to build the query string.
// - include: A list of related entities (balance, identity, ledger) to be included in the query.
// Returns:
// - A constructed SQL query string.
func prepareAccountQueries(queryBuilder strings.Builder, include []string) string {
	var selectFields []string
	// Default fields for the account
	selectFields = append(selectFields,
		"a.account_id", "a.name", "a.number", "a.bank_name",
		"a.currency", "a.ledger_id",
		"a.identity_id", "a.balance_id", "a.created_at", "a.meta_data")

	// Include balance fields if specified
	if contains(include, "balance") {
		selectFields = append(selectFields,
			"b.balance_id", "b.balance", "b.credit_balance", "b.debit_balance",
			"b.currency", "b.currency_multiplier", "b.ledger_id",
			"COALESCE(b.identity_id, '') as identity_id", "b.created_at", "b.meta_data")
	}

	// Include identity fields if specified
	if contains(include, "identity") {
		selectFields = append(selectFields,
			"i.identity_id", "i.first_name", "i.organization_name", "i.category", "i.last_name", "i.other_names",
			"i.gender", "i.dob", "i.email_address", "i.phone_number",
			"i.nationality", "i.street", "i.country", "i.state",
			"i.post_code", "i.city", "i.identity_type", "i.created_at", "i.meta_data")
	}

	// Include ledger fields if specified
	if contains(include, "ledger") {
		selectFields = append(selectFields,
			"l.ledger_id", "l.name", "l.created_at")
	}

	// Construct the query
	queryBuilder.WriteString("SELECT ")
	queryBuilder.WriteString(strings.Join(selectFields, ", "))
	queryBuilder.WriteString(`
        FROM (
            SELECT * FROM blnk.accounts WHERE account_id = $1
        ) AS a
    `)

	// Join identity if specified
	if contains(include, "identity") {
		queryBuilder.WriteString(`
            LEFT JOIN blnk.identity i ON a.identity_id = i.identity_id
        `)
	}

	// Join ledger if specified
	if contains(include, "ledger") {
		queryBuilder.WriteString(`
            LEFT JOIN blnk.ledgers l ON a.ledger_id = l.ledger_id
        `)
	}

	// Join balance if specified
	if contains(include, "balance") {
		queryBuilder.WriteString(`
            LEFT JOIN blnk.balances b ON a.balance_id = b.balance_id
        `)
	}

	return queryBuilder.String()
}

// scanAccountRow scans a row from the database into an Account object.
// It can also scan related Balance, Identity, and Ledger data if specified in the `include` parameter.
// Parameters:
// - row: The SQL row containing the account data.
// - tx: The active SQL transaction.
// - include: A list of related entities (balance, identity, ledger) to be included in the scan.
// Returns:
// - A pointer to the populated Account object or an error if the scan fails.
func scanAccountRow(row *sql.Row, tx *sql.Tx, include []string) (*model.Account, error) {
	account := &model.Account{}
	balance := &model.Balance{}
	identity := &model.Identity{}
	ledger := &model.Ledger{}

	metaDataJSON := []byte{}
	var scanArgs []interface{}

	// Default fields for the account
	scanArgs = append(scanArgs, &account.AccountID, &account.Name, &account.Number, &account.BankName,
		&account.Currency,
		&account.LedgerID, &account.IdentityID, &account.BalanceID, &balance.CreatedAt, &metaDataJSON)

	// Add fields for balance if included
	if contains(include, "balance") {
		scanArgs = append(scanArgs, &balance.BalanceID, &balance.Balance, &balance.CreditBalance,
			&balance.DebitBalance, &balance.Currency, &balance.CurrencyMultiplier,
			&balance.LedgerID, &balance.IdentityID, &balance.CreatedAt, &metaDataJSON)
	}

	// Add fields for identity if included
	if contains(include, "identity") {
		scanArgs = append(scanArgs, &identity.IdentityID, &identity.FirstName, &identity.OrganizationName, &identity.Category, &identity.LastName,
			&identity.OtherNames, &identity.Gender, &identity.DOB, &identity.EmailAddress,
			&identity.PhoneNumber, &identity.Nationality, &identity.Street, &identity.Country,
			&identity.State, &identity.PostCode, &identity.City, &identity.IdentityType, &identity.CreatedAt, &metaDataJSON)
	}

	// Add fields for ledger if included
	if contains(include, "ledger") {
		scanArgs = append(scanArgs, &ledger.LedgerID, &ledger.Name, &ledger.CreatedAt)
	}

	// Perform the row scan
	err := row.Scan(scanArgs...)
	if err != nil {
		fmt.Println("Error: ", err)
		_ = tx.Rollback()
		return nil, err
	}

	// Unmarshal the account metadata from JSON
	err = json.Unmarshal(metaDataJSON, &account.MetaData)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	// Assign related entities if included
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

// GetAllAccounts retrieves all accounts from the database.
// It returns a list of Account objects, each populated with metadata and account details.
// Returns:
// - A slice of Account objects or an error if the query or scan fails.
func (d Datasource) GetAllAccounts() ([]model.Account, error) {
	// Execute the SQL query to retrieve account data
	rows, err := d.Conn.Query(`
		SELECT account_id, name, number, bank_name, currency, created_at, meta_data 
		FROM blnk.accounts
		ORDER BY created_at DESC
	`)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	// Create a slice to store the account results
	var accounts []model.Account

	// Iterate through the rows
	for rows.Next() {
		account := model.Account{}
		var metaDataJSON []byte

		// Scan the row into an Account object
		err := rows.Scan(&account.AccountID, &account.Name, &account.Number, &account.BankName, &account.Currency, &account.CreatedAt, &metaDataJSON)
		if err != nil {
			return nil, err
		}

		// Unmarshal the metadata JSON into the MetaData field
		err = json.Unmarshal(metaDataJSON, &account.MetaData)
		if err != nil {
			return nil, err
		}

		// Append the account to the accounts slice
		accounts = append(accounts, account)
	}

	// Return the list of accounts
	return accounts, nil
}

// GetAccountByNumber retrieves an account based on its number.
// It queries the database for an account with the given number and returns the account details if found.
// Parameters:
// - number: The account number to search for.
// Returns:
// - A pointer to the Account object if found, or an error if the account is not found or a query error occurs.
func (d Datasource) GetAccountByNumber(number string) (*model.Account, error) {
	// Query the database for the account with the given number
	row := d.Conn.QueryRow(`
		SELECT account_id, name, number, bank_name, created_at, meta_data 
		FROM blnk.accounts WHERE number = $1
	`, number)

	account := &model.Account{}
	var metaDataJSON []byte

	// Scan the result into the Account object
	err := row.Scan(&account.AccountID, &account.Name, &account.Number, &account.BankName, &account.CreatedAt, &metaDataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, fmt.Errorf("account with number '%s' not found", number)
		}
		return nil, err
	}

	// Unmarshal the metadata JSON into the MetaData field
	err = json.Unmarshal(metaDataJSON, &account.MetaData)
	if err != nil {
		return nil, err
	}

	// Return the account object
	return account, nil
}

// UpdateAccount updates a specific account in the database.
// It updates the account's name, number, bank name, and metadata based on the account ID.
// Parameters:
// - account: A pointer to the Account object containing the updated account information.
// Returns:
// - An error if the update fails, otherwise returns nil.
func (d Datasource) UpdateAccount(account *model.Account) error {
	// Marshal the MetaData field into JSON
	metaDataJSON, err := json.Marshal(account.MetaData)
	if err != nil {
		return err
	}

	// Execute the SQL update statement
	_, err = d.Conn.Exec(`
		UPDATE blnk.accounts
		SET name = $2, number = $3, bank_name = $4, meta_data = $5
		WHERE account_id = $1
	`, account.AccountID, account.Name, account.Number, account.BankName, metaDataJSON)

	// Return any errors encountered during the update
	return err
}

// DeleteAccount deletes a specific account from the database.
// It removes the account with the given account ID from the accounts table.
// Parameters:
// - id: The unique ID of the account to be deleted.
// Returns:
// - An error if the deletion fails, otherwise returns nil.
func (d Datasource) DeleteAccount(id string) error {
	// Execute the SQL delete statement
	_, err := d.Conn.Exec(`
		DELETE FROM blnk.accounts WHERE account_id = $1
	`, id)

	// Return any errors encountered during the deletion
	return err
}

// GetAllAccountsWithFilter retrieves accounts with advanced filtering support.
// It delegates to GetAllAccountsWithFilterAndOptions with nil options.
//
// Parameters:
// - ctx: Context for the database operation.
// - filters: A QueryFilterSet containing the filter conditions.
// - limit: The maximum number of accounts to return.
// - offset: The offset to start fetching accounts from (for pagination).
//
// Returns:
// - []model.Account: A slice of accounts matching the filter criteria.
// - error: An error if the query fails or if there's an issue processing the results.
func (d Datasource) GetAllAccountsWithFilter(ctx context.Context, filters *filter.QueryFilterSet, limit, offset int) ([]model.Account, error) {
	accounts, _, err := d.GetAllAccountsWithFilterAndOptions(ctx, filters, nil, limit, offset)
	return accounts, err
}

// GetAllAccountsWithFilterAndOptions retrieves accounts with filtering, sorting, and optional count.
// It uses the filter package to build SQL WHERE and ORDER BY conditions.
//
// Parameters:
// - ctx: Context for the database operation.
// - filters: A QueryFilterSet containing the filter conditions.
// - opts: Query options including sorting and count settings.
// - limit: The maximum number of accounts to return.
// - offset: The offset to start fetching accounts from (for pagination).
//
// Returns:
// - []model.Account: A slice of accounts matching the filter criteria.
// - *int64: Optional total count of matching records (if opts.IncludeCount is true).
// - error: An error if the query fails or if there's an issue processing the results.
func (d Datasource) GetAllAccountsWithFilterAndOptions(ctx context.Context, filters *filter.QueryFilterSet, opts *filter.QueryOptions, limit, offset int) ([]model.Account, *int64, error) {
	if limit <= 0 || limit > 100 {
		limit = 20
	}

	if err := filter.ValidateSortByForTable(opts, "accounts"); err != nil {
		return nil, nil, apierror.NewAPIError(apierror.ErrBadRequest, "Invalid sort_by field", nil)
	}

	// Build filter conditions with options
	result, err := filter.BuildWithOptions(filters, "accounts", "", 1, opts)
	if err != nil {
		return nil, nil, apierror.NewAPIError(apierror.ErrBadRequest, fmt.Sprintf("Invalid filter: %s", err.Error()), err)
	}

	// Determine select fields based on whether count is requested
	selectFields := "account_id, name, number, bank_name, currency, ledger_id, identity_id, balance_id, created_at, meta_data"
	if opts != nil && opts.IncludeCount {
		selectFields += ", COUNT(*) OVER() AS total_count"
	}

	// Build base query
	baseQuery := fmt.Sprintf(`
		SELECT %s
		FROM blnk.accounts
	`, selectFields)

	var args []interface{}
	args = append(args, result.Args...)
	argPos := result.NextArgPos

	// Add WHERE clause if filters are provided
	if len(result.Conditions) > 0 {
		baseQuery += " WHERE " + strings.Join(result.Conditions, " AND ")
	}

	// Add ORDER BY clause
	baseQuery += " ORDER BY " + result.OrderBy

	// Add pagination
	baseQuery += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argPos, argPos+1)
	args = append(args, limit, offset)

	rows, err := d.Conn.QueryContext(ctx, baseQuery, args...)
	if err != nil {
		return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve accounts", err)
	}
	defer func() { _ = rows.Close() }()

	var accounts []model.Account
	var totalCount *int64

	for rows.Next() {
		account := model.Account{}
		var metaDataJSON []byte

		if opts != nil && opts.IncludeCount {
			var count int64
			err = rows.Scan(
				&account.AccountID, &account.Name, &account.Number, &account.BankName,
				&account.Currency, &account.LedgerID, &account.IdentityID, &account.BalanceID,
				&account.CreatedAt, &metaDataJSON,
				&count,
			)
			if totalCount == nil {
				totalCount = &count
			}
		} else {
			err = rows.Scan(
				&account.AccountID, &account.Name, &account.Number, &account.BankName,
				&account.Currency, &account.LedgerID, &account.IdentityID, &account.BalanceID,
				&account.CreatedAt, &metaDataJSON,
			)
		}
		if err != nil {
			return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan account data", err)
		}

		err = json.Unmarshal(metaDataJSON, &account.MetaData)
		if err != nil {
			return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		accounts = append(accounts, account)
	}

	if err = rows.Err(); err != nil {
		return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over accounts", err)
	}

	return accounts, totalCount, nil
}
