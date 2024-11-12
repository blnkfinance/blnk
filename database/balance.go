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
	"math/big"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/sirupsen/logrus"

	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
)

// Helper function to check if a slice contains a value.
func contains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

// Prepares a dynamic SQL query based on the fields to be included.
// This query fetches balance details, including optional joins for identity and ledger.
func prepareQueries(queryBuilder strings.Builder, include []string) string {
	var selectFields []string

	// Default fields for balances
	selectFields = append(selectFields,
		"b.balance_id", "b.balance", "b.credit_balance", "b.debit_balance",
		"b.currency", "b.currency_multiplier", "b.ledger_id",
		"COALESCE(b.identity_id, '') as identity_id", "b.created_at", "b.meta_data", "b.inflight_balance", "b.inflight_credit_balance", "b.inflight_debit_balance", "b.version", "b.indicator")

	// Conditionally include identity fields
	if contains(include, "identity") {
		selectFields = append(selectFields,
			"i.identity_id", "i.first_name", "i_name", "i.category", "i.last_name", "i.other_names",
			"i.gender", "i.dob", "i.email_address", "i.phone_number",
			"i.nationality", "i.street", "i.country", "i.state",
			"i.post_code", "i.city", "i.created_at")
	}

	// Conditionally include ledger fields
	if contains(include, "ledger") {
		selectFields = append(selectFields,
			"l.ledger_id", "l.name", "l.created_at")
	}

	// Construct the SQL query
	queryBuilder.WriteString("SELECT ")
	queryBuilder.WriteString(strings.Join(selectFields, ", "))
	queryBuilder.WriteString(`
        FROM (
            SELECT * FROM blnk.balances WHERE balance_id = $1
        ) AS b
    `)

	// Add optional joins for identity and ledger
	if contains(include, "identity") {
		queryBuilder.WriteString(`
            LEFT JOIN blnk.identity i ON b.identity_id = i.identity_id
        `)
	}
	if contains(include, "ledger") {
		queryBuilder.WriteString(`
            LEFT JOIN blnk.ledgers l ON b.ledger_id = l.ledger_id
        `)
	}

	return queryBuilder.String()
}

// Scans a SQL row result and maps it into a Balance object, including optional identity and ledger data.
// Converts string representations of big.Int fields into actual big.Int objects.
func scanRow(row *sql.Row, tx *sql.Tx, include []string) (*model.Balance, error) {
	balance := &model.Balance{}
	identity := &model.Identity{}
	ledger := &model.Ledger{}
	metaDataJSON := []byte{}

	// Temporary variables to hold string representations of big.Int fields
	var balanceStr, creditBalanceStr, debitBalanceStr, inflightBalanceStr, inflightCreditBalanceStr, inflightDebitBalanceStr string
	var indicator sql.NullString

	var scanArgs []interface{}
	// Add scan arguments for default balance fields
	scanArgs = append(scanArgs, &balance.BalanceID, &balanceStr, &creditBalanceStr,
		&debitBalanceStr, &balance.Currency, &balance.CurrencyMultiplier,
		&balance.LedgerID, &balance.IdentityID, &balance.CreatedAt, &metaDataJSON,
		&inflightBalanceStr, &inflightCreditBalanceStr, &inflightDebitBalanceStr, &balance.Version, &indicator)

	// Conditionally scan for identity fields
	if contains(include, "identity") {
		scanArgs = append(scanArgs, &identity.IdentityID, &identity.FirstName, &identity.OrganizationName, &identity.Category, &identity.LastName,
			&identity.OtherNames, &identity.Gender, &identity.DOB, &identity.EmailAddress,
			&identity.PhoneNumber, &identity.Nationality, &identity.Street, &identity.Country,
			&identity.State, &identity.PostCode, &identity.City, &identity.CreatedAt)
	}

	// Conditionally scan for ledger fields
	if contains(include, "ledger") {
		scanArgs = append(scanArgs, &ledger.LedgerID, &ledger.Name, &ledger.CreatedAt)
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	// Convert string representations to big.Int
	balance.Balance, _ = new(big.Int).SetString(balanceStr, 10)
	balance.CreditBalance, _ = new(big.Int).SetString(creditBalanceStr, 10)
	balance.DebitBalance, _ = new(big.Int).SetString(debitBalanceStr, 10)
	balance.InflightBalance, _ = new(big.Int).SetString(inflightBalanceStr, 10)
	balance.InflightCreditBalance, _ = new(big.Int).SetString(inflightCreditBalanceStr, 10)
	balance.InflightDebitBalance, _ = new(big.Int).SetString(inflightDebitBalanceStr, 10)

	// Handle null indicator field
	if indicator.Valid {
		balance.Indicator = indicator.String
	} else {
		balance.Indicator = ""
	}

	// Unmarshal metadata JSON
	err = json.Unmarshal(metaDataJSON, &balance.MetaData)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	// Attach identity and ledger objects if included
	if contains(include, "identity") {
		balance.Identity = identity
	}
	if contains(include, "ledger") {
		balance.Ledger = ledger
	}

	return balance, nil
}

// CreateBalance inserts a new balance record into the `blnk.balances` table in the database.
// It handles the generation of a unique balance ID, default values for fields, and any necessary error handling.
//
// Parameters:
// - balance: A model.Balance object containing the balance information to be created.
//
// Returns:
// - model.Balance: The created balance with its ID and timestamp populated.
// - error: Returns an APIError in case of failures such as database conflicts or other issues.
func (d Datasource) CreateBalance(balance model.Balance) (model.Balance, error) {
	// Marshal metadata into JSON
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return model.Balance{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	// Generate a unique balance ID and set the creation timestamp
	balance.BalanceID = model.GenerateUUIDWithSuffix("bln")
	balance.CreatedAt = time.Now()

	// Handle nullable fields
	var identityID interface{} = balance.IdentityID
	if balance.IdentityID == "" {
		identityID = nil
	}

	var indicator interface{} = balance.Indicator
	if balance.Indicator == "" {
		indicator = nil
	}

	// Set default values for balance fields if they are nil
	if balance.Balance == nil {
		balance.Balance = big.NewInt(0)
	}
	if balance.CreditBalance == nil {
		balance.CreditBalance = big.NewInt(0)
	}
	if balance.DebitBalance == nil {
		balance.DebitBalance = big.NewInt(0)
	}
	if balance.InflightBalance == nil {
		balance.InflightBalance = big.NewInt(0)
	}
	if balance.InflightCreditBalance == nil {
		balance.InflightCreditBalance = big.NewInt(0)
	}
	if balance.InflightDebitBalance == nil {
		balance.InflightDebitBalance = big.NewInt(0)
	}

	// Insert the balance into the database
	_, err = d.Conn.Exec(`
		INSERT INTO blnk.balances (balance_id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, identity_id, indicator, created_at, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11)
	`, balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, identityID, indicator, balance.CreatedAt, &metaDataJSON)

	if err != nil {
		// Handle specific PostgreSQL errors (e.g., unique or foreign key violations)
		pqErr, ok := err.(*pq.Error)
		if ok {
			switch pqErr.Code.Name() {
			case "unique_violation":
				return model.Balance{}, apierror.NewAPIError(apierror.ErrConflict, "Balance with this ID already exists", err)
			case "foreign_key_violation":
				return model.Balance{}, apierror.NewAPIError(apierror.ErrBadRequest, "Invalid ledger ID", err)
			default:
				return model.Balance{}, apierror.NewAPIError(apierror.ErrInternalServer, "Database error occurred", err)
			}
		}
		// Return a generic error if the specific type couldn't be determined
		return model.Balance{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to create balance", err)
	}

	// Return the created balance
	return balance, nil
}

// GetBalanceByID retrieves a balance by its ID from the database, along with optional related data such as identity or ledger, based on the `include` parameter.
// The method starts a transaction, executes the query, and processes the result.
//
// Parameters:
// - id: The unique ID of the balance to retrieve.
// - include: A slice of strings that specifies which related data to include in the result. Possible values include "identity" and "ledger".
//
// Returns:
// - *model.Balance: A pointer to the retrieved Balance object.
// - error: Returns an APIError in case of errors such as database failures or if the balance is not found.
func (d Datasource) GetBalanceByID(id string, include []string) (*model.Balance, error) {
	// Set a context with a 1-minute timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// Start a transaction
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	// Prepare and execute the query
	var queryBuilder strings.Builder
	query := prepareQueries(queryBuilder, include)
	row := tx.QueryRow(query, id)

	// Scan the result into a Balance object
	balance, err := scanRow(row, tx, include)
	if err != nil {
		if err == sql.ErrNoRows {
			// If no balance is found
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Balance with ID '%s' not found", id), err)
		} else {
			// Handle other errors
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan balance data", err)
		}
	}

	// Commit the transaction
	err = tx.Commit()
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	return balance, nil
}

// GetBalanceByIDLite retrieves a balance by its unique ID with a lighter set of fields.
// This version avoids loading additional related data like identity and ledger.
//
// Parameters:
// - id: The ID of the balance to retrieve.
//
// Returns:
// - *model.Balance: A pointer to the retrieved Balance object.
// - error: Returns an APIError in case of errors such as database failures or if the balance is not found.
func (d Datasource) GetBalanceByIDLite(id string) (*model.Balance, error) {
	var balance model.Balance
	var balanceValue, creditBalanceValue, debitBalanceValue, inflightBalanceValue, inflightCreditBalanceValue, inflightDebitBalanceValue int64
	var indicator sql.NullString

	// Execute the query
	row := d.Conn.QueryRow(`
	   SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version
	   FROM blnk.balances
	   WHERE balance_id = $1
	`, id)

	// Scan the result into a Balance object
	err := row.Scan(
		&balance.BalanceID,
		&indicator,
		&balance.Currency,
		&balance.CurrencyMultiplier,
		&balance.LedgerID,
		&balanceValue,
		&creditBalanceValue,
		&debitBalanceValue,
		&inflightBalanceValue,
		&inflightCreditBalanceValue,
		&inflightDebitBalanceValue,
		&balance.CreatedAt,
		&balance.Version,
	)

	// Handle null indicator field
	if indicator.Valid {
		balance.Indicator = indicator.String
	} else {
		balance.Indicator = ""
	}

	if err != nil {
		logrus.Errorf("balance lite error %v", err)
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Balance with ID '%s' not found", id), err)
		} else {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan balance data", err)
		}
	}

	// Convert balance fields from int64 to big.Int
	balance.Balance = big.NewInt(balanceValue)
	balance.CreditBalance = big.NewInt(creditBalanceValue)
	balance.DebitBalance = big.NewInt(debitBalanceValue)
	balance.InflightBalance = big.NewInt(inflightBalanceValue)
	balance.InflightCreditBalance = big.NewInt(inflightCreditBalanceValue)
	balance.InflightDebitBalance = big.NewInt(inflightDebitBalanceValue)

	return &balance, nil
}

// GetBalanceByIndicator retrieves a balance from the database using the specified indicator and currency.
// The function scans the query result into a Balance object and converts various fields from int64 to big.Int.
// It returns the balance if found, or an error if the balance does not exist.
//
// Parameters:
// - indicator: A unique identifier associated with the balance (e.g., an account identifier).
// - currency: The currency in which the balance is denominated.
//
// Returns:
// - *model.Balance: The retrieved balance object or an empty Balance object if not found.
// - error: An error if any issues occur during the query execution or data retrieval.
func (d Datasource) GetBalanceByIndicator(indicator, currency string) (*model.Balance, error) {
	var balance model.Balance
	var balanceValue, creditBalanceValue, debitBalanceValue, inflightBalanceValue, inflightCreditBalanceValue, inflightDebitBalanceValue int64

	// Execute query to find the balance with the given indicator and currency
	row := d.Conn.QueryRow(`
	   SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version
	   FROM blnk.balances
	   WHERE indicator = $1 AND currency = $2
	`, indicator, currency)

	// Scan the result into the Balance object
	err := row.Scan(
		&balance.BalanceID,
		&balance.Indicator,
		&balance.Currency,
		&balance.CurrencyMultiplier,
		&balance.LedgerID,
		&balanceValue,
		&creditBalanceValue,
		&debitBalanceValue,
		&inflightBalanceValue,
		&inflightCreditBalanceValue,
		&inflightDebitBalanceValue,
		&balance.CreatedAt,
		&balance.Version,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			// Handle the case where no balance was found with the given indicator and currency
			return &model.Balance{}, fmt.Errorf("balance with indicator '%s' not found", indicator)
		}
		// Return other types of errors, such as query execution failures
		return nil, err
	}

	// Convert the scanned int64 values into big.Int for precision in financial calculations
	balance.Balance = big.NewInt(balanceValue)
	balance.CreditBalance = big.NewInt(creditBalanceValue)
	balance.DebitBalance = big.NewInt(debitBalanceValue)
	balance.InflightBalance = big.NewInt(inflightBalanceValue)
	balance.InflightCreditBalance = big.NewInt(inflightCreditBalanceValue)
	balance.InflightDebitBalance = big.NewInt(inflightDebitBalanceValue)

	// Return the populated Balance object
	return &balance, nil
}

// GetAllBalances retrieves a limited set of balances from the database, up to 20 records.
// It processes each balance by scanning the query result, converting numerical fields to big.Int, and parsing metadata from JSON format.
// The function returns a slice of Balance objects or an error if any issues occur during the database query or data processing.
//
// Parameters:
// - limit: The maximum number of balances to return (e.g., 20).
// - offset: The offset to start fetching balances from (for pagination).
//
// Returns:
// - []model.Balance: A slice of Balance objects containing balance information such as balance amount, credit balance, debit balance, and metadata.
// - error: An error if any occurs during the query execution, data retrieval, or JSON parsing.
func (d Datasource) GetAllBalances(limit, offset int) ([]model.Balance, error) {
	var indicator sql.NullString
	// Execute SQL query to select all balances with a limit of 20 records
	rows, err := d.Conn.Query(`
		SELECT balance_id, indicator, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, meta_data
		FROM blnk.balances
		ORDER BY created_at DESC
		LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err // Return error if the query fails
	}
	defer func(rows *sql.Rows) {
		err := rows.Close() // Ensure rows are closed after query execution
		if err != nil {
			logrus.Error(err) // Log error if closing rows fails
		}
	}(rows)

	// Slice to store the retrieved balances
	var balances []model.Balance
	var balanceValue, creditBalanceValue, debitBalanceValue, inflightBalanceValue, inflightCreditBalanceValue, inflightDebitBalanceValue int64

	// Iterate through the rows and scan each balance into the Balance object
	for rows.Next() {
		balance := model.Balance{}
		var metaDataJSON []byte

		// Scan values from the current row into the balance object and temporary variables
		err = rows.Scan(
			&balance.BalanceID,
			&indicator,
			&balanceValue,
			&creditBalanceValue,
			&debitBalanceValue,
			&balance.Currency,
			&balance.CurrencyMultiplier,
			&balance.LedgerID,
			&balance.CreatedAt,
			&metaDataJSON,
		)
		if err != nil {
			return nil, err // Return error if scanning fails
		}

		fmt.Println("Indicator: ", indicator.String)
		// Handle null indicator field
		if indicator.Valid {
			balance.Indicator = indicator.String
		} else {
			balance.Indicator = ""
		}

		// Convert the scanned int64 values into big.Int for accurate balance calculations
		balance.Balance = big.NewInt(balanceValue)
		balance.CreditBalance = big.NewInt(creditBalanceValue)
		balance.DebitBalance = big.NewInt(debitBalanceValue)
		balance.InflightBalance = big.NewInt(inflightBalanceValue)
		balance.InflightCreditBalance = big.NewInt(inflightCreditBalanceValue)
		balance.InflightDebitBalance = big.NewInt(inflightDebitBalanceValue)

		// Parse the metadata JSON into the MetaData map field
		err = json.Unmarshal(metaDataJSON, &balance.MetaData)
		if err != nil {
			return nil, err // Return error if JSON parsing fails
		}

		// Append the balance to the slice of balances
		balances = append(balances, balance)
	}

	// Return the slice of balances
	return balances, nil
}

// GetSourceDestination retrieves balances for both the source and destination by their IDs.
// It queries the database using a stored procedure `blnk.get_balances_by_id`, which takes the sourceId and destinationId as inputs.
// The function processes each balance, converting balance fields to big.Int and parsing the metadata from JSON format.
// It returns a slice of pointers to Balance objects or an error if any issues occur during the query or data processing.
//
// Parameters:
// - sourceId: The ID of the source balance to retrieve.
// - destinationId: The ID of the destination balance to retrieve.
//
// Returns:
// - []*model.Balance: A slice of pointers to Balance objects containing the source and destination balances with their details such as balance amount, credit balance, debit balance, and metadata.
// - error: An error if any occurs during the query execution, data retrieval, or JSON parsing.
func (d Datasource) GetSourceDestination(sourceId, destinationId string) ([]*model.Balance, error) {
	// Execute SQL query to select balances for source and destination using a stored procedure
	rows, err := d.Conn.Query(`
		SELECT blnk.get_balances_by_id($1,$2)
	`, sourceId, destinationId)
	if err != nil {
		// Return an error if the query execution fails
		return nil, err
	}
	defer func(rows *sql.Rows) {
		// Ensure the rows are closed after the query is completed
		err := rows.Close()
		if err != nil {
			logrus.Error(err) // Log any error that occurs while closing the rows
		}
	}(rows)

	// Slice to store the retrieved balances
	var balances []*model.Balance

	// Iterate through the result set and scan each row into a Balance object
	for rows.Next() {
		balance := model.Balance{}
		var metaDataJSON []byte

		// Scan the values from the current row into the balance object and temporary metadata variable
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
			// Return an error if scanning the row fails
			return nil, err
		}

		// Parse the metadata JSON into the MetaData map field
		err = json.Unmarshal(metaDataJSON, &balance.MetaData)
		if err != nil {
			// Return an error if JSON parsing fails
			return nil, err
		}

		// Append the balance to the slice of balances
		balances = append(balances, &balance)
	}

	// Return the slice of balances containing the source and destination balances
	return balances, nil
}

// UpdateBalances updates both the source and destination balances in a single transaction.
// The function begins a database transaction, updates the balances, and commits the transaction if all updates succeed.
// In case of any failure, the transaction is rolled back to ensure data integrity.
//
// Parameters:
// - ctx: The context to manage the lifecycle of the transaction.
// - sourceBalance: A pointer to the source balance object that needs to be updated.
// - destinationBalance: A pointer to the destination balance object that needs to be updated.
//
// Returns:
// - error: Returns an error if there is a failure to start the transaction, update any of the balances, or commit the transaction.
func (d Datasource) UpdateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	// Begin a new transaction
	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		// Return an error if the transaction cannot be initiated
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	// Ensure that the transaction is rolled back if an error occurs during execution
	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	// Attempt to update the source balance
	if err := updateBalance(ctx, tx, sourceBalance); err != nil {
		// Return the error and rollback the transaction
		return err
	}

	// Attempt to update the destination balance
	if err := updateBalance(ctx, tx, destinationBalance); err != nil {
		// Return the error and rollback the transaction
		return err
	}

	// Commit the transaction if both updates succeed
	if err := tx.Commit(); err != nil {
		// Return an error if the commit fails
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	// Return nil if the transaction was successful
	return nil
}

// updateBalance updates a balance entry in the database.
// This function handles the logic of updating all balance-related fields while ensuring data consistency using optimistic locking.
// The version field is incremented after a successful update to maintain control over concurrent modifications.
//
// Parameters:
// - ctx: The context for managing the operation's lifecycle and cancellation.
// - tx: The database transaction in which the update is performed.
// - balance: A pointer to the balance object containing the updated balance information.
//
// Returns:
// - error: Returns an error if the update operation fails at any point, including issues with metadata marshalling, query execution, or optimistic locking.
func updateBalance(ctx context.Context, tx *sql.Tx, balance *model.Balance) error {
	// Marshal the MetaData into JSON format
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	// SQL query to update the balance
	query := `
        UPDATE blnk.balances
        SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
        WHERE balance_id = $1 AND version = $13
    `

	// Execute the update query within the provided transaction context
	result, err := tx.ExecContext(ctx, query, balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.InflightBalance.String(), balance.InflightCreditBalance.String(), balance.InflightDebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, metaDataJSON, balance.Version)
	if err != nil {
		// Return an error if the query execution fails
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update balance", err)
	}

	// Check if any rows were affected by the update
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		// Return an error if unable to get affected rows
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	// If no rows were updated, return an optimistic locking error
	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrConflict, fmt.Sprintf("Optimistic locking failure: balance with ID '%s' may have been updated or deleted by another transaction", balance.BalanceID), nil)
	}

	// Increment the version number after a successful update
	balance.Version++

	// Return nil indicating a successful update
	return nil
}

// UpdateBalance updates an existing balance entry in the database.
// This method takes a balance object and updates the corresponding fields in the database, based on the provided balance ID.
// It handles both the balance data and the associated metadata.
//
// Parameters:
// - balance: A pointer to the balance object containing the updated balance information. This includes fields such as `balance`, `credit_balance`, `debit_balance`, `currency`, `currency_multiplier`, and `meta_data`.
//
// Returns:
// - error: If the update operation encounters an error, such as a database failure or if the balance ID is not found, an `APIError` is returned.
func (d Datasource) UpdateBalance(balance *model.Balance) error {
	// Marshal the MetaData into JSON format
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	// Execute the SQL query to update the balance in the database
	result, err := d.Conn.Exec(`
		UPDATE blnk.balances
		SET balance = $2, credit_balance = $3, debit_balance = $4, currency = $5, currency_multiplier = $6, ledger_id = $7, created_at = $8, meta_data = $9
		WHERE balance_id = $1
	`, balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, metaDataJSON)

	// Handle SQL execution errors
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update balance", err)
	}

	// Check if any rows were affected by the update
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	// If no rows were updated, return a not-found error
	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Balance with ID '%s' not found", balance.BalanceID), nil)
	}

	// Return nil indicating a successful update
	return nil
}

// CreateMonitor creates a new BalanceMonitor record in the database.
// This function generates a unique MonitorID for the monitor, sets the creation timestamp,
// and inserts the monitor's data into the `blnk.balance_monitors` table.
//
// Parameters:
//   - monitor: A model.BalanceMonitor object containing details of the monitor to be created.
//     It includes fields like balance ID, field to monitor, operator, value, precision, precise_value, description, callback URL, etc.
//
// Returns:
// - model.BalanceMonitor: The newly created BalanceMonitor object with updated MonitorID and CreatedAt timestamp.
// - error: If any errors occur during the creation process, an `APIError` is returned.
func (d Datasource) CreateMonitor(monitor model.BalanceMonitor) (model.BalanceMonitor, error) {
	// Generate a unique MonitorID and set the current timestamp for CreatedAt
	monitor.MonitorID = model.GenerateUUIDWithSuffix("mon")
	monitor.CreatedAt = time.Now()

	// If PreciseValue is nil, initialize it to 0
	if monitor.Condition.PreciseValue == nil {
		monitor.Condition.PreciseValue = big.NewInt(0)
	}

	// Insert the monitor data into the balance_monitors table
	_, err := d.Conn.Exec(`
		INSERT INTO blnk.balance_monitors (monitor_id, balance_id, field, operator, value, precision, precise_value, description, call_back_url, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, monitor.MonitorID, monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Condition.Precision, monitor.Condition.PreciseValue.String(), monitor.Description, monitor.CallBackURL, monitor.CreatedAt)

	// Handle database errors
	if err != nil {
		pqErr, ok := err.(*pq.Error)
		if ok {
			// Handle unique violation error
			switch pqErr.Code.Name() {
			case "unique_violation":
				return model.BalanceMonitor{}, apierror.NewAPIError(apierror.ErrConflict, "Monitor with this ID already exists", err)
			// Handle foreign key violation error
			case "foreign_key_violation":
				return model.BalanceMonitor{}, apierror.NewAPIError(apierror.ErrBadRequest, "Invalid balance ID", err)
			// Handle other database errors
			default:
				return model.BalanceMonitor{}, apierror.NewAPIError(apierror.ErrInternalServer, "Database error occurred", err)
			}
		}
		// Return a generic internal server error if no specific error is matched
		return model.BalanceMonitor{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to create monitor", err)
	}

	// Return the successfully created BalanceMonitor object
	return monitor, nil
}

// GetMonitorByID retrieves a BalanceMonitor by its unique MonitorID from the database.
// It queries the `blnk.balance_monitors` table and maps the result into a model.BalanceMonitor object.
//
// Parameters:
// - id: The MonitorID of the monitor to retrieve.
//
// Returns:
// - *model.BalanceMonitor: A pointer to the BalanceMonitor object if found.
// - error: If the monitor is not found or if any errors occur during the query, an `APIError` is returned.
func (d Datasource) GetMonitorByID(id string) (*model.BalanceMonitor, error) {
	var preciseValue int64 // Temporary variable to hold the precise value as int64

	// Query the database to get the monitor details by MonitorID
	row := d.Conn.QueryRow(`
		SELECT monitor_id, balance_id, field, operator, value, precision, precise_value, description, call_back_url, created_at
		FROM blnk.balance_monitors WHERE monitor_id = $1
	`, id)

	// Initialize an empty BalanceMonitor object
	monitor := &model.BalanceMonitor{}
	// Initialize an empty AlertCondition object (part of the monitor)
	condition := &model.AlertCondition{}

	// Scan the result into the monitor and condition fields
	err := row.Scan(&monitor.MonitorID, &monitor.BalanceID, &condition.Field, &condition.Operator, &condition.Value, &condition.Precision, &preciseValue, &monitor.Description, &monitor.CallBackURL, &monitor.CreatedAt)
	if err != nil {
		// Handle the case where the monitor with the specified ID is not found
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Monitor with ID '%s' not found", id), err)
		}
		// Return an internal server error for other query issues
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve monitor", err)
	}

	// Populate the PreciseValue field in the condition (convert from int64 to big.Int)
	monitor.Condition = *condition
	monitor.Condition.PreciseValue = big.NewInt(preciseValue)

	// Return the populated BalanceMonitor object
	return monitor, nil
}

// GetAllMonitors retrieves all balance monitors from the database.
// It queries the `blnk.balance_monitors` table and returns a list of all monitors.
//
// Returns:
// - []model.BalanceMonitor: A slice of BalanceMonitor objects if the query is successful.
// - error: If an error occurs during the query or while scanning the result set, an `APIError` is returned.
func (d Datasource) GetAllMonitors() ([]model.BalanceMonitor, error) {
	// Query the database for all balance monitors
	rows, err := d.Conn.Query(`
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at
		FROM blnk.balance_monitors
	`)
	if err != nil {
		// Return an internal server error if the query fails
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve monitors", err)
	}
	defer rows.Close() // Ensure rows are closed after processing

	// Initialize an empty slice to store the retrieved monitors
	var monitors []model.BalanceMonitor

	// Iterate through each row in the result set
	for rows.Next() {
		monitor := model.BalanceMonitor{}   // Create an empty BalanceMonitor object
		condition := model.AlertCondition{} // Create an empty AlertCondition object (part of the monitor)

		// Scan the row into the monitor and condition fields
		err = rows.Scan(&monitor.MonitorID, &monitor.BalanceID, &condition.Field, &condition.Operator, &condition.Value, &monitor.Description, &monitor.CallBackURL, &monitor.CreatedAt)
		if err != nil {
			// Return an error if scanning fails
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan monitor data", err)
		}

		// Assign the scanned AlertCondition to the monitor
		monitor.Condition = condition

		// Append the monitor to the slice
		monitors = append(monitors, monitor)
	}

	// Check for errors encountered during iteration
	if err = rows.Err(); err != nil {
		// Return an error if there were issues iterating over the result set
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over monitors", err)
	}

	// Return the slice of monitors
	return monitors, nil
}

// GetBalanceMonitors retrieves all balance monitors associated with a specific balance ID from the database.
// It queries the `blnk.balance_monitors` table to find all monitors linked to the provided `balanceID`.
//
// Parameters:
// - balanceID: The ID of the balance for which monitors are being retrieved.
//
// Returns:
// - []model.BalanceMonitor: A slice of BalanceMonitor objects associated with the balance ID.
// - error: If an error occurs during the query or while scanning the result set, an `APIError` is returned.
func (d Datasource) GetBalanceMonitors(balanceID string) ([]model.BalanceMonitor, error) {
	// Query the database for monitors associated with the given balance ID
	rows, err := d.Conn.Query(`
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at, precision, precise_value
		FROM blnk.balance_monitors WHERE balance_id = $1
	`, balanceID)
	if err != nil {
		// Return an internal server error if the query fails
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve balance monitors", err)
	}
	defer rows.Close() // Ensure rows are closed after processing

	// Initialize an empty slice to store the retrieved monitors
	var monitors []model.BalanceMonitor

	// Iterate through each row in the result set
	for rows.Next() {
		var preciseValue int64              // Temporary variable to hold the precise value as int64
		monitor := model.BalanceMonitor{}   // Create an empty BalanceMonitor object
		condition := model.AlertCondition{} // Create an empty AlertCondition object (part of the monitor)

		// Scan the row into the monitor and condition fields
		err = rows.Scan(&monitor.MonitorID, &monitor.BalanceID, &condition.Field, &condition.Operator, &condition.Value, &monitor.Description, &monitor.CallBackURL, &monitor.CreatedAt, &condition.Precision, &preciseValue)
		if err != nil {
			// Return an error if scanning fails
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan monitor data", err)
		}

		// Assign the scanned AlertCondition to the monitor
		monitor.Condition = condition
		monitor.Condition.PreciseValue = big.NewInt(preciseValue)

		// Append the monitor to the slice
		monitors = append(monitors, monitor)
	}

	// Check for errors encountered during iteration
	if err = rows.Err(); err != nil {
		// Return an error if there were issues iterating over the result set
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over balance monitors", err)
	}

	// Return the slice of monitors
	return monitors, nil
}

// UpdateMonitor updates an existing balance monitor in the database.
// It updates fields such as `balance_id`, `field`, `operator`, `value`, `description`, and `call_back_url`
// for the monitor identified by `monitor_id`.
//
// Parameters:
// - monitor: A pointer to the `BalanceMonitor` object containing the updated values.
//
// Returns:
// - error: If the update fails, an appropriate `APIError` is returned.
func (d Datasource) UpdateMonitor(monitor *model.BalanceMonitor) error {
	// Execute the SQL update statement, replacing the placeholder values with the monitor's data
	result, err := d.Conn.Exec(`
		UPDATE blnk.balance_monitors
		SET balance_id = $2, field = $3, operator = $4, value = $5, description = $6, call_back_url = $7
		WHERE monitor_id = $1
	`, monitor.MonitorID, monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Description, monitor.CallBackURL)

	// If an error occurred during execution, return an internal server error
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update monitor", err)
	}

	// Check how many rows were affected by the update
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		// If an error occurred while checking rows affected, return an internal server error
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	// If no rows were affected, return a not found error indicating the monitor does not exist
	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Monitor with ID '%s' not found", monitor.MonitorID), nil)
	}

	// Return nil if the update was successful
	return nil
}

// DeleteMonitor deletes a balance monitor from the database by its monitor ID.
// It removes the monitor from the `blnk.balance_monitors` table.
//
// Parameters:
// - id: The ID of the monitor to be deleted.
//
// Returns:
// - error: If the deletion fails or the monitor is not found, an appropriate `APIError` is returned.
func (d Datasource) DeleteMonitor(id string) error {
	// Execute the SQL DELETE statement, removing the monitor by its ID
	result, err := d.Conn.Exec(`
		DELETE FROM blnk.balance_monitors WHERE monitor_id = $1
	`, id)

	// If an error occurred during execution, return an internal server error
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to delete monitor", err)
	}

	// Check how many rows were affected by the delete operation
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		// If an error occurred while checking rows affected, return an internal server error
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	// If no rows were affected, return a not found error indicating the monitor does not exist
	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Monitor with ID '%s' not found", id), nil)
	}

	// Return nil if the deletion was successful
	return nil
}
