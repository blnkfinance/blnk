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

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
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

// parseBigInt parses a string into a *big.Int, returning an error if parsing fails.
// This ensures we don't silently get nil values when database returns malformed data.
func parseBigInt(s string) (*big.Int, error) {
	n, ok := new(big.Int).SetString(s, 10)
	if !ok {
		return nil, fmt.Errorf("invalid big.Int value: %q", s)
	}
	return n, nil
}

// Prepares a dynamic SQL query based on the fields to be included.
// This query fetches balance details, including optional joins for identity and ledger.
func prepareQueries(queryBuilder strings.Builder, include []string) string {
	var selectFields []string

	// Default fields for balances
	selectFields = append(selectFields,
		"b.balance_id", "b.balance", "b.credit_balance", "b.debit_balance",
		"b.currency", "b.currency_multiplier", "b.ledger_id",
		"COALESCE(b.identity_id, '') as identity_id", "b.created_at", "b.meta_data", "b.inflight_balance", "b.inflight_credit_balance", "b.inflight_debit_balance", "b.version", "b.indicator", "b.track_fund_lineage", "COALESCE(b.allocation_strategy, 'FIFO') as allocation_strategy")

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
		&inflightBalanceStr, &inflightCreditBalanceStr, &inflightDebitBalanceStr, &balance.Version, &indicator, &balance.TrackFundLineage, &balance.AllocationStrategy)

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
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return nil, fmt.Errorf("scanRow: failed to rollback transaction: %v, original error: %v", rollbackErr, err)
		}
		return nil, err
	}

	// Convert string representations to big.Int
	balance.Balance, err = parseBigInt(balanceStr)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to parse balance: %w", err)
	}
	balance.CreditBalance, err = parseBigInt(creditBalanceStr)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to parse credit_balance: %w", err)
	}
	balance.DebitBalance, err = parseBigInt(debitBalanceStr)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to parse debit_balance: %w", err)
	}
	balance.InflightBalance, err = parseBigInt(inflightBalanceStr)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to parse inflight_balance: %w", err)
	}
	balance.InflightCreditBalance, err = parseBigInt(inflightCreditBalanceStr)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to parse inflight_credit_balance: %w", err)
	}
	balance.InflightDebitBalance, err = parseBigInt(inflightDebitBalanceStr)
	if err != nil {
		_ = tx.Rollback()
		return nil, fmt.Errorf("failed to parse inflight_debit_balance: %w", err)
	}

	// Handle null indicator field
	if indicator.Valid {
		balance.Indicator = indicator.String
	} else {
		balance.Indicator = ""
	}

	// Unmarshal metadata JSON
	err = json.Unmarshal(metaDataJSON, &balance.MetaData)
	if err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			return nil, fmt.Errorf("scanRow: failed to rollback transaction: %v, original error: %v", rollbackErr, err)
		}
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

	// Default allocation strategy to FIFO if not set
	allocationStrategy := balance.AllocationStrategy
	if allocationStrategy == "" {
		allocationStrategy = "FIFO"
	}

	// Insert the balance into the database
	_, err = d.Conn.Exec(`
		INSERT INTO blnk.balances (balance_id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, identity_id, indicator, created_at, meta_data, track_fund_lineage, allocation_strategy)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
	`, balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, identityID, indicator, balance.CreatedAt, &metaDataJSON, balance.TrackFundLineage, allocationStrategy)
	if err != nil {
		// Handle specific PostgreSQL errors (e.g., unique or foreign key violations)
		pqErr, ok := err.(*pq.Error)
		if ok {
			switch pqErr.Code.Name() {
			case "unique_violation":
				if strings.Contains(pqErr.Message, "unique_indicator_currency") {
					return model.Balance{}, nil
				}
				return model.Balance{}, apierror.NewAPIError(apierror.ErrConflict, fmt.Sprintf("Balance already exists: %s", balance.BalanceID), err)
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
// - withQueued: A boolean that specifies whether to include queued amounts in the result.
//
// Returns:
// - *model.Balance: A pointer to the retrieved Balance object.
// - error: Returns an APIError in case of errors such as database failures or if the balance is not found.
func (d Datasource) GetBalanceByID(id string, include []string, withQueued bool) (*model.Balance, error) {
	// Set a context with a 1-minute timeout
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	// Start a transaction
	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				err = fmt.Errorf("GetBalanceByID: failed to rollback transaction: %v, original error: %v", rollbackErr, err)
			}
		}
	}()

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

	// Get queued amounts only if requested
	if withQueued {
		queuedDebit, queuedCredit, err := d.GetQueuedAmounts(context.Background(), id)
		if err != nil {
			return nil, err
		}
		balance.QueuedDebitBalance = queuedDebit
		balance.QueuedCreditBalance = queuedCredit
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
	// Use string variables instead of int64 to handle any size
	var balanceValue, creditBalanceValue, debitBalanceValue string
	var inflightBalanceValue, inflightCreditBalanceValue, inflightDebitBalanceValue string
	var indicator sql.NullString
	var allocationStrategy sql.NullString

	// Execute the query
	row := d.Conn.QueryRow(`
       SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version, track_fund_lineage, COALESCE(allocation_strategy, 'FIFO') as allocation_strategy, COALESCE(identity_id, '') as identity_id
       FROM blnk.balances
       WHERE balance_id = $1
    `, id)

	// Scan the result into variables
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
		&balance.TrackFundLineage,
		&allocationStrategy,
		&balance.IdentityID,
	)

	// Handle null indicator field
	if indicator.Valid {
		balance.Indicator = indicator.String
	} else {
		balance.Indicator = ""
	}

	// Handle null allocation_strategy field
	if allocationStrategy.Valid {
		balance.AllocationStrategy = allocationStrategy.String
	} else {
		balance.AllocationStrategy = "FIFO"
	}

	if err != nil {
		logrus.Errorf("balance lite error %v", err)
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Balance with ID '%s' not found", id), err)
		} else {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan balance data", err)
		}
	}

	// Parse string values to big.Int
	balance.Balance, err = parseBigInt(balanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse balance: %w", err)
	}
	balance.CreditBalance, err = parseBigInt(creditBalanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse credit_balance: %w", err)
	}
	balance.DebitBalance, err = parseBigInt(debitBalanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse debit_balance: %w", err)
	}
	balance.InflightBalance, err = parseBigInt(inflightBalanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inflight_balance: %w", err)
	}
	balance.InflightCreditBalance, err = parseBigInt(inflightCreditBalanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inflight_credit_balance: %w", err)
	}
	balance.InflightDebitBalance, err = parseBigInt(inflightDebitBalanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inflight_debit_balance: %w", err)
	}

	return &balance, nil
}

// GetBalancesByIDsLite retrieves multiple balances by their IDs in a single query.
// Returns a map of balance_id to Balance for easy lookup.
// Balances that are not found are simply not included in the result map.
//
// Parameters:
// - ctx context.Context: The context for the operation.
// - ids []string: The list of balance IDs to retrieve.
//
// Returns:
// - map[string]*model.Balance: A map of balance_id to Balance.
// - error: Returns an error in case of database failures.
func (d Datasource) GetBalancesByIDsLite(ctx context.Context, ids []string) (map[string]*model.Balance, error) {
	if len(ids) == 0 {
		return make(map[string]*model.Balance), nil
	}

	rows, err := d.Conn.QueryContext(ctx, `
		SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version, track_fund_lineage, COALESCE(allocation_strategy, 'FIFO') as allocation_strategy, COALESCE(identity_id, '') as identity_id
		FROM blnk.balances
		WHERE balance_id = ANY($1)
	`, pq.Array(ids))
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to query balances", err)
	}
	defer func() { _ = rows.Close() }()

	result := make(map[string]*model.Balance)

	for rows.Next() {
		var balance model.Balance
		var balanceValue, creditBalanceValue, debitBalanceValue string
		var inflightBalanceValue, inflightCreditBalanceValue, inflightDebitBalanceValue string
		var indicator sql.NullString
		var allocationStrategy sql.NullString

		err := rows.Scan(
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
			&balance.TrackFundLineage,
			&allocationStrategy,
			&balance.IdentityID,
		)
		if err != nil {
			logrus.Errorf("balance batch scan error: %v", err)
			continue
		}

		if indicator.Valid {
			balance.Indicator = indicator.String
		}
		if allocationStrategy.Valid {
			balance.AllocationStrategy = allocationStrategy.String
		} else {
			balance.AllocationStrategy = "FIFO"
		}

		// Parse big.Int values
		var parseErr error
		balance.Balance, parseErr = parseBigInt(balanceValue)
		if parseErr != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, fmt.Sprintf("balance %s: failed to parse balance", balance.BalanceID), parseErr)
		}
		balance.CreditBalance, parseErr = parseBigInt(creditBalanceValue)
		if parseErr != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, fmt.Sprintf("balance %s: failed to parse credit_balance", balance.BalanceID), parseErr)
		}
		balance.DebitBalance, parseErr = parseBigInt(debitBalanceValue)
		if parseErr != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, fmt.Sprintf("balance %s: failed to parse debit_balance", balance.BalanceID), parseErr)
		}
		balance.InflightBalance, parseErr = parseBigInt(inflightBalanceValue)
		if parseErr != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, fmt.Sprintf("balance %s: failed to parse inflight_balance", balance.BalanceID), parseErr)
		}
		balance.InflightCreditBalance, parseErr = parseBigInt(inflightCreditBalanceValue)
		if parseErr != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, fmt.Sprintf("balance %s: failed to parse inflight_credit_balance", balance.BalanceID), parseErr)
		}
		balance.InflightDebitBalance, parseErr = parseBigInt(inflightDebitBalanceValue)
		if parseErr != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, fmt.Sprintf("balance %s: failed to parse inflight_debit_balance", balance.BalanceID), parseErr)
		}

		result[balance.BalanceID] = &balance
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error iterating over balances", err)
	}

	return result, nil
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
	// Change to string variables to handle large numbers
	var balanceValue, creditBalanceValue, debitBalanceValue string
	var inflightBalanceValue, inflightCreditBalanceValue, inflightDebitBalanceValue string
	var allocationStrategy sql.NullString

	// Execute query to find the balance with the given indicator and currency
	row := d.Conn.QueryRow(`
       SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version, track_fund_lineage, COALESCE(allocation_strategy, 'FIFO') as allocation_strategy, COALESCE(identity_id, '') as identity_id
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
		&balance.TrackFundLineage,
		&allocationStrategy,
		&balance.IdentityID,
	)
	if err != nil {
		if err == sql.ErrNoRows {
			// Handle the case where no balance was found with the given indicator and currency
			return &model.Balance{}, fmt.Errorf("balance with indicator '%s' not found", indicator)
		}
		// Return other types of errors, such as query execution failures
		return nil, err
	}

	// Handle null allocation_strategy field
	if allocationStrategy.Valid {
		balance.AllocationStrategy = allocationStrategy.String
	} else {
		balance.AllocationStrategy = "FIFO"
	}

	// Initialize big.Int values
	balance.Balance = new(big.Int)
	balance.CreditBalance = new(big.Int)
	balance.DebitBalance = new(big.Int)
	balance.InflightBalance = new(big.Int)
	balance.InflightCreditBalance = new(big.Int)
	balance.InflightDebitBalance = new(big.Int)

	// Parse string values to big.Int
	balance.Balance, err = parseBigInt(balanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse balance: %w", err)
	}
	balance.CreditBalance, err = parseBigInt(creditBalanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse credit_balance: %w", err)
	}
	balance.DebitBalance, err = parseBigInt(debitBalanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse debit_balance: %w", err)
	}
	balance.InflightBalance, err = parseBigInt(inflightBalanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inflight_balance: %w", err)
	}
	balance.InflightCreditBalance, err = parseBigInt(inflightCreditBalanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inflight_credit_balance: %w", err)
	}
	balance.InflightDebitBalance, err = parseBigInt(inflightDebitBalanceValue)
	if err != nil {
		return nil, fmt.Errorf("failed to parse inflight_debit_balance: %w", err)
	}

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
	rows, err := d.Conn.Query(`
        SELECT balance_id, indicator, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, currency, currency_multiplier, ledger_id, COALESCE(identity_id, '') as identity_id, created_at, meta_data
        FROM blnk.balances
        ORDER BY created_at DESC
        LIMIT $1 OFFSET $2
    `, limit, offset)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logrus.Error(err)
		}
	}(rows)

	var balances []model.Balance
	var balanceValue, creditBalanceValue, debitBalanceValue string
	var inflightBalanceValue, inflightCreditBalanceValue, inflightDebitBalanceValue string

	for rows.Next() {
		balance := model.Balance{}
		var metaDataJSON []byte

		err = rows.Scan(
			&balance.BalanceID,
			&indicator,
			&balanceValue,
			&creditBalanceValue,
			&debitBalanceValue,
			&inflightBalanceValue,
			&inflightCreditBalanceValue,
			&inflightDebitBalanceValue,
			&balance.Currency,
			&balance.CurrencyMultiplier,
			&balance.LedgerID,
			&balance.IdentityID,
			&balance.CreatedAt,
			&metaDataJSON,
		)
		if err != nil {
			return nil, err
		}

		if indicator.Valid {
			balance.Indicator = indicator.String
		} else {
			balance.Indicator = ""
		}

		balance.Balance, err = parseBigInt(balanceValue)
		if err != nil {
			return nil, fmt.Errorf("failed to parse balance: %w", err)
		}
		balance.CreditBalance, err = parseBigInt(creditBalanceValue)
		if err != nil {
			return nil, fmt.Errorf("failed to parse credit_balance: %w", err)
		}
		balance.DebitBalance, err = parseBigInt(debitBalanceValue)
		if err != nil {
			return nil, fmt.Errorf("failed to parse debit_balance: %w", err)
		}
		balance.InflightBalance, err = parseBigInt(inflightBalanceValue)
		if err != nil {
			return nil, fmt.Errorf("failed to parse inflight_balance: %w", err)
		}
		balance.InflightCreditBalance, err = parseBigInt(inflightCreditBalanceValue)
		if err != nil {
			return nil, fmt.Errorf("failed to parse inflight_credit_balance: %w", err)
		}
		balance.InflightDebitBalance, err = parseBigInt(inflightDebitBalanceValue)
		if err != nil {
			return nil, fmt.Errorf("failed to parse inflight_debit_balance: %w", err)
		}

		err = json.Unmarshal(metaDataJSON, &balance.MetaData)
		if err != nil {
			return nil, err
		}

		balances = append(balances, balance)
	}

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
	// SQL query to update the balance
	query := `
        UPDATE blnk.balances
        SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, version = version + 1
        WHERE balance_id = $1 AND version = $12
    `

	// Execute the update query within the provided transaction context
	result, err := tx.ExecContext(ctx, query, balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.InflightBalance.String(), balance.InflightCreditBalance.String(), balance.InflightDebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, balance.Version)
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
	defer func() { _ = rows.Close() }() // Ensure rows are closed after processing

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
	defer func() { _ = rows.Close() }() // Ensure rows are closed after processing

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

// TakeBalanceSnapshots creates daily snapshots of balances in batches.
// It uses the PostgreSQL function to process balances in chunks to avoid memory issues
// with large datasets.
//
// Parameters:
// - ctx: Context for the operation, allowing for timeouts and cancellation
// - batchSize: The number of balances to process in each batch
//
// Returns:
// - int: The total number of snapshots created
// - error: Returns an APIError if the operation fails
func (d Datasource) TakeBalanceSnapshots(ctx context.Context, batchSize int) (int, error) {
	var totalProcessed int

	// Call the PostgreSQL function to take snapshots in batches
	err := d.Conn.QueryRowContext(ctx, `
        SELECT blnk.take_daily_balance_snapshots_batched($1)
    `, batchSize).Scan(&totalProcessed)
	if err != nil {
		return 0, apierror.NewAPIError(
			apierror.ErrInternalServer,
			"Failed to take balance snapshots",
			err,
		)
	}

	return totalProcessed, nil
}

// validateBalanceTimeParams validates the input parameters for GetBalanceAtTime
func validateBalanceTimeParams(balanceID string, targetTime time.Time) error {
	if balanceID == "" {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Balance ID cannot be empty", nil)
	}

	if targetTime.IsZero() {
		return apierror.NewAPIError(apierror.ErrBadRequest, "Target time cannot be zero", nil)
	}

	return nil
}

// getBalanceInfo retrieves basic information about a balance
func (d Datasource) getBalanceInfo(ctx context.Context, tx *sql.Tx, balanceID string) (currency string, createdAt time.Time, err error) {
	err = tx.QueryRowContext(ctx, `
		SELECT currency, created_at FROM blnk.balances WHERE balance_id = $1
	`, balanceID).Scan(&currency, &createdAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return "", time.Time{}, apierror.NewAPIError(
				apierror.ErrNotFound,
				fmt.Sprintf("Balance '%s' not found", balanceID),
				err,
			)
		}
		return "", time.Time{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get balance information", err)
	}

	return currency, createdAt, nil
}

// getMostRecentSnapshot finds the most recent balance snapshot before the target time
func (d Datasource) getMostRecentSnapshot(ctx context.Context, tx *sql.Tx, balanceID string, targetTime time.Time) (creditBalance, debitBalance *big.Int, snapshotTime time.Time, err error) {
	snapshot := tx.QueryRowContext(ctx, `
		SELECT 
			balance,
			credit_balance,
			debit_balance,
			snapshot_time
		FROM blnk.balance_snapshots
		WHERE balance_id = $1
		AND snapshot_time <= $2
		ORDER BY snapshot_time DESC
		LIMIT 1
	`, balanceID, targetTime)

	var snapshotBalance, snapshotCredit, snapshotDebit string

	err = snapshot.Scan(&snapshotBalance, &snapshotCredit, &snapshotDebit, &snapshotTime)

	if err == nil {
		// Snapshot found, use it as starting point
		creditBalance, err = parseBigInt(snapshotCredit)
		if err != nil {
			return nil, nil, time.Time{}, fmt.Errorf("failed to parse snapshot credit_balance: %w", err)
		}
		debitBalance, err = parseBigInt(snapshotDebit)
		if err != nil {
			return nil, nil, time.Time{}, fmt.Errorf("failed to parse snapshot debit_balance: %w", err)
		}
		logrus.Debugf("Found snapshot for balance %s at %v with credit=%s, debit=%s",
			balanceID, snapshotTime, snapshotCredit, snapshotDebit)
		return creditBalance, debitBalance, snapshotTime, nil
	} else if err == sql.ErrNoRows {
		// No snapshot found, calculate from genesis (all transactions)
		logrus.Debugf("No snapshot found for balance %s, calculating from genesis", balanceID)
		return new(big.Int).SetInt64(0), new(big.Int).SetInt64(0), time.Time{}, nil
	}

	// Other error occurred
	return nil, nil, time.Time{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get balance snapshot", err)
}

// fetchTransactions retrieves transactions for a balance within a specific time range
// using effective_date if available, otherwise falling back to created_at
func fetchTransactions(ctx context.Context, tx *sql.Tx, balanceID string, startTime, targetTime time.Time) (*sql.Rows, error) {
	logrus.Debugf("Querying transactions from %v to %v for balance %s", startTime, targetTime, balanceID)
	rows, err := tx.QueryContext(ctx, `
        SELECT precise_amount, source, destination, created_at, 
               COALESCE(effective_date, created_at) as effective_date
        FROM blnk.transactions
        WHERE (source = $1 OR destination = $1)
        AND COALESCE(effective_date, created_at) > $2
        AND COALESCE(effective_date, created_at) <= $3
        AND status = 'APPLIED'
        ORDER BY COALESCE(effective_date, created_at) ASC
    `, balanceID, startTime, targetTime)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get transactions", err)
	}

	return rows, nil
}

// applyTransaction applies a single transaction to update balance totals
func applyTransaction(txn struct {
	PreciseAmount string
	Source        string
	Destination   string
	CreatedAt     time.Time
	EffectiveDate time.Time
}, balanceID string, creditBalance, debitBalance *big.Int,
) (*big.Int, *big.Int, error) {
	txAmount, ok := new(big.Int).SetString(txn.PreciseAmount, 10)
	if !ok {
		return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Invalid transaction amount", nil)
	}

	// Use the transaction amount to update the appropriate balance
	if txn.Source == balanceID {
		debitBalance = new(big.Int).Add(debitBalance, txAmount)
	}
	if txn.Destination == balanceID {
		creditBalance = new(big.Int).Add(creditBalance, txAmount)
	}

	return creditBalance, debitBalance, nil
}

// calculateBalanceFromTransactions applies transactions to calculate the balance at a specific time
func (d Datasource) calculateBalanceFromTransactions(ctx context.Context, tx *sql.Tx, balanceID string, startTime time.Time, targetTime time.Time, initialCredit, initialDebit *big.Int) (creditBalance, debitBalance *big.Int, err error) {
	creditBalance = new(big.Int).Set(initialCredit)
	debitBalance = new(big.Int).Set(initialDebit)

	// Fetch relevant transactions
	rows, err := fetchTransactions(ctx, tx, balanceID, startTime, targetTime)
	if err != nil {
		return nil, nil, err
	}
	defer func() { _ = rows.Close() }()

	// Count of processed transactions for debugging
	transactionCount := 0

	// Apply transactions
	for rows.Next() {
		var txn struct {
			PreciseAmount string
			Source        string
			Destination   string
			CreatedAt     time.Time
			EffectiveDate time.Time
		}

		if err := rows.Scan(&txn.PreciseAmount, &txn.Source, &txn.Destination, &txn.CreatedAt, &txn.EffectiveDate); err != nil {
			return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan transaction", err)
		}

		// Apply this transaction to update balances
		creditBalance, debitBalance, err = applyTransaction(txn, balanceID, creditBalance, debitBalance)
		if err != nil {
			return nil, nil, err
		}

		transactionCount++
	}

	logrus.Debugf("Processed %d transactions for balance %s", transactionCount, balanceID)

	if err = rows.Err(); err != nil {
		return nil, nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error processing transactions", err)
	}

	return creditBalance, debitBalance, nil
}

// GetBalanceAtTime retrieves the balance state at a specific point in time.
// It finds the most recent snapshot before the target time and applies any subsequent
// transactions to calculate the exact balance state. If fromSource is true, it skips
// using snapshots and calculates directly from all transactions.
//
// Parameters:
// - ctx: Context for the database operations
// - balanceID: The ID of the balance to query
// - targetTime: The point in time for which to get the balance state
// - fromSource: If true, calculation is done from all transactions rather than using snapshots
//
// Returns:
// - *Balance: The calculated balance state at the target time
// - error: An APIError if any issues occur during the operation
func (d Datasource) GetBalanceAtTime(ctx context.Context, balanceID string, targetTime time.Time, fromSource bool) (*model.Balance, error) {
	// Add context timeout
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	// Validate inputs
	if err := validateBalanceTimeParams(balanceID, targetTime); err != nil {
		return nil, err
	}

	// Start transaction
	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{
		ReadOnly:  true,
		Isolation: sql.LevelRepeatableRead,
	})
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to start transaction", err)
	}
	defer func() {
		if err != nil {
			if rollbackErr := tx.Rollback(); rollbackErr != nil {
				logrus.Errorf("GetBalanceAtTime: failed to rollback transaction: %v, original error: %v", rollbackErr, err)
			}
		}
	}()

	// Get basic balance information
	currency, balanceCreatedAt, err := d.getBalanceInfo(ctx, tx, balanceID)
	if err != nil {
		return nil, err
	}

	var creditBalance, debitBalance *big.Int
	var startTime time.Time

	if fromSource {
		// Skip snapshots and start from zero
		logrus.Debugf("Skipping snapshots for balance %s as requested, calculating from genesis", balanceID)
		creditBalance = new(big.Int).SetInt64(0)
		debitBalance = new(big.Int).SetInt64(0)
		startTime = time.Time{} // Use zero time to get all transactions
	} else {
		// Try to find the most recent snapshot
		creditBalance, debitBalance, startTime, err = d.getMostRecentSnapshot(ctx, tx, balanceID, targetTime)
		if err != nil {
			return nil, err
		}
	}

	// Calculate the balance by applying transactions since the snapshot (or from genesis)
	creditBalance, debitBalance, err = d.calculateBalanceFromTransactions(
		ctx, tx, balanceID, startTime, targetTime, creditBalance, debitBalance)
	if err != nil {
		return nil, err
	}

	// Calculate final balance
	balance := new(big.Int).Sub(creditBalance, debitBalance)

	logrus.Debugf("Final calculated balance for %s at %v: credit=%s, debit=%s, balance=%s",
		balanceID, targetTime, creditBalance.String(), debitBalance.String(), balance.String())

	// Commit transaction
	if err := tx.Commit(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	// Construct result
	result := &model.Balance{
		BalanceID:     balanceID,
		Balance:       balance,
		CreditBalance: creditBalance,
		DebitBalance:  debitBalance,
		Currency:      currency,
		CreatedAt:     balanceCreatedAt,
	}

	return result, nil
}

// UpdateBalanceIdentity updates the identity_id of a balance entry in the database.
//
// Parameters:
// - balanceID: The unique identifier of the balance whose identity reference is to be updated.
// - identityID: The identity ID to be associated with the balance.
//
// Returns:
// - error: An error is returned if the balance or identity does not exist or the database operation fails.
func (d Datasource) UpdateBalanceIdentity(balanceID string, identityID string) error {
	// Execute the SQL update statement to change the identity_id for the specified balance.
	result, err := d.Conn.Exec(`
		UPDATE blnk.balances
		SET identity_id = $2
		WHERE balance_id = $1
	`, balanceID, identityID)
	// Handle SQL execution errors
	if err != nil {
		// Delegate to apierror for consistent error handling across the project
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update balance identity", err)
	}

	// Ensure a row was actually updated
	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		// No rows were updated – the balance record does not exist
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Balance with ID '%s' not found", balanceID), nil)
	}

	return nil
}
