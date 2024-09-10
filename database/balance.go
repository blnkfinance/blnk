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

func contains(slice []string, val string) bool {
	for _, s := range slice {
		if s == val {
			return true
		}
	}
	return false
}

func prepareQueries(queryBuilder strings.Builder, include []string) string {
	var selectFields []string
	// Default fields for balances
	selectFields = append(selectFields,
		"b.balance_id", "b.balance", "b.credit_balance", "b.debit_balance",
		"b.currency", "b.currency_multiplier", "b.ledger_id",
		"COALESCE(b.identity_id, '') as identity_id", "b.created_at", "b.meta_data", "b.inflight_balance", "b.inflight_credit_balance", "b.inflight_debit_balance", "b.version")

	// Append fields and joins based on 'include'
	if contains(include, "identity") {
		selectFields = append(selectFields,
			"i.identity_id", "i.first_name", "i_name", "i.category", "i.last_name", "i.other_names",
			"i.gender", "i.dob", "i.email_address", "i.phone_number",
			"i.nationality", "i.street", "i.country", "i.state",
			"i.post_code", "i.city", "i.created_at")
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
            SELECT * FROM blnk.balances WHERE balance_id = $1
        ) AS b
    `)

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

func scanRow(row *sql.Row, tx *sql.Tx, include []string) (*model.Balance, error) {
	balance := &model.Balance{}
	identity := &model.Identity{}
	ledger := &model.Ledger{}
	metaDataJSON := []byte{}

	// Temporary variables to hold string representations of big.Int fields
	var balanceStr, creditBalanceStr, debitBalanceStr, inflightBalanceStr, inflightCreditBalanceStr, inflightDebitBalanceStr string

	var scanArgs []interface{}
	// Add scan arguments for default fields
	scanArgs = append(scanArgs, &balance.BalanceID, &balanceStr, &creditBalanceStr,
		&debitBalanceStr, &balance.Currency, &balance.CurrencyMultiplier,
		&balance.LedgerID, &balance.IdentityID, &balance.CreatedAt, &metaDataJSON,
		&inflightBalanceStr, &inflightCreditBalanceStr, &inflightDebitBalanceStr, &balance.Version)

	if contains(include, "identity") {
		scanArgs = append(scanArgs, &identity.IdentityID, &identity.FirstName, &identity.OrganizationName, &identity.Category, &identity.LastName,
			&identity.OtherNames, &identity.Gender, &identity.DOB, &identity.EmailAddress,
			&identity.PhoneNumber, &identity.Nationality, &identity.Street, &identity.Country,
			&identity.State, &identity.PostCode, &identity.City, &identity.CreatedAt)
	}

	if contains(include, "ledger") {
		scanArgs = append(scanArgs, &ledger.LedgerID, &ledger.Name, &ledger.CreatedAt)
	}

	err := row.Scan(scanArgs...)
	if err != nil {
		fmt.Println("Error: ", err)
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

	err = json.Unmarshal(metaDataJSON, &balance.MetaData)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	if contains(include, "identity") {
		balance.Identity = identity
	}
	if contains(include, "ledger") {
		balance.Ledger = ledger
	}

	return balance, nil
}

// CreateBalance inserts a new Balance into the database
func (d Datasource) CreateBalance(balance model.Balance) (model.Balance, error) {
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return model.Balance{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	balance.BalanceID = model.GenerateUUIDWithSuffix("bln")
	balance.CreatedAt = time.Now()

	var identityID interface{} = balance.IdentityID
	if balance.IdentityID == "" {
		identityID = nil
	}

	var indicator interface{} = balance.Indicator
	if balance.Indicator == "" {
		indicator = nil
	}

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
	_, err = d.Conn.Exec(`
		INSERT INTO blnk.balances (balance_id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, identity_id, indicator, created_at, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10,$11)
	`, balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, identityID, indicator, balance.CreatedAt, &metaDataJSON)

	if err != nil {
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
		return model.Balance{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to create balance", err)
	}

	return balance, nil
}

func (d Datasource) GetBalanceByID(id string, include []string) (*model.Balance, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	tx, err := d.Conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	var queryBuilder strings.Builder
	query := prepareQueries(queryBuilder, include)
	row := tx.QueryRow(query, id)
	balance, err := scanRow(row, tx, include)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Balance with ID '%s' not found", id), err)
		} else {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan balance data", err)
		}
	}
	err = tx.Commit()
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	return balance, nil
}

func (d Datasource) GetBalanceByIDLite(id string) (*model.Balance, error) {
	var balance model.Balance
	var balanceValue, creditBalanceValue, debitBalanceValue, inflightBalanceValue, inflightCreditBalanceValue, inflightDebitBalanceValue int64
	var indicator sql.NullString

	row := d.Conn.QueryRow(`
	   SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version 
	   FROM blnk.balances 
	   WHERE balance_id = $1
	`, id)

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

	// Convert sql.NullString to string
	if indicator.Valid {
		balance.Indicator = indicator.String
	} else {
		balance.Indicator = "" // or set to a default value
	}

	if err != nil {
		logrus.Errorf("balance lite error %v", err)
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Balance with ID '%s' not found", id), err)
		} else {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan balance data", err)
		}
	}

	// Convert the scanned int64 values into big.Int
	balance.Balance = big.NewInt(balanceValue)
	balance.CreditBalance = big.NewInt(creditBalanceValue)
	balance.DebitBalance = big.NewInt(debitBalanceValue)
	balance.InflightBalance = big.NewInt(inflightBalanceValue)
	balance.InflightCreditBalance = big.NewInt(inflightCreditBalanceValue)
	balance.InflightDebitBalance = big.NewInt(inflightDebitBalanceValue)

	return &balance, nil
}

func (d Datasource) GetBalanceByIndicator(indicator, currency string) (*model.Balance, error) {
	var balance model.Balance
	var balanceValue, creditBalanceValue, debitBalanceValue, inflightBalanceValue, inflightCreditBalanceValue, inflightDebitBalanceValue int64

	row := d.Conn.QueryRow(`
	   SELECT balance_id, indicator, currency, currency_multiplier, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version 
	   FROM blnk.balances 
	   WHERE indicator = $1 AND currency = $2
	`, indicator, currency)

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
			return &model.Balance{}, fmt.Errorf("balance with indicator '%s' not found", indicator)
		} else {
			return nil, err
		}
	}

	// Convert the scanned int64 values into big.Int
	balance.Balance = big.NewInt(balanceValue)
	balance.CreditBalance = big.NewInt(creditBalanceValue)
	balance.DebitBalance = big.NewInt(debitBalanceValue)
	balance.InflightBalance = big.NewInt(inflightBalanceValue)
	balance.InflightCreditBalance = big.NewInt(inflightCreditBalanceValue)
	balance.InflightDebitBalance = big.NewInt(inflightDebitBalanceValue)

	return &balance, nil
}

// GetAllBalances retrieves all balances from the database
func (d Datasource) GetAllBalances() ([]model.Balance, error) {
	// select all balances from database
	rows, err := d.Conn.Query(`
		SELECT balance_id, balance, credit_balance, debit_balance, currency, currency_multiplier, ledger_id, created_at, meta_data
		FROM blnk.balances
		LIMIT 20
	`)
	if err != nil {
		return nil, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logrus.Error(err)
		}
	}(rows)

	// create slice to store balances
	var balances []model.Balance
	var balanceValue, creditBalanceValue, debitBalanceValue, inflightBalanceValue, inflightCreditBalanceValue, inflightDebitBalanceValue int64

	// iterate through result set and parse metadata from JSON
	for rows.Next() {
		balance := model.Balance{}
		var metaDataJSON []byte
		err = rows.Scan(
			&balance.BalanceID,
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
			return nil, err
		}

		// Convert the scanned int64 values into big.Int
		balance.Balance = big.NewInt(balanceValue)
		balance.CreditBalance = big.NewInt(creditBalanceValue)
		balance.DebitBalance = big.NewInt(debitBalanceValue)
		balance.InflightBalance = big.NewInt(inflightBalanceValue)
		balance.InflightCreditBalance = big.NewInt(inflightCreditBalanceValue)
		balance.InflightDebitBalance = big.NewInt(inflightDebitBalanceValue)

		// convert metadata from JSON to map
		err = json.Unmarshal(metaDataJSON, &balance.MetaData)
		if err != nil {
			return nil, err
		}

		balances = append(balances, balance)
	}

	return balances, nil
}

func (d Datasource) GetSourceDestination(sourceId, destinationId string) ([]*model.Balance, error) {
	// select all balances from database
	rows, err := d.Conn.Query(`
		SELECT blnk.get_balances_by_id($1,$2)
	`, sourceId, destinationId)
	if err != nil {

		return nil, err
	}
	defer func(rows *sql.Rows) {
		err := rows.Close()
		if err != nil {
			logrus.Error(err)
		}
	}(rows)

	// create slice to store balances
	var balances []*model.Balance

	// iterate through result set and parse metadata from JSON
	for rows.Next() {
		balance := model.Balance{}
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

		balances = append(balances, &balance)
	}

	return balances, nil
}

func (d Datasource) UpdateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	tx, err := d.Conn.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelDefault})
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to begin transaction", err)
	}

	defer func(tx *sql.Tx) {
		_ = tx.Rollback()
	}(tx)

	if err := updateBalance(ctx, tx, sourceBalance); err != nil {
		return err
	}

	if err := updateBalance(ctx, tx, destinationBalance); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to commit transaction", err)
	}

	return nil
}

func updateBalance(ctx context.Context, tx *sql.Tx, balance *model.Balance) error {
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	query := `
        UPDATE blnk.balances
        SET balance = $2, credit_balance = $3, debit_balance = $4, inflight_balance = $5, inflight_credit_balance = $6, inflight_debit_balance = $7, currency = $8, currency_multiplier = $9, ledger_id = $10, created_at = $11, meta_data = $12, version = version + 1
        WHERE balance_id = $1 AND version = $13
    `

	result, err := tx.ExecContext(ctx, query, balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.InflightBalance.String(), balance.InflightCreditBalance.String(), balance.InflightDebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, metaDataJSON, balance.Version)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update balance", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrConflict, fmt.Sprintf("Optimistic locking failure: balance with ID '%s' may have been updated or deleted by another transaction", balance.BalanceID), nil)
	}

	balance.Version++

	return nil
}

// UpdateBalance updates a balance in the database
func (d Datasource) UpdateBalance(balance *model.Balance) error {
	metaDataJSON, err := json.Marshal(balance.MetaData)
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	result, err := d.Conn.Exec(`
		UPDATE blnk.balances
		SET balance = $2, credit_balance = $3, debit_balance = $4, currency = $5, currency_multiplier = $6, ledger_id = $7, created_at = $8, meta_data = $9
		WHERE balance_id = $1
	`, balance.BalanceID, balance.Balance.String(), balance.CreditBalance.String(), balance.DebitBalance.String(), balance.Currency, balance.CurrencyMultiplier, balance.LedgerID, balance.CreatedAt, metaDataJSON)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update balance", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Balance with ID '%s' not found", balance.BalanceID), nil)
	}

	return nil
}

func (d Datasource) CreateMonitor(monitor model.BalanceMonitor) (model.BalanceMonitor, error) {
	monitor.MonitorID = model.GenerateUUIDWithSuffix("mon")
	monitor.CreatedAt = time.Now()

	if monitor.Condition.PreciseValue == nil {
		monitor.Condition.PreciseValue = big.NewInt(0)
	}

	_, err := d.Conn.Exec(`
		INSERT INTO blnk.balance_monitors (monitor_id, balance_id, field, operator, value, precision, precise_value, description, call_back_url, created_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
	`, monitor.MonitorID, monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Condition.Precision, monitor.Condition.PreciseValue.String(), monitor.Description, monitor.CallBackURL, monitor.CreatedAt)

	if err != nil {
		pqErr, ok := err.(*pq.Error)
		if ok {
			switch pqErr.Code.Name() {
			case "unique_violation":
				return model.BalanceMonitor{}, apierror.NewAPIError(apierror.ErrConflict, "Monitor with this ID already exists", err)
			case "foreign_key_violation":
				return model.BalanceMonitor{}, apierror.NewAPIError(apierror.ErrBadRequest, "Invalid balance ID", err)
			default:
				return model.BalanceMonitor{}, apierror.NewAPIError(apierror.ErrInternalServer, "Database error occurred", err)
			}
		}
		return model.BalanceMonitor{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to create monitor", err)
	}
	return monitor, nil
}

func (d Datasource) GetMonitorByID(id string) (*model.BalanceMonitor, error) {
	var preciseValue int64

	row := d.Conn.QueryRow(`
		SELECT monitor_id, balance_id, field, operator, value, precision, precise_value, description, call_back_url, created_at 
		FROM blnk.balance_monitors WHERE monitor_id = $1
	`, id)

	monitor := &model.BalanceMonitor{}
	condition := &model.AlertCondition{}
	err := row.Scan(&monitor.MonitorID, &monitor.BalanceID, &condition.Field, &condition.Operator, &condition.Value, &condition.Precision, &preciseValue, &monitor.Description, &monitor.CallBackURL, &monitor.CreatedAt)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Monitor with ID '%s' not found", id), err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve monitor", err)
	}
	monitor.Condition = *condition
	monitor.Condition.PreciseValue = big.NewInt(preciseValue)
	return monitor, nil
}

func (d Datasource) GetAllMonitors() ([]model.BalanceMonitor, error) {
	rows, err := d.Conn.Query(`
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at 
		FROM blnk.balance_monitors
	`)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve monitors", err)
	}
	defer rows.Close()

	var monitors []model.BalanceMonitor
	for rows.Next() {
		monitor := model.BalanceMonitor{}
		condition := model.AlertCondition{}
		err = rows.Scan(&monitor.MonitorID, &monitor.BalanceID, &condition.Field, &condition.Operator, &condition.Value, &monitor.Description, &monitor.CallBackURL, &monitor.CreatedAt)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan monitor data", err)
		}
		monitor.Condition = condition
		monitors = append(monitors, monitor)
	}
	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over monitors", err)
	}
	return monitors, nil
}

func (d Datasource) GetBalanceMonitors(balanceID string) ([]model.BalanceMonitor, error) {
	rows, err := d.Conn.Query(`
		SELECT monitor_id, balance_id, field, operator, value, description, call_back_url, created_at 
		FROM blnk.balance_monitors WHERE balance_id = $1
	`, balanceID)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve balance monitors", err)
	}
	defer rows.Close()

	var monitors []model.BalanceMonitor
	for rows.Next() {
		monitor := model.BalanceMonitor{}
		condition := model.AlertCondition{}
		err = rows.Scan(&monitor.MonitorID, &monitor.BalanceID, &condition.Field, &condition.Operator, &condition.Value, &monitor.Description, &monitor.CallBackURL, &monitor.CreatedAt)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan monitor data", err)
		}
		monitor.Condition = condition
		monitors = append(monitors, monitor)
	}
	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over balance monitors", err)
	}
	return monitors, nil
}

func (d Datasource) UpdateMonitor(monitor *model.BalanceMonitor) error {
	result, err := d.Conn.Exec(`
		UPDATE blnk.balance_monitors
		SET balance_id = $2, field = $3, operator = $4, value = $5, description = $6, call_back_url = $7
		WHERE monitor_id = $1
	`, monitor.MonitorID, monitor.BalanceID, monitor.Condition.Field, monitor.Condition.Operator, monitor.Condition.Value, monitor.Description, monitor.CallBackURL)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to update monitor", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Monitor with ID '%s' not found", monitor.MonitorID), nil)
	}

	return nil
}

func (d Datasource) DeleteMonitor(id string) error {
	result, err := d.Conn.Exec(`
		DELETE FROM blnk.balance_monitors WHERE monitor_id = $1
	`, id)

	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to delete monitor", err)
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return apierror.NewAPIError(apierror.ErrInternalServer, "Failed to get rows affected", err)
	}

	if rowsAffected == 0 {
		return apierror.NewAPIError(apierror.ErrNotFound, fmt.Sprintf("Monitor with ID '%s' not found", id), nil)
	}

	return nil
}
