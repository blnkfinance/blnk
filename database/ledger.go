package database

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jerry-enebeli/blnk/model"
)

func (d Datasource) CreateLedger(ledger model.Ledger) (model.Ledger, error) {
	// convert metadata to JSONB
	metaDataJSON, err := json.Marshal(ledger.MetaData)
	if err != nil {
		return model.Ledger{}, err
	}

	ledger.LedgerID = GenerateUUIDWithSuffix("ldg")
	ledger.CreatedAt = time.Now()

	// insert into database
	_, err = d.Conn.Exec(`
		INSERT INTO ledgers (meta_data, name, ledger_id)
		VALUES ($1, $2,$3)

	`, metaDataJSON, ledger.Name, ledger.LedgerID)

	if err != nil {
		return model.Ledger{}, err
	}

	return ledger, nil
}

// GetAllLedgers retrieves all ledgers from the database
func (d Datasource) GetAllLedgers() ([]model.Ledger, error) {
	// select all ledgers from database
	rows, err := d.Conn.Query(`
		SELECT ledger_id,name, created_at, meta_data
		FROM ledgers
		LIMIT 20
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// create slice to store ledgers
	ledgers := []model.Ledger{}

	// iterate through result set and parse metadata from JSONB
	for rows.Next() {
		ledger := model.Ledger{}
		var metaDataJSON []byte
		err = rows.Scan(&ledger.LedgerID, &ledger.Name, &ledger.CreatedAt, &metaDataJSON)
		if err != nil {
			return nil, err
		}

		// convert metadata from JSONB to map
		err = json.Unmarshal(metaDataJSON, &ledger.MetaData)
		if err != nil {
			return nil, err
		}

		ledgers = append(ledgers, ledger)
	}

	return ledgers, nil
}

// GetLedgerByID retrieves a single ledger from the database by ID
func (d Datasource) GetLedgerByID(id string) (*model.Ledger, error) {
	ledger := model.Ledger{}

	// select ledger from database by ID
	row := d.Conn.QueryRow(`
		SELECT ledger_id, name, created_at, meta_data
		FROM ledgers
		WHERE ledger_id = $1
	`, id)

	var metaDataJSON []byte
	err := row.Scan(&ledger.LedgerID, &ledger.Name, &ledger.CreatedAt, &metaDataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			// Handle no rows error
			return nil, fmt.Errorf("ledger with ID '%s' not found", id)
		} else {
			// Handle other errors
			return nil, err
		}
	}

	// convert metadata from JSONB to map
	err = json.Unmarshal(metaDataJSON, &ledger.MetaData)
	if err != nil {
		return nil, err
	}

	return &ledger, nil
}
