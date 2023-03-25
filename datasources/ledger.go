package datasources

import (
	"database/sql"
	"encoding/json"

	"github.com/jerry-enebeli/blnk"
)

func (d datasource) CreateLedger(ledger blnk.Ledger) (blnk.Ledger, error) {
	// convert metadata to JSONB
	metaDataJSON, err := json.Marshal(ledger.MetaData)
	if err != nil {
		return blnk.Ledger{}, err
	}

	// insert into database
	_, err = d.conn.Exec(`
		INSERT INTO ledgers (meta_data)
		VALUES ($1)
	`, metaDataJSON)

	if err != nil {
		return blnk.Ledger{}, err
	}

	return ledger, nil
}

// GetAllLedgers retrieves all ledgers from the database
func (d datasource) GetAllLedgers() ([]blnk.Ledger, error) {
	// select all ledgers from database
	rows, err := d.conn.Query(`
		SELECT id, created, meta_data
		FROM ledgers
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// create slice to store ledgers
	ledgers := []blnk.Ledger{}

	// iterate through result set and parse metadata from JSONB
	for rows.Next() {
		ledger := blnk.Ledger{}
		var metaDataJSON []byte
		err = rows.Scan(&ledger.ID, &ledger.Created, &metaDataJSON)
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
func (d datasource) GetLedgerByID(id int64) (*blnk.Ledger, error) {
	// select ledger from database by ID
	row := d.conn.QueryRow(`
		SELECT id, created, meta_data
		FROM ledgers
		WHERE id = $1
	`, id)

	ledger := blnk.Ledger{}
	var metaDataJSON []byte
	err := row.Scan(&ledger.ID, &ledger.Created, &metaDataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			// ledger not found, return nil
			return nil, nil
		}
		return nil, err
	}

	// convert metadata from JSONB to map
	err = json.Unmarshal(metaDataJSON, &ledger.MetaData)
	if err != nil {
		return nil, err
	}

	return &ledger, nil
}
