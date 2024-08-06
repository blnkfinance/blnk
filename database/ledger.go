package database

import (
	"database/sql"
	"encoding/json"
	"time"

	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/lib/pq"
)

func (d Datasource) CreateLedger(ledger model.Ledger) (model.Ledger, error) {
	metaDataJSON, err := json.Marshal(ledger.MetaData)
	if err != nil {
		return model.Ledger{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to marshal metadata", err)
	}

	ledger.LedgerID = model.GenerateUUIDWithSuffix("ldg")
	ledger.CreatedAt = time.Now()

	_, err = d.Conn.Exec(`
		INSERT INTO blnk.ledgers (meta_data, name, ledger_id)
		VALUES ($1, $2, $3)
	`, metaDataJSON, ledger.Name, ledger.LedgerID)

	if err != nil {
		pqErr, ok := err.(*pq.Error)
		if ok {
			switch pqErr.Code.Name() {
			case "unique_violation":
				return model.Ledger{}, apierror.NewAPIError(apierror.ErrConflict, "Ledger with this name or ID already exists", err)
			default:
				return model.Ledger{}, apierror.NewAPIError(apierror.ErrInternalServer, "Database error occurred", err)
			}
		}
		return model.Ledger{}, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to create ledger", err)
	}

	return ledger, nil
}

func (d Datasource) GetAllLedgers() ([]model.Ledger, error) {
	rows, err := d.Conn.Query(`
		SELECT ledger_id, name, created_at, meta_data
		FROM blnk.ledgers
		LIMIT 20
	`)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve ledgers", err)
	}
	defer rows.Close()

	ledgers := []model.Ledger{}

	for rows.Next() {
		ledger := model.Ledger{}
		var metaDataJSON []byte
		err = rows.Scan(&ledger.LedgerID, &ledger.Name, &ledger.CreatedAt, &metaDataJSON)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to scan ledger data", err)
		}

		err = json.Unmarshal(metaDataJSON, &ledger.MetaData)
		if err != nil {
			return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
		}

		ledgers = append(ledgers, ledger)
	}

	if err = rows.Err(); err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Error occurred while iterating over ledgers", err)
	}

	return ledgers, nil
}

func (d Datasource) GetLedgerByID(id string) (*model.Ledger, error) {
	ledger := model.Ledger{}

	row := d.Conn.QueryRow(`
		SELECT ledger_id, name, created_at, meta_data
		FROM blnk.ledgers
		WHERE ledger_id = $1
	`, id)

	var metaDataJSON []byte
	err := row.Scan(&ledger.LedgerID, &ledger.Name, &ledger.CreatedAt, &metaDataJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			return nil, apierror.NewAPIError(apierror.ErrNotFound, "Ledger not found", err)
		}
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to retrieve ledger", err)
	}

	err = json.Unmarshal(metaDataJSON, &ledger.MetaData)
	if err != nil {
		return nil, apierror.NewAPIError(apierror.ErrInternalServer, "Failed to unmarshal metadata", err)
	}

	return &ledger, nil
}
