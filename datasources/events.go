package datasources

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/jerry-enebeli/blnk"
)

// CreateEventMapper creates an event mapper in the database
func (d datasource) CreateEventMapper(eventMapper blnk.EventMapper) (blnk.EventMapper, error) {
	// convert mapping_instruction to JSONB
	instructionJSON, err := json.Marshal(eventMapper.MappingInstruction)
	if err != nil {
		return blnk.EventMapper{}, err
	}

	eventMapper.MapperID = GenerateUUIDWithSuffix("map")
	eventMapper.CreatedAt = time.Now()

	// insert into database
	_, err = d.conn.Exec(`
		INSERT INTO event_mappers (name, mapping_instruction, mapper_id)
		VALUES ($1, $2, $3)
	`, eventMapper.Name, instructionJSON, eventMapper.MapperID)

	if err != nil {
		return blnk.EventMapper{}, err
	}

	return eventMapper, nil
}

// GetAllEventMappers retrieves all event mappers from the database
func (d datasource) GetAllEventMappers() ([]blnk.EventMapper, error) {
	// select all event mappers from database
	rows, err := d.conn.Query(`
		SELECT mapper_id, name, created_at, mapping_instruction
		FROM event_mappers
	`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// create slice to store event mappers
	mappers := []blnk.EventMapper{}

	// iterate through result set and parse mapping_instruction from JSONB
	for rows.Next() {
		mapper := blnk.EventMapper{}
		var instructionJSON []byte
		err = rows.Scan(&mapper.MapperID, &mapper.Name, &mapper.CreatedAt, &instructionJSON)
		if err != nil {
			return nil, err
		}

		// convert mapping_instruction from JSONB to map
		err = json.Unmarshal(instructionJSON, &mapper.MappingInstruction)
		if err != nil {
			return nil, err
		}

		mappers = append(mappers, mapper)
	}

	return mappers, nil
}

// GetEventMapperByID retrieves a single event mapper from the database by ID
func (d datasource) GetEventMapperByID(id string) (*blnk.EventMapper, error) {
	// select event mapper from database by ID
	row := d.conn.QueryRow(`
		SELECT mapper_id, name, created_at, mapping_instruction
		FROM event_mappers
		WHERE mapper_id = $1
	`, id)

	mapper := blnk.EventMapper{}
	var instructionJSON []byte
	err := row.Scan(&mapper.MapperID, &mapper.Name, &mapper.CreatedAt, &instructionJSON)
	if err != nil {
		if err == sql.ErrNoRows {
			// Handle no rows error
			return nil, fmt.Errorf("event mapper with ID '%s' not found", id)
		} else {
			// Handle other errors
			return nil, err
		}
	}

	// convert mapping_instruction from JSONB to map
	err = json.Unmarshal(instructionJSON, &mapper.MappingInstruction)
	if err != nil {
		return nil, err
	}

	return &mapper, nil
}

// UpdateEventMapper updates an existing event mapper in the database
func (d datasource) UpdateEventMapper(eventMapper blnk.EventMapper) error {
	instructionJSON, err := json.Marshal(eventMapper.MappingInstruction)
	if err != nil {
		return err
	}

	_, err = d.conn.Exec(`
		UPDATE event_mappers
		SET name = $1, mapping_instruction = $2
		WHERE mapper_id = $3
	`, eventMapper.Name, instructionJSON, eventMapper.MapperID)

	return err
}

// DeleteEventMapper deletes an event mapper from the database by ID
func (d datasource) DeleteEventMapper(id string) error {
	_, err := d.conn.Exec(`
		DELETE FROM event_mappers
		WHERE mapper_id = $1
	`, id)
	return err
}
