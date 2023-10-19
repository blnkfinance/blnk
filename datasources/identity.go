package datasources

import (
	"context"
	"encoding/json"
	"time"

	"github.com/jerry-enebeli/blnk"
)

// CreateIdentity inserts a new Identity into the database
func (d datasource) CreateIdentity(identity blnk.Identity) (blnk.Identity, error) {
	metaDataJSON, err := json.Marshal(identity.MetaData)
	if err != nil {
		return identity, err
	}

	identity.IdentityID = GenerateUUIDWithSuffix("idt")
	identity.CreatedAt = time.Now()

	_, err = d.conn.Exec(`
		INSERT INTO identity (identity_id,identity_type, first_name, last_name, other_names, gender, dob, email_address, phone_number, nationality, organization_name, category, street, country, state, post_code, city, created_at, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18, $19)
	`, identity.IdentityID, identity.IdentityType, identity.Individual.FirstName, identity.Individual.LastName, identity.Individual.OtherNames, identity.Individual.Gender, identity.Individual.DOB, identity.Individual.EmailAddress, identity.Individual.PhoneNumber, identity.Individual.Nationality, identity.Organization.Name, identity.Organization.Category, identity.Street, identity.Country, identity.State, identity.PostCode, identity.City, identity.CreatedAt, metaDataJSON)

	return identity, err
}

// GetIdentityByID retrieves an identity from the database by ID
func (d datasource) GetIdentityByID(id string) (*blnk.Identity, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 1*time.Minute)
	defer cancel()

	tx, err := d.conn.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	row := tx.QueryRow(`
	SELECT identity_id, identity_type, first_name, last_name, other_names, gender, dob, email_address, phone_number, nationality, organization_name, category, street, country, state, post_code, city, created_at, meta_data
	FROM identity
	WHERE identity_id = $1 FOR UPDATE SKIP LOCKED
`, id)

	identity := &blnk.Identity{}
	var metaDataJSON []byte
	err = row.Scan(
		&identity.IdentityID, &identity.IdentityType,
		&identity.Individual.FirstName, &identity.Individual.LastName, &identity.Individual.OtherNames, &identity.Individual.Gender, &identity.Individual.DOB, &identity.Individual.EmailAddress, &identity.Individual.PhoneNumber, &identity.Individual.Nationality,
		&identity.Organization.Name, &identity.Organization.Category,
		&identity.Street, &identity.Country, &identity.State, &identity.PostCode, &identity.City, &identity.CreatedAt, &metaDataJSON,
	)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	err = json.Unmarshal(metaDataJSON, &identity.MetaData)
	if err != nil {
		_ = tx.Rollback()
		return nil, err
	}

	err = tx.Commit()
	if err != nil {
		return nil, err
	}

	return identity, nil
}

// GetAllIdentities retrieves all identities from the database
func (d datasource) GetAllIdentities() ([]blnk.Identity, error) {
	rows, err := d.conn.Query(`
	SELECT identity_id, identity_type, first_name, last_name, other_names, gender, dob, email_address, phone_number, nationality, organization_name, category, street, country, state, post_code, city, created_at, meta_data
	FROM identity
`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var identities []blnk.Identity
	for rows.Next() {
		identity := blnk.Identity{}
		var metaDataJSON []byte
		err = rows.Scan(
			&identity.IdentityID, &identity.IdentityType,
			&identity.Individual.FirstName, &identity.Individual.LastName, &identity.Individual.OtherNames, &identity.Individual.Gender, &identity.Individual.DOB, &identity.Individual.EmailAddress, &identity.Individual.PhoneNumber, &identity.Individual.Nationality,
			&identity.Organization.Name, &identity.Organization.Category,
			&identity.Street, &identity.Country, &identity.State, &identity.PostCode, &identity.City, &identity.CreatedAt, &metaDataJSON,
		)
		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(metaDataJSON, &identity.MetaData)
		if err != nil {
			return nil, err
		}

		identities = append(identities, identity)
	}

	return identities, nil
}

// UpdateIdentity updates an identity in the database
func (d datasource) UpdateIdentity(identity *blnk.Identity) error {
	metaDataJSON, err := json.Marshal(identity.MetaData)
	if err != nil {
		return err
	}

	_, err = d.conn.Exec(`
		UPDATE identity
		SET identity_type = $2, first_name = $3, last_name = $4, other_names = $5, gender = $6, dob = $7, email_address = $8, phone_number = $9, nationality = $10, organization_name = $11, category = $12, street = $13, country = $14, state = $15, post_code = $16, city = $17, created_at = $18, meta_data = $19
		WHERE identity_id = $1
	`, identity.IdentityID, identity.IdentityType, identity.Individual.FirstName, identity.Individual.LastName, identity.Individual.OtherNames, identity.Individual.Gender, identity.Individual.DOB, identity.Individual.EmailAddress, identity.Individual.PhoneNumber, identity.Individual.Nationality, identity.Organization.Name, identity.Organization.Category, identity.Street, identity.Country, identity.State, identity.PostCode, identity.City, identity.CreatedAt, metaDataJSON)

	return err
}

// DeleteIdentity deletes an identity from the database by ID
func (d datasource) DeleteIdentity(id string) error {
	_, err := d.conn.Exec(`
		DELETE FROM identity
		WHERE identity_id = $1
	`, id)
	return err
}
