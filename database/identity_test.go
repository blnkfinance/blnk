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
	"database/sql"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/stretchr/testify/assert"
)

func TestCreateIdentity_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	identity := model.Identity{
		IdentityType:     "individual",
		FirstName:        "John",
		LastName:         "Doe",
		OtherNames:       "JD",
		Gender:           "Male",
		DOB:              time.Now(),
		EmailAddress:     "john.doe@example.com",
		PhoneNumber:      "123456789",
		Nationality:      "US",
		OrganizationName: "Acme Corp",
		Category:         "Customer",
		Street:           "123 Main St",
		Country:          "USA",
		State:            "CA",
		PostCode:         "90210",
		City:             "Los Angeles",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(identity.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.identity").
		WithArgs(sqlmock.AnyArg(), identity.IdentityType, identity.FirstName, identity.LastName, identity.OtherNames, identity.Gender, identity.DOB, identity.EmailAddress, identity.PhoneNumber, identity.Nationality, identity.OrganizationName, identity.Category, identity.Street, identity.Country, identity.State, identity.PostCode, identity.City, sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	createdIdentity, err := ds.CreateIdentity(identity)
	assert.NoError(t, err)
	assert.NotEmpty(t, createdIdentity.IdentityID)
	assert.WithinDuration(t, time.Now(), createdIdentity.CreatedAt, time.Second)
}

func TestCreateIdentity_Fail(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	identity := model.Identity{
		IdentityType: "individual",
		FirstName:    "John",
		LastName:     "Doe",
	}

	mock.ExpectExec("INSERT INTO blnk.identity").
		WithArgs(sqlmock.AnyArg(), identity.IdentityType, identity.FirstName, identity.LastName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg()).
		WillReturnError(fmt.Errorf("failed to insert"))

	_, err = ds.CreateIdentity(identity)
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrInternalServer, err.(apierror.APIError).Code)
}

func TestGetIdentityByID_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT identity_id, identity_type, first_name, last_name").
		WithArgs("idt123").
		WillReturnError(sql.ErrNoRows)
	mock.ExpectRollback()

	_, err = ds.GetIdentityByID("idt123")
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrNotFound, err.(apierror.APIError).Code)
}

func TestGetIdentityByID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	expectedIdentity := &model.Identity{
		IdentityID:       "idt123",
		IdentityType:     "individual",
		FirstName:        "John",
		LastName:         "Doe",
		OtherNames:       "JD",
		Gender:           "Male",
		DOB:              time.Now(),
		EmailAddress:     "john.doe@example.com",
		PhoneNumber:      "123456789",
		Nationality:      "US",
		OrganizationName: "Acme Corp",
		Category:         "Customer",
		Street:           "123 Main St",
		Country:          "USA",
		State:            "CA",
		PostCode:         "90210",
		City:             "Los Angeles",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, _ := json.Marshal(expectedIdentity.MetaData)

	mock.ExpectBegin()
	mock.ExpectQuery("SELECT identity_id, identity_type, first_name, last_name, other_names, gender, dob, email_address, phone_number, nationality, organization_name, category, street, country, state, post_code, city, created_at, meta_data").
		WithArgs("idt123").
		WillReturnRows(sqlmock.NewRows([]string{
			"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob", "email_address", "phone_number", "nationality", "organization_name", "category", "street", "country", "state", "post_code", "city", "created_at", "meta_data",
		}).AddRow(expectedIdentity.IdentityID, expectedIdentity.IdentityType, expectedIdentity.FirstName, expectedIdentity.LastName, expectedIdentity.OtherNames, expectedIdentity.Gender, expectedIdentity.DOB, expectedIdentity.EmailAddress, expectedIdentity.PhoneNumber, expectedIdentity.Nationality, expectedIdentity.OrganizationName, expectedIdentity.Category, expectedIdentity.Street, expectedIdentity.Country, expectedIdentity.State, expectedIdentity.PostCode, expectedIdentity.City, expectedIdentity.CreatedAt, metaDataJSON))
	mock.ExpectCommit()

	identity, err := ds.GetIdentityByID("idt123")
	assert.NoError(t, err)
	assert.Equal(t, expectedIdentity.IdentityID, identity.IdentityID)
	assert.Equal(t, expectedIdentity.FirstName, identity.FirstName)
	assert.Equal(t, expectedIdentity.LastName, identity.LastName)
	assert.Equal(t, expectedIdentity.MetaData, identity.MetaData)
}

func TestGetAllIdentities_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	expectedIdentities := []model.Identity{
		{
			IdentityID:       "idt1",
			FirstName:        "John",
			LastName:         "Doe",
			OtherNames:       "JD",
			Gender:           "Male",
			DOB:              time.Now(),
			EmailAddress:     "john.doe@example.com",
			PhoneNumber:      "123456789",
			Nationality:      "US",
			OrganizationName: "Acme Corp",
			Category:         "Customer",
			Street:           "123 Main St",
			Country:          "USA",
			State:            "CA",
			PostCode:         "90210",
			City:             "Los Angeles",
			MetaData: map[string]interface{}{
				"key": "value",
			},
		},
		{
			IdentityID:       "idt2",
			FirstName:        "Jane",
			LastName:         "Smith",
			OtherNames:       "",
			Gender:           "Female",
			DOB:              time.Now(),
			EmailAddress:     "jane.smith@example.com",
			PhoneNumber:      "987654321",
			Nationality:      "US",
			OrganizationName: "XYZ Corp",
			Category:         "Supplier",
			Street:           "456 Market St",
			Country:          "USA",
			State:            "CA",
			PostCode:         "94105",
			City:             "San Francisco",
			MetaData: map[string]interface{}{
				"key2": "value2",
			},
		},
	}

	// Properly marshal the metadata to JSON strings
	metaData1, err := json.Marshal(expectedIdentities[0].MetaData)
	assert.NoError(t, err)
	metaData2, err := json.Marshal(expectedIdentities[1].MetaData)
	assert.NoError(t, err)

	// Mock the query result to return all 19 columns
	mock.ExpectQuery("SELECT identity_id, identity_type, first_name, last_name, other_names, gender, dob, email_address, phone_number, nationality, organization_name, category, street, country, state, post_code, city, created_at, meta_data").
		WillReturnRows(sqlmock.NewRows([]string{
			"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob", "email_address", "phone_number", "nationality", "organization_name", "category", "street", "country", "state", "post_code", "city", "created_at", "meta_data",
		}).
			AddRow(expectedIdentities[0].IdentityID, expectedIdentities[0].IdentityType, expectedIdentities[0].FirstName, expectedIdentities[0].LastName, expectedIdentities[0].OtherNames, expectedIdentities[0].Gender, expectedIdentities[0].DOB, expectedIdentities[0].EmailAddress, expectedIdentities[0].PhoneNumber, expectedIdentities[0].Nationality, expectedIdentities[0].OrganizationName, expectedIdentities[0].Category, expectedIdentities[0].Street, expectedIdentities[0].Country, expectedIdentities[0].State, expectedIdentities[0].PostCode, expectedIdentities[0].City, expectedIdentities[0].CreatedAt, metaData1).
			AddRow(expectedIdentities[1].IdentityID, expectedIdentities[1].IdentityType, expectedIdentities[1].FirstName, expectedIdentities[1].LastName, expectedIdentities[1].OtherNames, expectedIdentities[1].Gender, expectedIdentities[1].DOB, expectedIdentities[1].EmailAddress, expectedIdentities[1].PhoneNumber, expectedIdentities[1].Nationality, expectedIdentities[1].OrganizationName, expectedIdentities[1].Category, expectedIdentities[1].Street, expectedIdentities[1].Country, expectedIdentities[1].State, expectedIdentities[1].PostCode, expectedIdentities[1].City, expectedIdentities[1].CreatedAt, metaData2))

	// Execute the function under test
	identities, err := ds.GetAllIdentities()
	assert.NoError(t, err)
	assert.Len(t, identities, 2)
	assert.Equal(t, expectedIdentities[0].IdentityID, identities[0].IdentityID)
	assert.Equal(t, expectedIdentities[1].IdentityID, identities[1].IdentityID)
}

func TestUpdateIdentity_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	identity := &model.Identity{
		IdentityID:   "idt1",
		FirstName:    "John",
		LastName:     "Doe",
		EmailAddress: "john.doe@example.com",
		PhoneNumber:  "123456789",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(identity.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("UPDATE blnk.identity").
		WithArgs(identity.IdentityID, identity.IdentityType, identity.FirstName, identity.LastName, sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.UpdateIdentity(identity)
	assert.NoError(t, err)
}

func TestDeleteIdentity_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	mock.ExpectExec("DELETE FROM blnk.identity").
		WithArgs("idt123").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.DeleteIdentity("idt123")
	assert.NoError(t, err)
}

func TestDeleteIdentity_NotFound(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	mock.ExpectExec("DELETE FROM blnk.identity").
		WithArgs("idt123").
		WillReturnResult(sqlmock.NewResult(1, 0))

	err = ds.DeleteIdentity("idt123")
	assert.Error(t, err)
	assert.Equal(t, apierror.ErrNotFound, err.(apierror.APIError).Code)
}
