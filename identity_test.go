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

package blnk

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/model"

	"github.com/brianvoe/gofakeit/v6"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
)

func TestCreateIdentity(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	identity := model.Identity{
		IdentityType:     "individual",
		OrganizationName: "",
		Category:         "",
		FirstName:        gofakeit.FirstName(),
		LastName:         gofakeit.LastName(),
		OtherNames:       gofakeit.LastName(),
		Gender:           gofakeit.Gender(),
		DOB:              gofakeit.Date(),
		EmailAddress:     gofakeit.Email(),
		PhoneNumber:      gofakeit.Phone(),
		Nationality:      gofakeit.Country(),
		Street:           gofakeit.Street(),
		Country:          gofakeit.Country(),
		State:            gofakeit.State(),
		PostCode:         "0000",
		City:             gofakeit.City(),
		MetaData:         nil,
	}
	metaDataJSON, _ := json.Marshal(identity.MetaData)

	mock.ExpectExec("INSERT INTO blnk.identity").
		WithArgs(sqlmock.AnyArg(), identity.IdentityType, identity.FirstName, identity.LastName, identity.OtherNames, identity.Gender, identity.DOB, identity.EmailAddress, identity.PhoneNumber, identity.Nationality, identity.OrganizationName, identity.Category, identity.Street, identity.Country, identity.State, identity.PostCode, identity.City, sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	result, err := d.CreateIdentity(identity)
	assert.NoError(t, err)
	assert.NotEmpty(t, result.IdentityID)
	assert.Equal(t, identity.FirstName, result.FirstName)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetIdentity(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "test-id"

	// Expect transaction to begin
	mock.ExpectBegin()

	// Updated mock data with all fields
	row := sqlmock.NewRows([]string{
		"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob",
		"email_address", "phone_number", "nationality", "organization_name", "category",
		"street", "country", "state", "post_code", "city", "created_at", "meta_data",
	}).AddRow(
		testID, "Individual", "John", "Doe", "Other Names", "Male", time.Now(),
		"john@example.com", "1234567890", "Nationality", "Organization", "Category",
		"Street", "Country", "State", "PostCode", "City", time.Now(), `{"key":"value"}`,
	)

	// Updated query to match the actual method's query
	mock.ExpectQuery("SELECT .* FROM blnk.identity WHERE identity_id =").
		WithArgs(testID).
		WillReturnRows(row)

	// Expect transaction to commit
	mock.ExpectCommit()

	result, err := d.GetIdentity(testID)

	// Updated assertions for all fields
	assert.NoError(t, err)
	assert.NotNil(t, result)
	assert.Equal(t, testID, result.IdentityID)
	assert.Equal(t, "Individual", result.IdentityType)
	assert.Equal(t, "John", result.FirstName)
	// ... continue with assertions for all fields ...

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestGetAllIdentities(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	rows := sqlmock.NewRows([]string{
		"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob",
		"email_address", "phone_number", "nationality", "organization_name", "category",
		"street", "country", "state", "post_code", "city", "created_at", "meta_data",
	}).AddRow(
		"idt_12345", "individual", "John", "Doe", "Other Names", "Male", time.Now(),
		"john@example.com", "1234567890", "Nationality", "Organization", "Category",
		"Street", "Country", "State", "PostCode", "City", time.Now(), `{"key":"value"}`,
	).AddRow(
		"idt_4442345", "individual", "John", "Doe", "Other Names", "Male", time.Now(),
		"john@example.com", "1234567890", "Nationality", "Organization", "Category",
		"Street", "Country", "State", "PostCode", "City", time.Now(), `{"key":"value"}`,
	)

	mock.ExpectQuery("SELECT .* FROM blnk.identity").WillReturnRows(rows)

	result, err := d.GetAllIdentities()

	assert.NoError(t, err)
	assert.Len(t, result, 2)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestUpdateIdentity(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	identity := &model.Identity{
		IdentityID:       "idt_123",
		IdentityType:     "individual",
		OrganizationName: "Test Org",
		Category:         "Test Category",
		FirstName:        "John",
		LastName:         "Doe",
		OtherNames:       "Middle",
		Gender:           "Male",
		DOB:              time.Now(),
		EmailAddress:     "john.doe@example.com",
		PhoneNumber:      "1234567890",
		Nationality:      "US",
		Street:           "123 Main St",
		Country:          "United States",
		State:            "NY",
		PostCode:         "10001",
		City:             "New York",
		MetaData:         map[string]interface{}{"key": "value"},
	}

	// Marshal the metadata to JSON for the SQL parameter
	metaDataJSON, err := json.Marshal(identity.MetaData)
	assert.NoError(t, err)

	// Update the SQL pattern to include meta_data field
	mock.ExpectExec(`UPDATE blnk\.identity SET identity_type = \$1, first_name = \$2, last_name = \$3, other_names = \$4, gender = \$5, dob = \$6, email_address = \$7, phone_number = \$8, nationality = \$9, organization_name = \$10, category = \$11, street = \$12, country = \$13, state = \$14, post_code = \$15, city = \$16, meta_data = \$17 WHERE identity_id = \$18`).
		WithArgs(
			identity.IdentityType,
			identity.FirstName,
			identity.LastName,
			identity.OtherNames,
			identity.Gender,
			identity.DOB,
			identity.EmailAddress,
			identity.PhoneNumber,
			identity.Nationality,
			identity.OrganizationName,
			identity.Category,
			identity.Street,
			identity.Country,
			identity.State,
			identity.PostCode,
			identity.City,
			metaDataJSON,        // Add the meta_data parameter
			identity.IdentityID, // Update parameter index to $18
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.UpdateIdentity(identity)
	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDeleteIdentity(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "idt_123"

	mock.ExpectExec("DELETE FROM blnk.identity WHERE identity_id =").
		WithArgs(testID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.DeleteIdentity(testID)

	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestTokenizeIdentityField_InvalidField(t *testing.T) {
	datasource, _, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	err = d.TokenizeIdentityField("idt_123", "invalid_field")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not tokenizable")
}

func TestTokenizeIdentityField_Success(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "idt_123"

	mock.ExpectBegin()
	row := sqlmock.NewRows([]string{
		"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob",
		"email_address", "phone_number", "nationality", "organization_name", "category",
		"street", "country", "state", "post_code", "city", "created_at", "meta_data",
	}).AddRow(
		testID, "Individual", "John", "Doe", "Other", "Male", time.Now(),
		"john@example.com", "1234567890", "US", "Org", "Cat",
		"Street", "Country", "State", "12345", "City", time.Now(), `{}`,
	)
	mock.ExpectQuery("SELECT .* FROM blnk.identity WHERE identity_id =").
		WithArgs(testID).
		WillReturnRows(row)
	mock.ExpectCommit()

	mock.ExpectExec("UPDATE blnk.identity SET").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), testID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.TokenizeIdentityField(testID, "EmailAddress")
	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestTokenizeIdentityField_AlreadyTokenized(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "idt_123"

	mock.ExpectBegin()
	row := sqlmock.NewRows([]string{
		"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob",
		"email_address", "phone_number", "nationality", "organization_name", "category",
		"street", "country", "state", "post_code", "city", "created_at", "meta_data",
	}).AddRow(
		testID, "Individual", "John", "Doe", "Other", "Male", time.Now(),
		"john@example.com", "1234567890", "US", "Org", "Cat",
		"Street", "Country", "State", "12345", "City", time.Now(), `{"tokenized_fields":{"EmailAddress":true}}`,
	)
	mock.ExpectQuery("SELECT .* FROM blnk.identity WHERE identity_id =").
		WithArgs(testID).
		WillReturnRows(row)
	mock.ExpectCommit()

	err = d.TokenizeIdentityField(testID, "EmailAddress")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "already tokenized")
}

func TestTokenizeIdentity_Success(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "idt_123"

	mock.ExpectBegin()
	row1 := sqlmock.NewRows([]string{
		"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob",
		"email_address", "phone_number", "nationality", "organization_name", "category",
		"street", "country", "state", "post_code", "city", "created_at", "meta_data",
	}).AddRow(
		testID, "Individual", "John", "Doe", "Other", "Male", time.Now(),
		"john@example.com", "1234567890", "US", "Org", "Cat",
		"Street", "Country", "State", "12345", "City", time.Now(), `{}`,
	)
	mock.ExpectQuery("SELECT .* FROM blnk.identity WHERE identity_id =").
		WithArgs(testID).
		WillReturnRows(row1)
	mock.ExpectCommit()

	mock.ExpectExec("UPDATE blnk.identity SET").
		WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), testID).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = d.TokenizeIdentity(testID, []string{"EmailAddress"})
	assert.NoError(t, err)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestDetokenizeIdentityField_NotTokenized(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "idt_123"

	mock.ExpectBegin()
	row := sqlmock.NewRows([]string{
		"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob",
		"email_address", "phone_number", "nationality", "organization_name", "category",
		"street", "country", "state", "post_code", "city", "created_at", "meta_data",
	}).AddRow(
		testID, "Individual", "John", "Doe", "Other", "Male", time.Now(),
		"john@example.com", "1234567890", "US", "Org", "Cat",
		"Street", "Country", "State", "12345", "City", time.Now(), `{}`,
	)
	mock.ExpectQuery("SELECT .* FROM blnk.identity WHERE identity_id =").
		WithArgs(testID).
		WillReturnRows(row)
	mock.ExpectCommit()

	_, err = d.DetokenizeIdentityField(testID, "email_address")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "not tokenized")
}

func TestDetokenizeIdentity_Empty(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "idt_123"

	mock.ExpectBegin()
	row := sqlmock.NewRows([]string{
		"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob",
		"email_address", "phone_number", "nationality", "organization_name", "category",
		"street", "country", "state", "post_code", "city", "created_at", "meta_data",
	}).AddRow(
		testID, "Individual", "John", "Doe", "Other", "Male", time.Now(),
		"john@example.com", "1234567890", "US", "Org", "Cat",
		"Street", "Country", "State", "12345", "City", time.Now(), `{}`,
	)
	mock.ExpectQuery("SELECT .* FROM blnk.identity WHERE identity_id =").
		WithArgs(testID).
		WillReturnRows(row)
	mock.ExpectCommit()

	result, err := d.DetokenizeIdentity(testID)
	assert.NoError(t, err)
	assert.Empty(t, result)

	if err := mock.ExpectationsWereMet(); err != nil {
		t.Errorf("there were unfulfilled expectations: %s", err)
	}
}

func TestTokenizeAllPII(t *testing.T) {
	datasource, mock, err := newTestDataSource()
	if err != nil {
		t.Fatalf("Error creating test data source: %s", err)
	}

	d, err := NewBlnk(datasource)
	if err != nil {
		t.Fatalf("Error creating Blnk instance: %s", err)
	}

	testID := "idt_123"

	for i := 0; i < 10; i++ {
		mock.ExpectBegin()
		row := sqlmock.NewRows([]string{
			"identity_id", "identity_type", "first_name", "last_name", "other_names", "gender", "dob",
			"email_address", "phone_number", "nationality", "organization_name", "category",
			"street", "country", "state", "post_code", "city", "created_at", "meta_data",
		}).AddRow(
			testID, "Individual", "John", "Doe", "Other", "Male", time.Now(),
			"john@example.com", "1234567890", "US", "Org", "Cat",
			"Street", "Country", "State", "12345", "City", time.Now(), `{}`,
		)
		mock.ExpectQuery("SELECT .* FROM blnk.identity WHERE identity_id =").
			WithArgs(testID).
			WillReturnRows(row)
		mock.ExpectCommit()

		mock.ExpectExec("UPDATE blnk.identity SET").
			WithArgs(sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), testID).
			WillReturnResult(sqlmock.NewResult(1, 1))
	}

	err = d.TokenizeAllPII(testID)
	assert.NoError(t, err)
}

func TestConvertToStructFieldName(t *testing.T) {
	tests := []struct {
		input    string
		expected string
	}{
		{"email_address", "Email_address"},
		{"first_name", "First_name"},
		{"phone_number", "Phone_number"},
		{"EmailAddress", "EmailAddress"},
		{"", ""},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			result := convertToStructFieldName(tt.input)
			assert.Equal(t, tt.expected, result)
		})
	}
}
