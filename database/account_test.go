package database

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/stretchr/testify/assert"
)

func TestCreateAccount_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	account := model.Account{
		Name:       "Test Account",
		Number:     "1234567890",
		BankName:   "Test Bank",
		Currency:   "USD",
		LedgerID:   "ldg1",
		IdentityID: "idt1",
		BalanceID:  "bal1",
		MetaData: map[string]interface{}{
			"key": "value",
		},
	}

	metaDataJSON, err := json.Marshal(account.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("INSERT INTO blnk.accounts").
		WithArgs(sqlmock.AnyArg(), account.Name, account.Number, account.BankName, account.Currency, account.LedgerID, account.IdentityID, account.BalanceID, sqlmock.AnyArg(), metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	createdAccount, err := ds.CreateAccount(account)
	assert.NoError(t, err)
	assert.NotEmpty(t, createdAccount.AccountID)
}

func TestGetAccountByID_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{
		"key": "value",
	}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	// Begin transaction expectation
	mock.ExpectBegin()

	// Mock the query result for account retrieval
	row := sqlmock.NewRows([]string{"account_id", "name", "number", "bank_name", "currency", "ledger_id", "identity_id", "balance_id", "created_at", "meta_data"}).
		AddRow("acc1", "Test Account", "1234567890", "Test Bank", "USD", "ldg1", "idt1", "bal1", time.Now(), metaDataJSON)

	mock.ExpectQuery("SELECT a.account_id, a.name, a.number, a.bank_name").
		WithArgs("acc1").
		WillReturnRows(row)

	// Commit transaction expectation
	mock.ExpectCommit()

	account, err := ds.GetAccountByID("acc1", []string{})
	assert.NoError(t, err)
	assert.Equal(t, "acc1", account.AccountID)
	assert.Equal(t, "Test Account", account.Name)

	// Check if all expectations were met
	err = mock.ExpectationsWereMet()
	assert.NoError(t, err)
}

func TestGetAllAccounts_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{
		"key": "value",
	}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	rows := sqlmock.NewRows([]string{"account_id", "name", "number", "bank_name", "currency", "created_at", "meta_data"}).
		AddRow("acc1", "Test Account 1", "1234567890", "Test Bank", "USD", time.Now(), metaDataJSON).
		AddRow("acc2", "Test Account 2", "0987654321", "Test Bank", "USD", time.Now(), metaDataJSON)

	mock.ExpectQuery("SELECT account_id, name, number, bank_name").
		WillReturnRows(rows)

	accounts, err := ds.GetAllAccounts()
	assert.NoError(t, err)
	assert.Len(t, accounts, 2)
	assert.Equal(t, "acc1", accounts[0].AccountID)
	assert.Equal(t, "Test Account 1", accounts[0].Name)
}

func TestGetAccountByNumber_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	metaData := map[string]interface{}{
		"key": "value",
	}
	metaDataJSON, err := json.Marshal(metaData)
	assert.NoError(t, err)

	row := sqlmock.NewRows([]string{"account_id", "name", "number", "bank_name", "created_at", "meta_data"}).
		AddRow("acc1", "Test Account", "1234567890", "Test Bank", time.Now(), metaDataJSON)

	mock.ExpectQuery("SELECT account_id, name, number, bank_name").
		WithArgs("1234567890").
		WillReturnRows(row)

	account, err := ds.GetAccountByNumber("1234567890")
	assert.NoError(t, err)
	assert.Equal(t, "acc1", account.AccountID)
	assert.Equal(t, "Test Account", account.Name)
}

func TestUpdateAccount_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	account := &model.Account{
		AccountID: "acc1",
		Name:      "Updated Account",
		Number:    "1234567890",
		BankName:  "Updated Bank",
		MetaData: map[string]interface{}{
			"key": "updated_value",
		},
	}

	metaDataJSON, err := json.Marshal(account.MetaData)
	assert.NoError(t, err)

	mock.ExpectExec("UPDATE blnk.accounts").
		WithArgs(account.AccountID, account.Name, account.Number, account.BankName, metaDataJSON).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.UpdateAccount(account)
	assert.NoError(t, err)
}

func TestDeleteAccount_Success(t *testing.T) {
	db, mock, err := sqlmock.New()
	assert.NoError(t, err)
	defer db.Close()

	ds := Datasource{Conn: db}

	mock.ExpectExec("DELETE FROM blnk.accounts").
		WithArgs("acc1").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = ds.DeleteAccount("acc1")
	assert.NoError(t, err)
}
