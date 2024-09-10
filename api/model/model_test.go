package model

import (
	"testing"
	"time"

	"github.com/jerry-enebeli/blnk/model"
	"github.com/stretchr/testify/assert"
)

func TestSourceOrSourcesValidation(t *testing.T) {
	tests := []struct {
		name        string
		transaction RecordTransaction
		wantErr     bool
	}{
		{
			name:        "Valid with Source",
			transaction: RecordTransaction{Source: "source1"},
			wantErr:     false,
		},
		{
			name:        "Valid with Sources",
			transaction: RecordTransaction{Sources: []model.Distribution{{Identifier: "source1", Distribution: "100"}}},
			wantErr:     false,
		},
		{
			name:        "Invalid with both Source and Sources",
			transaction: RecordTransaction{Source: "source1", Sources: []model.Distribution{{Identifier: "source2", Distribution: "100"}}},
			wantErr:     true,
		},
		{
			name:        "Invalid with neither Source nor Sources",
			transaction: RecordTransaction{},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := sourceOrSourcesValidation(&tt.transaction)(nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestDestinationOrDestinationsValidation(t *testing.T) {
	tests := []struct {
		name        string
		transaction RecordTransaction
		wantErr     bool
	}{
		{
			name:        "Valid with Destination",
			transaction: RecordTransaction{Destination: "dest1"},
			wantErr:     false,
		},
		{
			name:        "Valid with Destinations",
			transaction: RecordTransaction{Destinations: []model.Distribution{{Identifier: "dest1", Distribution: "100"}}},
			wantErr:     false,
		},
		{
			name:        "Invalid with both Destination and Destinations",
			transaction: RecordTransaction{Destination: "dest1", Destinations: []model.Distribution{{Identifier: "dest2", Distribution: "100"}}},
			wantErr:     true,
		},
		{
			name:        "Invalid with neither Destination nor Destinations",
			transaction: RecordTransaction{},
			wantErr:     true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := destinationOrDestinationsValidation(&tt.transaction)(nil)
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateCreateLedger(t *testing.T) {
	tests := []struct {
		name    string
		ledger  CreateLedger
		wantErr bool
	}{
		{
			name:    "Valid Ledger",
			ledger:  CreateLedger{Name: "Test Ledger"},
			wantErr: false,
		},
		{
			name:    "Invalid Ledger - Empty Name",
			ledger:  CreateLedger{},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.ledger.ValidateCreateLedger()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateCreateBalance(t *testing.T) {
	tests := []struct {
		name    string
		balance CreateBalance
		wantErr bool
	}{
		{
			name:    "Valid Balance",
			balance: CreateBalance{LedgerId: "ledger1", Currency: "USD"},
			wantErr: false,
		},
		{
			name:    "Invalid Balance - Missing LedgerId",
			balance: CreateBalance{Currency: "USD"},
			wantErr: true,
		},
		{
			name:    "Invalid Balance - Missing Currency",
			balance: CreateBalance{LedgerId: "ledger1"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.balance.ValidateCreateBalance()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateCreateAccount(t *testing.T) {
	tests := []struct {
		name    string
		account CreateAccount
		wantErr bool
	}{
		{
			name:    "Valid Account with LedgerId",
			account: CreateAccount{LedgerId: "ledger1", IdentityId: "identity1", Currency: "USD"},
			wantErr: false,
		},
		{
			name:    "Valid Account with BalanceId",
			account: CreateAccount{BalanceId: "balance1"},
			wantErr: false,
		},
		{
			name:    "Invalid Account - Missing Required Fields",
			account: CreateAccount{},
			wantErr: true,
		},
		{
			name:    "Invalid Account - Both LedgerId and BalanceId",
			account: CreateAccount{LedgerId: "ledger1", BalanceId: "balance1", IdentityId: "identity1", Currency: "USD"},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.account.ValidateCreateAccount()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestValidateRecordTransaction(t *testing.T) {
	tests := []struct {
		name        string
		transaction RecordTransaction
		wantErr     bool
	}{
		{
			name: "Valid Transaction",
			transaction: RecordTransaction{
				Amount:      100,
				Currency:    "USD",
				Reference:   "ref1",
				Description: "Test transaction",
				Source:      "source1",
				Destination: "dest1",
			},
			wantErr: false,
		},
		{
			name: "Invalid Transaction - Missing Required Fields",
			transaction: RecordTransaction{
				Amount: 100,
			},
			wantErr: true,
		},
		{
			name: "Invalid Transaction - Invalid ScheduledFor",
			transaction: RecordTransaction{
				Amount:       100,
				Currency:     "USD",
				Reference:    "ref1",
				Description:  "Test transaction",
				Source:       "source1",
				Destination:  "dest1",
				ScheduledFor: "invalid-date",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.transaction.ValidateRecordTransaction()
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				assert.NoError(t, err)
			}
		})
	}
}

func TestToLedger(t *testing.T) {
	createLedger := CreateLedger{
		Name:     "Test Ledger",
		MetaData: map[string]interface{}{"key": "value"},
	}

	ledger := createLedger.ToLedger()

	assert.Equal(t, createLedger.Name, ledger.Name)
	assert.Equal(t, createLedger.MetaData, ledger.MetaData)
}

func TestToBalance(t *testing.T) {
	createBalance := CreateBalance{
		LedgerId:   "ledger1",
		IdentityId: "identity1",
		Currency:   "USD",
		MetaData:   map[string]interface{}{"key": "value"},
		Precision:  2,
	}

	balance := createBalance.ToBalance()

	assert.Equal(t, createBalance.LedgerId, balance.LedgerID)
	assert.Equal(t, createBalance.IdentityId, balance.IdentityID)
	assert.Equal(t, createBalance.Currency, balance.Currency)
	assert.Equal(t, createBalance.MetaData, balance.MetaData)
	assert.Equal(t, createBalance.Precision, balance.CurrencyMultiplier)
}

func TestToAccount(t *testing.T) {
	createAccount := CreateAccount{
		BalanceId:  "balance1",
		LedgerId:   "ledger1",
		IdentityId: "identity1",
		Currency:   "USD",
		Number:     "123456",
		BankName:   "Test Bank",
		MetaData:   map[string]interface{}{"key": "value"},
	}

	account := createAccount.ToAccount()

	assert.Equal(t, createAccount.BalanceId, account.BalanceID)
	assert.Equal(t, createAccount.LedgerId, account.LedgerID)
	assert.Equal(t, createAccount.IdentityId, account.IdentityID)
	assert.Equal(t, createAccount.Currency, account.Currency)
	assert.Equal(t, createAccount.Number, account.Number)
	assert.Equal(t, createAccount.BankName, account.BankName)
	assert.Equal(t, createAccount.MetaData, account.MetaData)
}

func TestToTransaction(t *testing.T) {
	now := time.Now()
	scheduledFor := now.Add(24 * time.Hour)
	inflightExpiryDate := now.Add(48 * time.Hour)

	recordTransaction := RecordTransaction{
		Currency:           "USD",
		Source:             "source1",
		Description:        "Test transaction",
		Reference:          "ref1",
		ScheduledFor:       scheduledFor.Format(time.RFC3339),
		Destination:        "dest1",
		Amount:             100,
		AllowOverDraft:     true,
		MetaData:           map[string]interface{}{"key": "value"},
		Sources:            []model.Distribution{{Identifier: "source1", Distribution: "100"}},
		Destinations:       []model.Distribution{{Identifier: "dest1", Distribution: "100"}},
		Inflight:           true,
		Precision:          2,
		InflightExpiryDate: inflightExpiryDate.Format(time.RFC3339),
		Rate:               1.5,
	}

	transaction := recordTransaction.ToTransaction()

	assert.Equal(t, recordTransaction.Currency, transaction.Currency)
	assert.Equal(t, recordTransaction.Source, transaction.Source)
	assert.Equal(t, recordTransaction.Description, transaction.Description)
	assert.Equal(t, recordTransaction.Reference, transaction.Reference)
	assert.Equal(t, recordTransaction.Destination, transaction.Destination)
	assert.Equal(t, recordTransaction.Amount, transaction.Amount)
	assert.Equal(t, recordTransaction.AllowOverDraft, transaction.AllowOverdraft)
	assert.Equal(t, recordTransaction.MetaData, transaction.MetaData)
	assert.Equal(t, recordTransaction.Sources, transaction.Sources)
	assert.Equal(t, recordTransaction.Destinations, transaction.Destinations)
	assert.Equal(t, recordTransaction.Inflight, transaction.Inflight)
	assert.Equal(t, recordTransaction.Precision, transaction.Precision)
	assert.Equal(t, recordTransaction.Rate, transaction.Rate)
}
