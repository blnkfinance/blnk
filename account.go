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
	"fmt"
	"net/http"

	"github.com/jerry-enebeli/blnk/internal/request"
	"github.com/jerry-enebeli/blnk/model"

	"github.com/jerry-enebeli/blnk/config"
)

// applyExternalAccount applies external account details to the given account.
// It fetches the configuration, checks if auto-generation is enabled, and makes an HTTP request to get account details.
// If the account details are successfully retrieved, they are applied to the account.
//
// Parameters:
// - account *model.Account: A pointer to the Account model to which external details will be applied.
//
// Returns:
// - error: An error if the operation fails.
func applyExternalAccount(account *model.Account) error {
	type accountDetails struct {
		AccountNumber string `json:"account_number"`
		BankName      string `json:"bank_name"`
	}

	cnf, err := config.Fetch()
	if err != nil {
		return err
	}

	if cnf.AccountNumberGeneration.EnableAutoGeneration {
		req, err := http.NewRequest("GET", cnf.AccountNumberGeneration.HttpService.Url, nil)
		if err != nil {
			return err
		}

		// Set the Authorization header for the HTTP request using the configuration.
		req.Header.Set("Authorization", cnf.AccountNumberGeneration.HttpService.Headers.Authorization)
		var response accountDetails
		_, err = request.Call(req, &response)
		if err != nil {
			return err
		}

		if response.AccountNumber != "" && response.BankName != "" {
			account.Number = response.AccountNumber
			account.BankName = response.BankName
		}
	}

	return nil
}

// applyAccountName applies a name to the given account based on its identity.
// If the account name is empty, it fetches the identity and sets the account name
// based on the identity type (organization or individual).
//
// Parameters:
// - account *model.Account: A pointer to the Account model to which the name will be applied.
//
// Returns:
// - error: An error if the identity could not be retrieved.
func (l *Blnk) applyAccountName(account *model.Account) error {
	if account.Name == "" {

		identity, err := l.GetIdentity(account.IdentityID)
		if err != nil {
			return err
		}
		if identity.IdentityType == "organization" {
			account.Name = identity.OrganizationName
		} else {
			account.Name = fmt.Sprintf("%s %s", identity.FirstName, identity.LastName)
		}
	}
	return nil
}

// overrideLedgerAndIdentity overrides the ledger and identity details of the given account
// based on the balance information. It fetches the balance by its ID and updates the account
// with the balance's identity ID, ledger ID, and currency if they are not empty.
//
// Parameters:
// - account *model.Account: A pointer to the Account model to be updated.
//
// Returns:
// - error: An error if the balance could not be retrieved.
func (l *Blnk) overrideLedgerAndIdentity(account *model.Account) error {
	balance, err := l.datasource.GetBalanceByIDLite(account.BalanceID)
	if err != nil {
		return err
	}

	if balance.IdentityID != "" {
		account.IdentityID = balance.IdentityID
	}

	if balance.LedgerID != "" {
		account.LedgerID = balance.LedgerID
	}

	if balance.Currency != "" {
		account.Currency = balance.Currency
	}
	return nil
}

// CreateAccount creates a new account in the database.
// It overrides the ledger and identity details, applies the account name, and fetches external account details.
//
// Parameters:
// - account model.Account: The Account model to be created.
//
// Returns:
// - model.Account: The created Account model.
// - error: An error if the account could not be created.
func (l *Blnk) CreateAccount(account model.Account) (model.Account, error) {
	err := l.overrideLedgerAndIdentity(&account)
	if err != nil {
		return model.Account{}, err
	}

	err = l.applyAccountName(&account)
	if err != nil {
		return model.Account{}, err
	}

	err = applyExternalAccount(&account)
	if err != nil {
		return model.Account{}, err
	}
	return l.datasource.CreateAccount(account)
}

// GetAccount retrieves an account by its ID.
// It fetches the account from the datasource and includes additional data as specified.
//
// Parameters:
// - id string: The ID of the account to retrieve.
// - include []string: A slice of strings specifying additional data to include.
//
// Returns:
// - *model.Account: A pointer to the Account model if found.
// - error: An error if the account could not be retrieved.
func (l *Blnk) GetAccount(id string, include []string) (*model.Account, error) {
	return l.datasource.GetAccountByID(id, include)
}

// GetAccountByNumber retrieves an account from the database by its account number.
//
// Parameters:
// - id string: The account number of the account to retrieve.
//
// Returns:
// - *model.Account: A pointer to the Account model if found.
// - error: An error if the account could not be retrieved.
func (l *Blnk) GetAccountByNumber(id string) (*model.Account, error) {
	return l.datasource.GetAccountByNumber(id)
}

// GetAllAccounts retrieves all accounts from the database.
//
// Returns:
// - []model.Account: A slice of Account models.
// - error: An error if the accounts could not be retrieved.
func (l *Blnk) GetAllAccounts() ([]model.Account, error) {
	return l.datasource.GetAllAccounts()
}
