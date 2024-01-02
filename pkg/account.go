package pkg

import (
	"fmt"

	"github.com/jerry-enebeli/blnk"
)

// CreateAccount creates a new account in the database.
func (l Blnk) CreateAccount(account blnk.Account) (blnk.Account, error) {
	if account.Name == "" {
		identity, err := l.GetIdentity(account.IdentityID)
		if err != nil {
			return blnk.Account{}, err
		}
		if identity.IdentityType == "organization" {
			account.Name = identity.OrganizationName
		} else {
			account.Name = fmt.Sprintf("%s %s", identity.FirstName, identity.LastName)
		}
	}
	return l.datasource.CreateAccount(account)
}

// GetAccount retrieves an account from the database by ID.
func (l Blnk) GetAccount(id string) (*blnk.Account, error) {
	return l.datasource.GetAccountByID(id)
}

// GetAccountByNumber retrieves an account from the database by ID.
func (l Blnk) GetAccountByNumber(id string) (*blnk.Account, error) {
	return l.datasource.GetAccountByNumber(id)
}

// GetAllAccounts retrieves all accounts from the database.
func (l Blnk) GetAllAccounts() ([]blnk.Account, error) {
	return l.datasource.GetAllAccounts()
}

// UpdateAccount updates an account in the database.
func (l Blnk) UpdateAccount(account *blnk.Account) error {
	return l.datasource.UpdateAccount(account)
}

// DeleteAccount deletes an account from the database by ID.
func (l Blnk) DeleteAccount(id string) error {
	return l.datasource.DeleteAccount(id)
}
