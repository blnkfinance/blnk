package blnk

import (
	"fmt"
	"net/http"

	"github.com/jerry-enebeli/blnk/internal/request"
	"github.com/jerry-enebeli/blnk/model"

	"github.com/jerry-enebeli/blnk/config"
)

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

func (l *Blnk) GetAccount(id string, include []string) (*model.Account, error) {
	return l.datasource.GetAccountByID(id, include)
}

// GetAccountByNumber retrieves an account from the database by ID.
func (l *Blnk) GetAccountByNumber(id string) (*model.Account, error) {
	return l.datasource.GetAccountByNumber(id)
}

// GetAllAccounts retrieves all accounts from the database.
func (l *Blnk) GetAllAccounts() ([]model.Account, error) {
	return l.datasource.GetAllAccounts()
}
