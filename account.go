package blnk

import (
	"fmt"
	"net/http"

	"github.com/jerry-enebeli/blnk/internal/request"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/jerry-enebeli/blnk/config"
)

// applyExternalAccount sets the account number and bank name for a given blnk.Account object.
// It fetches configuration details and, if auto-generation of account numbers is enabled,
// makes an HTTP request to a specified service to retrieve these details.
func applyExternalAccount(account *model.Account) error {
	// Define a struct to hold account details received from the HTTP service.
	type accountDetails struct {
		AccountNumber string `json:"account_number"`
		BankName      string `json:"bank_name"`
	}

	// Fetch configuration settings.
	cnf, err := config.Fetch()
	if err != nil {
		// If there's an error in fetching the configuration, return from the function.
		return err
	}

	// Check if automatic account number generation is enabled in the configuration.
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

func (l Blnk) applyAccountName(account *model.Account) error {
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

func (l Blnk) overrideLedgerAndIdentity(account *model.Account) error {
	balance, err := l.GetBalanceByID(account.BalanceID, nil)
	if err != nil {
		return err
	}

	//if balance has an identity, it overrides account identity with the balance identity
	if balance.IdentityID != "" {
		account.IdentityID = balance.IdentityID
	}

	//if balance has a ledger, it overrides account ledger with the balance ledger
	if balance.LedgerID != "" {
		account.LedgerID = balance.LedgerID
	}

	//if balance has a currency, it overrides account currency with the balance currency
	if balance.Currency != "" {
		account.Currency = balance.Currency
	}
	return nil
}

func (l Blnk) setupBalance(account *model.Account) error {
	if account.BalanceID == "" {
		balance, err := l.CreateBalance(model.Balance{LedgerID: account.LedgerID, Currency: account.Currency, IdentityID: account.IdentityID})
		if err != nil {
			return err
		}
		account.BalanceID = balance.BalanceID
	}
	return nil
}

// CreateAccount creates a new account in the database.
func (l Blnk) CreateAccount(account model.Account) (model.Account, error) {
	err := l.setupBalance(&account)
	if err != nil {
		return model.Account{}, err
	}

	err = l.overrideLedgerAndIdentity(&account)
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

// GetAccount retrieves an account from the database by ID.
func (l Blnk) GetAccount(id string, include []string) (*model.Account, error) {
	return l.datasource.GetAccountByID(id, include)
}

// GetAccountByNumber retrieves an account from the database by ID.
func (l Blnk) GetAccountByNumber(id string) (*model.Account, error) {
	return l.datasource.GetAccountByNumber(id)
}

// GetAllAccounts retrieves all accounts from the database.
func (l Blnk) GetAllAccounts() ([]model.Account, error) {
	return l.datasource.GetAllAccounts()
}

// UpdateAccount updates an account in the database.
func (l Blnk) UpdateAccount(account *model.Account) error {
	return l.datasource.UpdateAccount(account)
}

// DeleteAccount deletes an account from the database by ID.
func (l Blnk) DeleteAccount(id string) error {
	return l.datasource.DeleteAccount(id)
}
