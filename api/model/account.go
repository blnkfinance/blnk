package model

type CreateAccount struct {
	BankName   string                 `json:"bank_name"`
	Number     string                 `json:"number"`
	Currency   string                 `json:"currency"`
	IdentityId string                 `json:"identity_id"`
	LedgerId   string                 `json:"ledger_id"`
	BalanceId  string                 `json:"balance_id"`
	MetaData   map[string]interface{} `json:"meta_data"`
}
