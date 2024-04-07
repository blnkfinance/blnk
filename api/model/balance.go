package model

type CreateBalance struct {
	LedgerId   string                 `json:"ledger_id"`
	IdentityId string                 `json:"identity_id"`
	Currency   string                 `json:"currency"`
	Preceision int64                  `json:"preceision"`
	MetaData   map[string]interface{} `json:"meta_data"`
}

type CreateBalanceMonitor struct {
	BalanceId   string                 `json:"balance_id"`
	Condition   MonitorCondition       `json:"condition"`
	CallBackURL string                 `json:"call_back_url"`
	MetaData    map[string]interface{} `json:"meta_data"`
}

type MonitorCondition struct {
	Field    string `json:"field"`
	Operator string `json:"operator"`
	Value    int64  `json:"value"`
}
