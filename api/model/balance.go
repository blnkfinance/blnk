package model

type CreateBalance struct {
	LedgerId   string                 `json:"ledger_id"`
	IdentityId string                 `json:"identity_id"`
	Currency   string                 `json:"currency"`
	Precision  float64                `json:"precision"`
	MetaData   map[string]interface{} `json:"meta_data"`
}

type CreateBalanceMonitor struct {
	BalanceId   string                 `json:"balance_id"`
	Condition   MonitorCondition       `json:"condition"`
	CallBackURL string                 `json:"call_back_url"`
	MetaData    map[string]interface{} `json:"meta_data"`
}

type MonitorCondition struct {
	Precision float64 `json:"precision"`
	Field     string  `json:"field"`
	Operator  string  `json:"operator"`
	Value     float64 `json:"value"`
}
