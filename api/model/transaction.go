package model

import (
	"github.com/jerry-enebeli/blnk/model"
)

type RecordTransaction struct {
	Amount         float64                `json:"amount"`
	Precision      float64                `json:"precision"`
	AllowOverDraft bool                   `json:"allow_overdraft"`
	Inflight       bool                   `json:"inflight"`
	Source         string                 `json:"source"`
	Reference      string                 `json:"reference"`
	Drcr           string                 `json:"drcr"`
	Destination    string                 `json:"destination"`
	Description    string                 `json:"description"`
	Currency       string                 `json:"currency"`
	BalanceId      string                 `json:"balance_id"`
	ScheduledFor   string                 `json:"scheduled_for"`
	Sources        []model.Distribution   `json:"sources"`
	Destinations   []model.Distribution   `json:"destinations"`
	MetaData       map[string]interface{} `json:"meta_data"`
}
