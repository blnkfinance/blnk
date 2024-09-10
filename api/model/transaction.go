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
package model

import (
	"github.com/jerry-enebeli/blnk/model"
)

type RecordTransaction struct {
	Amount             float64                `json:"amount"`
	Rate               float64                `json:"rate"`
	Precision          float64                `json:"precision"`
	AllowOverDraft     bool                   `json:"allow_overdraft"`
	Inflight           bool                   `json:"inflight"`
	Source             string                 `json:"source"`
	Reference          string                 `json:"reference"`
	Destination        string                 `json:"destination"`
	Description        string                 `json:"description"`
	Currency           string                 `json:"currency"`
	BalanceId          string                 `json:"balance_id"`
	ScheduledFor       string                 `json:"scheduled_for"`
	InflightExpiryDate string                 `json:"inflight_expiry_date,omitempty"`
	Sources            []model.Distribution   `json:"sources"`
	Destinations       []model.Distribution   `json:"destinations"`
	MetaData           map[string]interface{} `json:"meta_data"`
}

type InflightUpdate struct {
	Status string  `json:"status"`
	Amount float64 `json:"amount"`
}
