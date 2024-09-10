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
