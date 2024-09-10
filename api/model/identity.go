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

import "time"

type CreateIdentity struct {
	IdentityType     string                 `json:"identity_type"`
	FirstName        string                 `json:"first_name"`
	LastName         string                 `json:"last_name"`
	OtherNames       string                 `json:"other_names"`
	Gender           string                 `json:"gender"`
	Dob              time.Time              `json:"dob"`
	EmailAddress     string                 `json:"email_address"`
	PhoneNumber      string                 `json:"phone_number"`
	Nationality      string                 `json:"nationality"`
	OrganizationName string                 `json:"organization_name"`
	Category         string                 `json:"category"`
	Street           string                 `json:"street"`
	Country          string                 `json:"country"`
	State            string                 `json:"state"`
	PostCode         string                 `json:"post_code"`
	City             string                 `json:"city"`
	CreatedAt        time.Time              `json:"created_at"`
	MetaData         map[string]interface{} `json:"meta_data"`
}
