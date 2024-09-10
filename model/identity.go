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

type Identity struct {
	IdentityID       string                 `json:"identity_id" form:"identity_id"`
	IdentityType     string                 `json:"identity_type" form:"identity_type"`
	OrganizationName string                 `json:"organization_name" form:"organization_name"`
	Category         string                 `json:"category" form:"category"`
	FirstName        string                 `json:"first_name" form:"first_name"`
	LastName         string                 `json:"last_name" form:"last_name"`
	OtherNames       string                 `json:"other_names" form:"other_names"`
	Gender           string                 `json:"gender" form:"gender"`
	EmailAddress     string                 `json:"email_address" form:"email_address"`
	PhoneNumber      string                 `json:"phone_number" form:"phone_number"`
	Nationality      string                 `json:"nationality" form:"nationality"`
	Street           string                 `json:"street" form:"street"`
	Country          string                 `json:"country" form:"country"`
	State            string                 `json:"state" form:"state"`
	PostCode         string                 `json:"post_code" form:"postCode"`
	City             string                 `json:"city" form:"city"`
	DOB              time.Time              `json:"dob" form:"dob"`
	CreatedAt        time.Time              `json:"created_at" form:"createdAt"`
	MetaData         map[string]interface{} `json:"meta_data" form:"metaData"`
}
