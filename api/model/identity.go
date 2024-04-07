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
