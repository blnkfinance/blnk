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
	MetaData         map[string]interface{} `json:"meta_data" form:"metaData"`
	DOB              time.Time              `json:"dob" form:"dob"`
	CreatedAt        time.Time              `json:"created_at" form:"createdAt"`
}
