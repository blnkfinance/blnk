package model

type CreateLedger struct {
	Name     string                 `json:"name"`
	MetaData map[string]interface{} `json:"meta_data"`
}
