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
package api

import (
	"fmt"
	"net/http"
	"strconv"

	apimodel "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
)

// CreateIdentity creates a new identity record in the system.
// It binds the incoming JSON request to an Identity object, validates it,
// and then creates the identity record. If any errors occur during validation
// or creation, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or creating the identity.
// - 201 Created: If the identity is successfully created.
func (a Api) CreateIdentity(c *gin.Context) {
	var identity model.Identity
	if err := c.ShouldBindJSON(&identity); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := a.blnk.CreateIdentity(identity)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

// GetIdentity retrieves an identity record by its ID.
// It extracts the ID from the route parameters and fetches the identity record.
// If the ID is missing or there's an error retrieving the identity, it responds
// with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID is missing or there's an error retrieving the identity.
// - 200 OK: If the identity is successfully retrieved.
func (a Api) GetIdentity(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetIdentity(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// UpdateIdentity updates an existing identity record by its ID.
// It binds the incoming JSON request to an Identity object, updates the record,
// and responds with a success message. If any errors occur during binding,
// validation, or update, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON, updating the identity, or missing ID.
// - 200 OK: If the identity is successfully updated.
func (a Api) UpdateIdentity(c *gin.Context) {
	var identity model.Identity
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	if err := c.ShouldBindJSON(&identity); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	identity.IdentityID = id
	err := a.blnk.UpdateIdentity(&identity)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Identity updated successfully"})
}

// DeleteIdentity deletes an existing identity record by its ID.
// It extracts the ID from the route parameters and deletes the record. If the ID is missing
// or there's an error deleting the identity, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID is missing or there's an error deleting the identity.
// - 200 OK: If the identity is successfully deleted.
func (a Api) DeleteIdentity(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	err := a.blnk.DeleteIdentity(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Identity deleted successfully"})
}

// GetAllIdentities retrieves all identity records in the system.
// It fetches the identity records and responds with the list of identities.
// Supports advanced filtering via query parameters in the format: field_operator=value
// Example filters:
//   - first_name_eq=John
//   - category_in=individual,corporate
//   - created_at_gte=2024-01-01
//
// If there's an error retrieving the identities, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error retrieving the identities or invalid filters.
// - 200 OK: If the identities are successfully retrieved.
func (a Api) GetAllIdentities(c *gin.Context) {
	// Extract limit and offset from query parameters
	limitStr := c.DefaultQuery("limit", "20")
	offsetStr := c.DefaultQuery("offset", "0")

	limitInt, err := strconv.Atoi(limitStr)
	if err != nil || limitInt <= 0 {
		limitInt = 20
	}

	offsetInt, err := strconv.Atoi(offsetStr)
	if err != nil || offsetInt < 0 {
		offsetInt = 0
	}

	// Check if advanced filters are present
	if HasFilters(c) {
		filters, parseErrors := ParseFiltersFromContext(c, nil)
		if len(parseErrors) > 0 {
			c.JSON(http.StatusBadRequest, gin.H{"errors": parseErrors})
			return
		}

		// Use the new filter method
		resp, err := a.blnk.GetAllIdentitiesWithFilter(c.Request.Context(), filters, limitInt, offsetInt)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, resp)
		return
	}

	// Fall back to the legacy method when no filters are present
	identities, err := a.blnk.GetAllIdentities()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, identities)
}

// FilterIdentities filters identities using a JSON request body.
// This endpoint accepts a POST request with filters specified in JSON format.
//
// Request body format:
//
//	{
//	  "filters": [
//	    {"field": "first_name", "operator": "eq", "value": "John"},
//	    {"field": "category", "operator": "in", "values": ["individual", "corporate"]}
//	  ],
//	  "limit": 20,
//	  "offset": 0
//	}
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error parsing the filters or retrieving identities.
// - 200 OK: If the identities are successfully retrieved.
func (a Api) FilterIdentities(c *gin.Context) {
	filters, opts, limit, offset, err := ParseFiltersFromBody(c)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, count, err := a.blnk.GetAllIdentitiesWithFilterAndOptions(c.Request.Context(), filters, opts, limit, offset)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if opts.IncludeCount {
		c.JSON(http.StatusOK, FilterResponse{Data: resp, TotalCount: count})
	} else {
		c.JSON(http.StatusOK, resp)
	}
}

// TokenizeIdentityField tokenizes a specific field in an identity.
// It extracts the identity ID and field name from the route parameters,
// tokenizes the field, and responds with a success message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID or field is missing, or there's an error tokenizing the field.
// - 200 OK: If the field is successfully tokenized.
func (a Api) TokenizeIdentityField(c *gin.Context) {
	id, idExists := c.Params.Get("id")
	field, fieldExists := c.Params.Get("field")

	if !idExists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "identity ID is required"})
		return
	}

	if !fieldExists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "field name is required"})
		return
	}

	err := a.blnk.TokenizeIdentityField(id, field)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Field tokenized successfully"})
}

// DetokenizeIdentityField detokenizes a specific field in an identity.
// It extracts the identity ID and field name from the route parameters,
// detokenizes the field, and responds with the original value.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID or field is missing, or there's an error detokenizing the field.
// - 200 OK: If the field is successfully detokenized, returning the original value.
func (a Api) DetokenizeIdentityField(c *gin.Context) {
	id, idExists := c.Params.Get("id")
	field, fieldExists := c.Params.Get("field")

	if !idExists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "identity ID is required"})
		return
	}

	if !fieldExists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "field name is required"})
		return
	}

	originalValue, err := a.blnk.DetokenizeIdentityField(id, field)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"field": field, "value": originalValue})
}

// TokenizeIdentity tokenizes multiple fields in an identity.
// It binds the incoming JSON request containing the list of fields to tokenize,
// tokenizes each field, and responds with a success message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID is missing, there's an error binding JSON, or there's an error tokenizing fields.
// - 200 OK: If the fields are successfully tokenized.
func (a Api) TokenizeIdentity(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "identity ID is required"})
		return
	}

	var request apimodel.TokenizeRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if len(request.Fields) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "at least one field must be specified"})
		return
	}

	err := a.blnk.TokenizeIdentity(id, request.Fields)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Fields tokenized successfully"})
}

// DetokenizeIdentity detokenizes multiple fields in an identity.
// It binds the incoming JSON request containing the list of fields to detokenize,
// detokenizes each field, and responds with the original values.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID is missing, there's an error binding JSON, or there's an error detokenizing fields.
// - 200 OK: If the fields are successfully detokenized, returning the original values.
func (a Api) DetokenizeIdentity(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "identity ID is required"})
		return
	}

	var request apimodel.DetokenizeRequest
	if err := c.ShouldBindJSON(&request); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// If no specific fields are provided, detokenize all tokenized fields
	if len(request.Fields) == 0 {
		detokenizedFields, err := a.blnk.DetokenizeIdentity(id)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		c.JSON(http.StatusOK, gin.H{"fields": detokenizedFields})
		return
	}

	// Detokenize specific fields
	result := make(map[string]string)
	for _, field := range request.Fields {
		value, err := a.blnk.DetokenizeIdentityField(id, field)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		result[field] = value
	}

	c.JSON(http.StatusOK, gin.H{"fields": result})
}

// GetTokenizedFields returns a list of fields that are currently tokenized for an identity.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID is missing or there's an error retrieving the identity.
// - 200 OK: Returns the list of tokenized fields.
func (a Api) GetTokenizedFields(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "identity ID is required"})
		return
	}

	identity, err := a.blnk.GetIdentity(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	tokenizedFields := make([]string, 0)

	// Debug the metadata structure
	if identity.MetaData != nil {
		tokenizedFieldsRaw, exists := identity.MetaData["tokenized_fields"]
		if exists {
			// Try map[string]bool first
			if tokenizedMap, ok := tokenizedFieldsRaw.(map[string]bool); ok {
				for field, isTokenized := range tokenizedMap {
					if isTokenized {
						tokenizedFields = append(tokenizedFields, field)
					}
				}
			} else if tokenizedMap, ok := tokenizedFieldsRaw.(map[string]interface{}); ok {
				// Try map[string]interface{} with boolean values
				for field, val := range tokenizedMap {
					if boolVal, ok := val.(bool); ok && boolVal {
						tokenizedFields = append(tokenizedFields, field)
					}
				}
			} else {
				c.JSON(http.StatusOK, gin.H{
					"tokenized_fields": tokenizedFields,
					"debug_info": gin.H{
						"has_metadata":         true,
						"tokenized_field_type": fmt.Sprintf("%T", tokenizedFieldsRaw),
						"raw_metadata":         identity.MetaData,
					},
				})
				return
			}
		}
	}

	c.JSON(http.StatusOK, gin.H{"tokenized_fields": tokenizedFields})
}
