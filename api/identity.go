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
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk/model"
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
// If there's an error retrieving the identities, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error retrieving the identities.
// - 200 OK: If the identities are successfully retrieved.
func (a Api) GetAllIdentities(c *gin.Context) {
	identities, err := a.blnk.GetAllIdentities()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, identities)
}
