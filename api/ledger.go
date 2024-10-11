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
	"strconv"

	model2 "github.com/jerry-enebeli/blnk/api/model"

	"github.com/gin-gonic/gin"
)

// CreateLedger creates a new ledger record in the system.
// It binds the incoming JSON request to a CreateLedger object, validates it,
// and then creates the ledger record. If any errors occur during validation
// or creation, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or validating the ledger.
// - 201 Created: If the ledger is successfully created.
func (a Api) CreateLedger(c *gin.Context) {
	var newLedger model2.CreateLedger
	if err := c.ShouldBindJSON(&newLedger); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	err := newLedger.ValidateCreateLedger()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	resp, err := a.blnk.CreateLedger(newLedger.ToLedger())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

// GetLedger retrieves a ledger record by its ID.
// It extracts the ID from the route parameters and fetches the ledger record.
// If the ID is missing or there's an error retrieving the ledger, it responds
// with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID is missing or there's an error retrieving the ledger.
// - 200 OK: If the ledger is successfully retrieved.
func (a Api) GetLedger(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. Pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetLedgerByID(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// GetAllLedgers retrieves all ledger records in the system.
// It fetches the ledger records and responds with the list of ledgers.
// If there's an error retrieving the ledgers, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error retrieving the ledger records.
// - 200 OK: If the ledger records are successfully retrieved.
func (a Api) GetAllLedgers(c *gin.Context) {
	// Extract limit and offset from query parameters
	limit := c.DefaultQuery("limit", "10")  // Default limit is 10 if not provided
	offset := c.DefaultQuery("offset", "0") // Default offset is 0 if not provided

	// Convert limit and offset to integers
	limitInt, err := strconv.Atoi(limit)
	if err != nil || limitInt < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid limit value"})
		return
	}

	offsetInt, err := strconv.Atoi(offset)
	if err != nil || offsetInt < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid offset value"})
		return
	}

	// Call the GetAllLedgers method with limit and offset
	resp, err := a.blnk.GetAllLedgers(limitInt, offsetInt)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}
