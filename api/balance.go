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
	"time"

	model2 "github.com/jerry-enebeli/blnk/api/model"

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk/model"
)

// CreateBalance creates a new balance record in the system.
// It binds the incoming JSON request to a CreateBalance object, validates it,
// and then creates the balance record. If any errors occur during validation
// or creation, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or validating the balance.
// - 201 Created: If the balance is successfully created.
func (a Api) CreateBalance(c *gin.Context) {
	var newBalance model2.CreateBalance
	if err := c.ShouldBindJSON(&newBalance); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := newBalance.ValidateCreateBalance()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	resp, err := a.blnk.CreateBalance(c.Request.Context(), newBalance.ToBalance())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

// GetBalance retrieves a balance record by its ID.
// It extracts the ID from the route parameters and the 'include' query
// parameter to fetch additional related information. If the ID is missing
// or there's an error retrieving the balance, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID is missing or there's an error retrieving the balance.
// - 200 OK: If the balance is successfully retrieved.
func (a Api) GetBalance(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	// Extract 'include' parameter from the query
	includes := c.QueryArray("include")

	// Extract 'with_queued' parameter from the query, default to false
	withQueued := c.DefaultQuery("with_queued", "false") == "true"

	resp, err := a.blnk.GetBalanceByID(c.Request.Context(), id, includes, withQueued)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// GetBalances retrieves a list of balance records with pagination.
// It extracts the 'limit' and 'offset' query parameters to control pagination,
// and the 'include' query parameter to fetch additional related information.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error retrieving the balances or invalid query parameters.
// - 200 OK: If the balances are successfully retrieved.
func (a Api) GetBalances(c *gin.Context) {
	// Extract pagination parameters (limit and offset)
	limit, err := strconv.Atoi(c.DefaultQuery("limit", "10")) // Default to 10 if not specified
	if err != nil || limit <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid limit value"})
		return
	}

	offset, err := strconv.Atoi(c.DefaultQuery("offset", "0")) // Default to 0 if not specified
	if err != nil || offset < 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid offset value"})
		return
	}

	// Fetch balances with pagination
	resp, err := a.blnk.GetAllBalances(c.Request.Context(), limit, offset)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// CreateBalanceMonitor creates a new balance monitor record in the system.
// It binds the incoming JSON request to a CreateBalanceMonitor object, validates it,
// and then creates the monitor record. If any errors occur during validation
// or creation, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or validating the balance monitor.
// - 201 Created: If the balance monitor is successfully created.
func (a Api) CreateBalanceMonitor(c *gin.Context) {
	var newMonitor model2.CreateBalanceMonitor
	if err := c.ShouldBindJSON(&newMonitor); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := newMonitor.ValidateCreateBalanceMonitor()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	resp, err := a.blnk.CreateMonitor(c.Request.Context(), newMonitor.ToBalanceMonitor())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

// GetBalanceMonitor retrieves a balance monitor record by its ID.
// It extracts the ID from the route parameters. If the ID is missing
// or there's an error retrieving the monitor, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID is missing or there's an error retrieving the balance monitor.
// - 200 OK: If the balance monitor is successfully retrieved.
func (a Api) GetBalanceMonitor(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetMonitorByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// GetAllBalanceMonitors retrieves all balance monitor records in the system.
// It fetches the monitor records and responds with the list of monitors.
// If there's an error retrieving the monitors, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error retrieving the balance monitors.
// - 200 OK: If the balance monitors are successfully retrieved.
func (a Api) GetAllBalanceMonitors(c *gin.Context) {
	monitors, err := a.blnk.GetAllMonitors(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, monitors)
}

// GetBalanceMonitorsByBalanceID retrieves all balance monitors associated with a specific balance ID.
// It extracts the balance ID from the route parameters. If the balance ID is missing
// or there's an error retrieving the monitors, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the balance ID is missing or there's an error retrieving the balance monitors.
// - 200 OK: If the balance monitors are successfully retrieved.
func (a Api) GetBalanceMonitorsByBalanceID(c *gin.Context) {
	balanceID, passed := c.Params.Get("balance_id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "balance_id is required. pass balance_id in the route /:balance_id"})
		return
	}

	monitors, err := a.blnk.GetMonitorByID(c.Request.Context(), balanceID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, monitors)
}

// UpdateBalanceMonitor updates an existing balance monitor record by its ID.
// It binds the incoming JSON request to a BalanceMonitor object, updates the record,
// and responds with a success message. If any errors occur during binding, validation,
// or update, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON, validating the balance monitor, or updating the record.
// - 200 OK: If the balance monitor is successfully updated.
func (a Api) UpdateBalanceMonitor(c *gin.Context) {
	var monitor model.BalanceMonitor
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	if err := c.ShouldBindJSON(&monitor); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	monitor.MonitorID = id
	err := a.blnk.UpdateMonitor(c.Request.Context(), &monitor)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "BalanceMonitor updated successfully"})
}

// DeleteBalanceMonitor deletes an existing balance monitor record by its ID.
// It extracts the ID from the route parameters and deletes the record. If the ID is missing
// or there's an error deleting the monitor, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the ID is missing or there's an error deleting the balance monitor.
// - 200 OK: If the balance monitor is successfully deleted.
func (a Api) DeleteBalanceMonitor(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	err := a.blnk.DeleteMonitor(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "BalanceMonitor deleted successfully"})
}

// TakeBalanceSnapshots creates daily snapshots of balances in batches.
// It accepts an optional 'batch_size' query parameter to control the batch processing size.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in the batch size parameter or during snapshot creation.
// - 200 OK: If the snapshots are successfully created, returns the total number of snapshots created.
func (a Api) TakeBalanceSnapshots(c *gin.Context) {
	// Get batch size from query parameter, default to 1000 if not specified
	batchSize, err := strconv.Atoi(c.DefaultQuery("batch_size", "1000"))
	if err != nil || batchSize <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid batch_size value"})
		return
	}

	// Call the service to take snapshots
	a.blnk.TakeBalanceSnapshots(c.Request.Context(), batchSize)

	c.JSON(http.StatusOK, gin.H{
		"message": "Snapshotting in progress. should be completed shortly",
	})
}

// GetBalanceAtTime retrieves a balance's state at a specific point in time.
// It extracts the balance ID from the route parameters and the timestamp from query parameters.
// The timestamp should be provided in ISO 8601 format (e.g., "2024-01-01T15:04:05Z").
// Optionally accepts a "from_source" query parameter to calculate balance from all transactions.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the balance ID is missing, timestamp is invalid, or there's an error retrieving the balance.
// - 200 OK: If the historical balance state is successfully retrieved.
func (a Api) GetBalanceAtTime(c *gin.Context) {
	balanceID, exists := c.Params.Get("id")
	if !exists {
		c.JSON(http.StatusBadRequest, gin.H{"error": "balance ID is required"})
		return
	}

	var timestamp time.Time
	timestampStr := c.Query("timestamp")
	if timestampStr == "" {
		// Use current time if no timestamp is provided
		timestamp = time.Now().UTC()
	} else {
		var err error
		timestamp, err = time.Parse(time.RFC3339, timestampStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{
				"error": "invalid timestamp format. Please use ISO 8601 format (e.g., 2024-01-01T15:04:05Z)",
			})
			return
		}
	}

	// Check if the request specifies to calculate from source transactions
	fromSourceStr := c.Query("from_source")
	fromSource := fromSourceStr == "true" || fromSourceStr == "1"

	balance, err := a.blnk.GetBalanceAtTime(c.Request.Context(), balanceID, timestamp, fromSource)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	balanceResult := map[string]interface{}{
		"balance":        balance.Balance,
		"debit_balance":  balance.DebitBalance,
		"credit_balance": balance.CreditBalance,
		"currency":       balance.Currency,
		"balance_id":     balance.BalanceID,
	}

	c.JSON(http.StatusOK, gin.H{
		"balance":     balanceResult,
		"timestamp":   timestamp.Format(time.RFC3339),
		"from_source": fromSource,
	})
}

// GetBalanceByIndicator retrieves a balance by its indicator and currency.
// It extracts the indicator and currency from the route parameters.
// If either parameter is missing or there's an error retrieving the balance,
// it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If indicator or currency is missing or there's an error retrieving the balance.
// - 200 OK: If the balance is successfully retrieved.
func (a Api) GetBalanceByIndicator(c *gin.Context) {
	indicator, indicatorPassed := c.Params.Get("indicator")
	if !indicatorPassed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "indicator is required. pass indicator in the route /indicator/:indicator"})
		return
	}

	currency, currencyPassed := c.Params.Get("currency")
	if !currencyPassed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "currency is required. pass currency in the route /currency/:currency"})
		return
	}

	resp, err := a.blnk.GetBalanceByIndicator(c.Request.Context(), indicator, currency)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}
