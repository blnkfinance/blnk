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
	"errors"
	"math/big"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
	pgconn "github.com/blnkfinance/blnk/internal/pg-conn"
	"github.com/blnkfinance/blnk/model"

	"github.com/gin-gonic/gin"
	validation "github.com/go-ozzo/ozzo-validation/v4"
)

// transformTransaction prepares a transaction for API response by ensuring
// that metadata fields are properly represented as first-class fields.
// This maintains backward compatibility while keeping responses clean.
func transformTransaction(txn *model.Transaction) *model.Transaction {
	if txn == nil {
		return &model.Transaction{}
	}

	// Deep copy instead of shallow copy
	result := *txn

	// Deep copy the slices to avoid race conditions
	if txn.Sources != nil {
		result.Sources = make([]model.Distribution, len(txn.Sources))
		copy(result.Sources, txn.Sources)
	}

	if txn.Destinations != nil {
		result.Destinations = make([]model.Distribution, len(txn.Destinations))
		copy(result.Destinations, txn.Destinations)
	}

	// Deep copy the metadata map
	if txn.MetaData != nil {
		result.MetaData = make(map[string]interface{})
		for k, v := range txn.MetaData {
			result.MetaData[k] = v
		}
	}

	// Check for inflight flag in metadata and move it to the main field
	if result.MetaData != nil {
		if inflightVal, exists := result.MetaData["inflight"]; exists {
			if inflight, ok := inflightVal.(bool); ok && inflight {
				result.Inflight = true
				// Remove from metadata to avoid duplication
				delete(result.MetaData, "inflight")
			}
		}

		// If metadata is now empty, set it to nil
		if len(result.MetaData) == 0 {
			result.MetaData = nil
		}
	}

	return &result
}

func handleRecordTransactionValidationError(c *gin.Context, err error) {
	var validationErrors validation.Errors
	if errors.As(err, &validationErrors) {
		if precisionErr, ok := validationErrors["Precision"]; ok && errors.Is(precisionErr, model2.ErrPrecisionMustBeInteger) {
			c.JSON(http.StatusBadRequest, gin.H{"error": precisionErr.Error()})
			return
		}
	}

	if errors.Is(err, model2.ErrPrecisionMustBeInteger) {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
}

// RecordTransaction handles the recording of a new transaction.
// It binds the incoming JSON request to a RecordTransaction object, validates it,
// and then records the transaction. If any errors occur during validation or recording,
// it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or validating the transaction.
// - 201 Created: If the transaction is successfully recorded.
func (a Api) RecordTransaction(c *gin.Context) {
	var newTransaction model2.RecordTransaction
	// Bind the incoming JSON request to the newTransaction model
	if err := c.ShouldBindJSON(&newTransaction); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	// Validate the transaction data
	err := newTransaction.ValidateRecordTransaction()
	if err != nil {
		handleRecordTransactionValidationError(c, err)
		return
	}

	// Record the transaction using the Blnk service
	resp, err := a.blnk.RecordTransaction(c.Request.Context(), newTransaction.ToTransaction())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Return a response with the recorded transaction, properly transformed
	c.JSON(http.StatusCreated, transformTransaction(resp))
}

// QueueTransaction handles queuing a new transaction for later processing.
// It binds the incoming JSON request to a RecordTransaction object, validates it,
// and then queues the transaction. If any errors occur during validation or queuing,
// it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or validating the transaction.
// - 201 Created: If the transaction is successfully queued.
func (a Api) QueueTransaction(c *gin.Context) {
	var newTransaction model2.RecordTransaction
	// Bind the incoming JSON request to the newTransaction model
	if err := c.ShouldBindJSON(&newTransaction); err != nil {
		logrus.WithError(err).Error("failed to bind transaction JSON")
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}

	// Validate the transaction data
	err := newTransaction.ValidateRecordTransaction()
	if err != nil {
		handleRecordTransactionValidationError(c, err)
		return
	}

	// Queue the transaction using the Blnk service
	resp, err := a.blnk.QueueTransaction(c.Request.Context(), newTransaction.ToTransaction())
	if err != nil {
		logrus.WithError(err).Error("failed to queue transaction")
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Return a response with the queued transaction, properly transformed
	c.JSON(http.StatusCreated, transformTransaction(resp))
}

// RefundTransaction processes a refund for a transaction based on the given ID.
// It retrieves the transaction to be refunded and processes it in batches. If any errors
// occur during retrieval or processing, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in retrieving the transaction or no transaction is found to refund.
// - 201 Created: If the refund is successfully processed.
func (a Api) RefundTransaction(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}
	transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, big.NewInt(0), 1, false, a.blnk.GetRefundableTransactionsByParentID, a.blnk.RefundWorker)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(transaction) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no transaction to refund"})
		return
	}
	resp := transformTransaction(transaction[0])
	c.JSON(http.StatusCreated, resp)
}

// GetTransaction retrieves a transaction by its ID.
// It returns the transaction details if found. If the ID is not provided or an error
// occurs while retrieving the transaction, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in retrieving the transaction or the ID is missing.
// - 200 OK: If the transaction is successfully retrieved.
func (a Api) GetTransaction(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetTransaction(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, transformTransaction(resp))
}

// GetTransactionByRef retrieves a transaction by its reference.
// It returns the transaction details if found. If the reference is not provided or an error
// occurs while retrieving the transaction, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in retrieving the transaction or the reference is missing.
// - 200 OK: If the transaction is successfully retrieved.
func (a Api) GetTransactionByRef(c *gin.Context) {
	reference, passed := c.Params.Get("reference")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "reference is required. pass reference in the route /ref/:reference"})
		return
	}

	resp, err := a.blnk.GetTransactionByRef(c.Request.Context(), reference)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, transformTransaction(&resp))
}

// GetAllTransactions retrieves all transactions with pagination and optional filtering.
// Supports advanced filtering via query parameters in the format: field_operator=value
// Example filters:
//   - status_eq=APPLIED
//   - currency_in=USD,EUR
//   - amount_gte=1000
//   - created_at_between=2024-01-01|2024-12-31
//   - source_eq=bln_123
//   - destination_eq=bln_456
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error retrieving the transactions or invalid filters.
// - 200 OK: If the transactions are successfully retrieved.
func (a Api) GetAllTransactions(c *gin.Context) {
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
		transactions, err := a.blnk.GetAllTransactionsWithFilter(c.Request.Context(), filters, limitInt, offsetInt)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}

		// Transform each transaction for response
		result := make([]*model.Transaction, len(transactions))
		for i := range transactions {
			result[i] = transformTransaction(&transactions[i])
		}

		c.JSON(http.StatusOK, result)
		return
	}

	// Fall back to legacy method when no filters are present
	transactions, err := a.blnk.GetAllTransactions(limitInt, offsetInt)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Transform each transaction for response
	result := make([]*model.Transaction, len(transactions))
	for i := range transactions {
		result[i] = transformTransaction(&transactions[i])
	}

	c.JSON(http.StatusOK, result)
}

// FilterTransactions filters transactions using a JSON request body.
// This endpoint accepts a POST request with filters specified in JSON format,
// providing more flexibility than query parameter filters.
//
// Request body format:
//
//	{
//	  "filters": [
//	    {"field": "status", "operator": "eq", "value": "APPLIED"},
//	    {"field": "amount", "operator": "gte", "value": 1000},
//	    {"field": "currency", "operator": "in", "values": ["USD", "EUR"]}
//	  ],
//	  "limit": 20,
//	  "offset": 0
//	}
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error parsing the filters or retrieving transactions.
// - 200 OK: If the transactions are successfully retrieved.
func (a Api) FilterTransactions(c *gin.Context) {
	filters, opts, limit, offset, err := ParseFiltersFromBody(c, "transactions")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	transactions, count, err := a.blnk.GetAllTransactionsWithFilterAndOptions(c.Request.Context(), filters, opts, limit, offset)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Transform each transaction for response
	result := make([]*model.Transaction, len(transactions))
	for i := range transactions {
		result[i] = transformTransaction(&transactions[i])
	}

	if opts.IncludeCount {
		c.JSON(http.StatusOK, FilterResponse{Data: result, TotalCount: count})
	} else {
		c.JSON(http.StatusOK, result)
	}
}

// UpdateInflightStatus updates the status of an inflight transaction based on the provided ID and status.
// It processes the transaction in batches according to the specified status (commit or void).
// If any errors occur during processing or if the status is unsupported, it responds with an appropriate error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in updating the status or if the ID or status is missing or unsupported.
// - 200 OK: If the inflight transaction status is successfully updated.
func (a Api) UpdateInflightStatus(c *gin.Context) {
	var resp *model.Transaction
	id, passed := c.Params.Get("txID")
	var req model2.InflightUpdate
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}
	err := c.BindJSON(&req)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	cnf, err := config.Fetch()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	pg, err := pgconn.GetDBConnection(cnf)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	amount := model.ApplyPrecisionWithDBLookup(&model.Transaction{Amount: req.Amount, TransactionID: id}, pg)

	if req.PreciseAmount != nil {
		amount = req.PreciseAmount
	}

	status := req.Status
	if status == "commit" {
		transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, amount, 1, false, a.blnk.GetInflightTransactionsByParentID, a.blnk.CommitWorker)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if len(transaction) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "no transaction to commit"})
			return
		}
		resp = transformTransaction(transaction[0])
	} else if status == "void" {
		transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, amount, 1, false, a.blnk.GetInflightTransactionsByParentID, a.blnk.VoidWorker)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if len(transaction) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "no transaction to void"})
			return
		}
		resp = transformTransaction(transaction[0])
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.New("status not supported. use either commit or void")})
		return
	}

	c.JSON(http.StatusOK, resp)
}

// CreateBulkTransactions handles the creation of multiple transactions in a batch.
// It parses the request, calls the Blnk service to handle the core logic,
// and returns the appropriate HTTP response based on the result.
func (a Api) CreateBulkTransactions(c *gin.Context) {
	// Parse the request into the model struct
	var req model.BulkTransactionRequest
	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid request body: " + err.Error()})
		return
	}

	// Call the service layer method to handle bulk transaction creation
	result, err := a.blnk.CreateBulkTransactions(c.Request.Context(), &req)
	// Handle the response based on the result and error from the service layer
	if err != nil {
		// If there was an error during synchronous processing
		logrus.WithError(err).WithField("batch_id", result.BatchID).Error("bulk transaction API error")
		c.JSON(http.StatusBadRequest, gin.H{
			"error":    result.Error, // Use the detailed error from the result
			"batch_id": result.BatchID,
		})
		return
	}

	// Handle successful responses
	if req.RunAsync {
		// Async request acknowledged
		c.JSON(http.StatusAccepted, gin.H{
			"message":  "Bulk transaction processing started",
			"batch_id": result.BatchID,
			"status":   result.Status, // Should be "processing"
		})
	} else {
		// Synchronous request completed successfully
		c.JSON(http.StatusCreated, gin.H{
			"batch_id":          result.BatchID,
			"status":            result.Status, // Should be "applied" or "inflight"
			"transaction_count": result.TransactionCount,
		})
	}
}

func (a Api) GetTransactionLineage(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	lineage, err := a.blnk.GetTransactionLineage(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, lineage)
}

// RecoverQueuedTransactions triggers manual recovery of stuck queued transactions.
// Accepts an optional "threshold" query parameter (e.g. "5m", "10m") specifying
// the minimum age of transactions to recover. Defaults to 2 minutes, which is also
// the enforced minimum.
func (a Api) RecoverQueuedTransactions(c *gin.Context) {
	threshold := 2 * time.Minute
	if thresholdStr := c.Query("threshold"); thresholdStr != "" {
		parsed, err := time.ParseDuration(thresholdStr)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid threshold duration: " + err.Error()})
			return
		}
		threshold = parsed
	}

	recovered, err := a.blnk.RecoverQueuedTransactions(c.Request.Context(), threshold)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"recovered": recovered, "threshold": threshold.String()})
}
