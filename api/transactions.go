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
	"context"
	"errors"
	"io"
	"math/big"
	"net/http"
	"strconv"
	"time"

	"github.com/sirupsen/logrus"

	blnk "github.com/blnkfinance/blnk"
	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
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
			respondCode(c, apierror.ErrTxnPrecisionNotInteger, precisionErr.Error(), nil)
			return
		}
	}

	if errors.Is(err, model2.ErrPrecisionMustBeInteger) {
		respondCode(c, apierror.ErrTxnPrecisionNotInteger, err.Error(), nil)
		return
	}

	respondCode(c, apierror.ErrTxnValidation, err.Error(), nil, withLegacyKey("errors"))
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
		respondCode(c, apierror.ErrGenMalformedRequest, "Invalid input", nil)
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
		respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrTxnNotFound), withDefault(apierror.ErrTxnValidation))
		return
	}

	// Return a response with the queued transaction, properly transformed
	c.JSON(http.StatusCreated, transformTransaction(resp))
}

// refundTransactionRequest is the optional JSON body of a refund
// request. It is OPTIONAL: an empty body keeps the default behavior
// (the refund is queued for asynchronous processing).
type refundTransactionRequest struct {
	// SkipQueue, when true, processes the refund synchronously instead
	// of placing it on the transaction queue — useful for time-sensitive
	// refunds where the caller needs immediate confirmation. Mirrors the
	// skip_queue flag on Create Transaction.
	SkipQueue bool `json:"skip_queue"`
}

// RefundTransaction processes a refund for a transaction based on the given ID.
// It retrieves the transaction to be refunded and processes it in batches. If any errors
// occur during retrieval or processing, it responds with an appropriate error message.
//
// An optional JSON body {"skip_queue": true} processes the refund
// synchronously; an absent or empty body queues it (the default).
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
		respondCode(c, apierror.ErrGenMissingParameter, "id is required. pass id in the route /:id", nil)
		return
	}

	// The body is optional — the long-standing bodiless
	// `POST /refund-transaction/:id` must keep working unchanged. An
	// empty body makes encoding/json (via ShouldBindJSON) return io.EOF;
	// treat ONLY that as "no options supplied" and reject any other
	// bind error. Detecting emptiness via io.EOF is reliable regardless
	// of Content-Length / chunked transfer encoding.
	var req refundTransactionRequest
	if err := c.ShouldBindJSON(&req); err != nil && !errors.Is(err, io.EOF) {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}

	transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, big.NewInt(0), 1, false, a.blnk.GetRefundableTransactionsByParentID, a.blnk.RefundWorkerWithOptions(req.SkipQueue))
	if err != nil {
		respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrTxnNotFound), withDefault(apierror.ErrGenBadRequest))
		return
	}
	if len(transaction) == 0 {
		respondCode(c, apierror.ErrTxnNotFound, "no transaction to refund", nil)
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
		respondCode(c, apierror.ErrGenMissingParameter, "id is required. pass id in the route /:id", nil)
		return
	}

	resp, err := a.blnk.GetTransaction(c.Request.Context(), id)
	if err != nil {
		respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrTxnNotFound))
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
		respondCode(c, apierror.ErrGenMissingParameter, "reference is required. pass reference in the route /ref/:reference", nil)
		return
	}

	resp, err := a.blnk.GetTransactionByRef(c.Request.Context(), reference)
	if err != nil {
		respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrTxnNotFound))
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
			respondCode(c, apierror.ErrGenValidation, "invalid filter parameters", parseErrors,
				withLegacyKey("errors"), withLegacyValue(parseErrors))
			return
		}

		// Use the new filter method
		transactions, err := a.blnk.GetAllTransactionsWithFilter(c.Request.Context(), filters, limitInt, offsetInt)
		if err != nil {
			respondError(c, err)
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
		respondError(c, err)
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
		respondCode(c, apierror.ErrGenValidation, err.Error(), nil)
		return
	}

	transactions, count, err := a.blnk.GetAllTransactionsWithFilterAndOptions(c.Request.Context(), filters, opts, limit, offset)
	if err != nil {
		respondError(c, err)
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
		respondCode(c, apierror.ErrGenMissingParameter, "id is required. pass id in the route /:id", nil)
		return
	}
	err := c.BindJSON(&req)
	if err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}

	cnf, err := config.Fetch()
	if err != nil {
		respondError(c, err)
		return
	}
	ds, err := database.GetDBConnection(cnf)
	if err != nil {
		respondError(c, err)
		return
	}

	amount := model.ApplyPrecisionWithDBLookup(&model.Transaction{Amount: req.Amount, TransactionID: id}, ds.Conn)

	if req.PreciseAmount != nil {
		if req.PreciseAmount.Sign() <= 0 {
			respondCode(c, apierror.ErrTxnInvalidAmount, "precise_amount must be positive", nil)
			return
		}
		amount = req.PreciseAmount
	}

	status := req.Status
	if status != blnk.InflightActionCommit && status != blnk.InflightActionVoid {
		respondCode(c, apierror.ErrTxnInvalidStatusAction, "status not supported. use either commit or void", nil)
		return
	}

	// Default: route the action through the inflight-commit queue. The response
	// is the still-inflight parent plus a queued marker; the worker applies it.
	if !req.SkipQueue {
		parent, err := a.blnk.QueueInflightAction(c.Request.Context(), id, amount, status)
		if err != nil {
			if errors.Is(err, blnk.ErrInflightActionQueued) {
				respondCode(c, apierror.ErrGenConflict, "a commit or void is already queued for this transaction", nil)
				return
			}
			respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrTxnNotFound), withDefault(apierror.ErrGenBadRequest))
			return
		}
		c.JSON(http.StatusOK, queuedInflightResponse{Transaction: transformTransaction(parent), Queued: true})
		return
	}

	// skip_queue: process synchronously (legacy behavior).
	if status == blnk.InflightActionCommit {
		transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, amount, 1, false, a.blnk.GetInflightTransactionsByParentID, a.blnk.CommitWorker)
		if err != nil {
			respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrTxnNotFound), withDefault(apierror.ErrGenBadRequest))
			return
		}
		if len(transaction) == 0 {
			respondCode(c, apierror.ErrTxnNotInflight, "no transaction to commit", nil)
			return
		}
		resp = transformTransaction(transaction[0])
	} else {
		transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, amount, 1, false, a.blnk.GetInflightTransactionsByParentID, a.blnk.VoidWorker)
		if err != nil {
			respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrTxnNotFound), withDefault(apierror.ErrGenBadRequest))
			return
		}
		if len(transaction) == 0 {
			respondCode(c, apierror.ErrTxnNotInflight, "no transaction to void", nil)
			return
		}
		resp = transformTransaction(transaction[0])
	}

	c.JSON(http.StatusOK, resp)
}

// queuedInflightResponse is the body returned when a commit/void is queued. The
// embedded transaction promotes all its fields to the top level (unchanged for
// existing clients); `queued` marks that the action was accepted but not yet
// applied (the transaction is still INFLIGHT).
type queuedInflightResponse struct {
	*model.Transaction
	Queued bool `json:"queued"`
}

// CreateBulkTransactions handles the creation of multiple transactions in a batch.
// It parses the request, calls the Blnk service to handle the core logic,
// and returns the appropriate HTTP response based on the result.
func (a Api) CreateBulkTransactions(c *gin.Context) {
	var req model2.BulkTransactionRequest
	if err := c.BindJSON(&req); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, "Invalid request body: "+err.Error(), nil)
		return
	}

	if len(req.Transactions) == 0 {
		respondCode(c, apierror.ErrTxnBulkEmpty, "transactions cannot be empty", nil)
		return
	}
	if len(req.Transactions) > model2.MaxBulkTransactionItems {
		respondCode(c, apierror.ErrTxnBulkLimitExceeded,
			"too many transactions; max is "+strconv.Itoa(model2.MaxBulkTransactionItems), nil)
		return
	}

	for i, transaction := range req.Transactions {
		if transaction == nil {
			respondCode(c, apierror.ErrTxnValidation, "transactions["+strconv.Itoa(i)+"] is required",
				gin.H{"index": i}, withLegacyKey("errors"))
			return
		}

		if err := transaction.ValidateRecordTransaction(); err != nil {
			respondCode(c, apierror.ErrTxnValidation, "transactions["+strconv.Itoa(i)+"]: "+err.Error(),
				gin.H{"index": i}, withLegacyKey("errors"))
			return
		}
	}

	bulkReq := req.ToBulkTransactionRequest()

	// Call the service layer method to handle bulk transaction creation
	result, err := a.blnk.CreateBulkTransactions(c.Request.Context(), bulkReq)
	// Handle the response based on the result and error from the service layer
	if err != nil {
		// Some failures (e.g. async-bulk semaphore saturation) return a nil
		// result alongside a typed API error; surface that error's own status
		// code instead of dereferencing the nil result.
		if result == nil {
			respondError(c, err, withDefault(apierror.ErrGenBadRequest))
			return
		}
		// If there was an error during synchronous processing.
		// classifyMessage picks the most specific catalog code from the
		// detailed result error; batch_id stays a top-level sibling field.
		logrus.WithError(err).WithField("batch_id", result.BatchID).Error("bulk transaction API error")
		code, ok := classifyMessage(result.Error)
		if !ok {
			code = apierror.ErrGenBadRequest
		}
		c.JSON(apierror.StatusForCode(code), gin.H{
			"error":        result.Error, // Use the detailed error from the result
			"batch_id":     result.BatchID,
			"error_detail": apierror.NewErrorResponse(code, result.Error, gin.H{"batch_id": result.BatchID}).Error,
		})
		return
	}

	// Handle successful responses
	if bulkReq.RunAsync {
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
		respondCode(c, apierror.ErrGenMissingParameter, "id is required. pass id in the route /:id", nil)
		return
	}

	lineage, err := a.blnk.GetTransactionLineage(c.Request.Context(), id)
	if err != nil {
		respondError(c, err, withUpgrade(apierror.ErrGenNotFound, apierror.ErrTxnNotFound))
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
			respondCode(c, apierror.ErrGenValidation, "invalid threshold duration: "+err.Error(), nil)
			return
		}
		threshold = parsed
	}

	recovered, err := a.blnk.RecoverQueuedTransactions(c.Request.Context(), threshold)
	if err != nil {
		respondError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"recovered": recovered, "threshold": threshold.String()})
}

// bulkInflightMaxWorkers caps the worker-pool size used by the bulk handlers.
// 8 is a deliberate compromise: large enough to win meaningful concurrency
// vs sequential N round-trips, small enough not to flood the lock service or
// connection pool when many bulk calls are in flight.
const bulkInflightMaxWorkers = 8

// toAPIResults converts service-layer BulkInflightOutcome values to the
// public BulkInflightResult shape returned by the handlers.
func toAPIResults(outcomes []blnk.BulkInflightOutcome) (model2.BulkInflightResponse, int) {
	resp := model2.BulkInflightResponse{
		Results: make([]model2.BulkInflightResult, len(outcomes)),
	}
	for i, o := range outcomes {
		r := model2.BulkInflightResult{TransactionID: o.TransactionID}
		if o.Err == nil {
			r.Status = "succeeded"
			resp.Succeeded++
		} else {
			r.Status = "failed"
			r.Code = o.Code
			r.Message = o.Err.Error()
			resp.Failed++
		}
		resp.Results[i] = r
	}
	return resp, resp.Failed
}

// BulkVoidInflight voids many independently-created inflight transactions
// in a single call. Each id is processed in its own worker; per-item
// failures are reported in the response body and do not abort the rest of
// the batch. Returns 200 with the breakdown even when some items fail.
//
// Responses:
// - 400 Bad Request: malformed payload, empty list, or > MaxBulkInflightItems.
// - 200 OK: bulk processed; see succeeded/failed counts in body.
func (a Api) BulkVoidInflight(c *gin.Context) {
	var req model2.BulkInflightVoidRequest
	if err := c.BindJSON(&req); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}
	if len(req.TransactionIDs) == 0 {
		respondCode(c, apierror.ErrTxnBulkEmpty, "transaction_ids cannot be empty", nil)
		return
	}
	if len(req.TransactionIDs) > model2.MaxBulkInflightItems {
		respondCode(c, apierror.ErrTxnBulkLimitExceeded, "too many transaction_ids; max is "+strconv.Itoa(model2.MaxBulkInflightItems), nil)
		return
	}

	items := make([]blnk.BulkInflightItem, len(req.TransactionIDs))
	for i, id := range req.TransactionIDs {
		items[i] = blnk.BulkInflightItem{TransactionID: id}
	}

	if !req.SkipQueue {
		c.JSON(http.StatusOK, a.queueBulkInflight(c.Request.Context(), blnk.InflightActionVoid, items))
		return
	}

	outcomes, err := a.blnk.BulkInflightUpdate(c.Request.Context(), blnk.BulkInflightVoid, items, bulkInflightMaxWorkers)
	if err != nil {
		respondError(c, err, withDefault(apierror.ErrGenBadRequest))
		return
	}
	resp, _ := toAPIResults(outcomes)
	c.JSON(http.StatusOK, resp)
}

// queueBulkInflight enqueues a commit/void per item and builds the per-item
// response: QUEUED on success, ALREADY_QUEUED when one is already in flight
// (both count as accepted), and a classified failure code if pre-validation
// rejects the item synchronously.
func (a Api) queueBulkInflight(ctx context.Context, action string, items []blnk.BulkInflightItem) model2.BulkInflightResponse {
	resp := model2.BulkInflightResponse{Results: make([]model2.BulkInflightResult, 0, len(items))}
	for _, it := range items {
		_, err := a.blnk.QueueInflightAction(ctx, it.TransactionID, it.Amount, action)
		switch {
		case err == nil:
			resp.Results = append(resp.Results, model2.BulkInflightResult{TransactionID: it.TransactionID, Status: "queued", Code: "QUEUED"})
			resp.Succeeded++
		case errors.Is(err, blnk.ErrInflightActionQueued):
			resp.Results = append(resp.Results, model2.BulkInflightResult{TransactionID: it.TransactionID, Status: "queued", Code: "ALREADY_QUEUED", Message: err.Error()})
			resp.Succeeded++
		default:
			resp.Results = append(resp.Results, model2.BulkInflightResult{TransactionID: it.TransactionID, Status: "failed", Code: blnk.ClassifyInflightError(err), Message: err.Error()})
			resp.Failed++
		}
	}
	return resp
}

// BulkCommitInflight commits many independently-created inflight
// transactions in a single call. Unlike the void variant, each item may
// carry an optional `amount` / `precise_amount` for partial commits; zero
// or missing means commit the full remaining inflight amount.
//
// Responses:
// - 400 Bad Request: malformed payload, empty list, or > MaxBulkInflightItems.
// - 200 OK: bulk processed; see succeeded/failed counts in body.
func (a Api) BulkCommitInflight(c *gin.Context) {
	var req model2.BulkInflightCommitRequest
	if err := c.BindJSON(&req); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}
	if len(req.Transactions) == 0 {
		respondCode(c, apierror.ErrTxnBulkEmpty, "transactions cannot be empty", nil)
		return
	}
	if len(req.Transactions) > model2.MaxBulkInflightItems {
		respondCode(c, apierror.ErrTxnBulkLimitExceeded, "too many transactions; max is "+strconv.Itoa(model2.MaxBulkInflightItems), nil)
		return
	}

	cnf, err := config.Fetch()
	if err != nil {
		respondError(c, err)
		return
	}
	ds, err := database.GetDBConnection(cnf)
	if err != nil {
		respondError(c, err)
		return
	}

	items := make([]blnk.BulkInflightItem, len(req.Transactions))
	for i, it := range req.Transactions {
		// Match single-tx semantics: precise_amount wins; else apply precision
		// via the same DB-lookup helper used by UpdateInflightStatus.
		var amount *big.Int
		if it.PreciseAmount != nil {
			amount = it.PreciseAmount
		} else {
			amount = model.ApplyPrecisionWithDBLookup(&model.Transaction{Amount: it.Amount, TransactionID: it.TransactionID}, ds.Conn)
		}
		items[i] = blnk.BulkInflightItem{TransactionID: it.TransactionID, Amount: amount}
	}

	if !req.SkipQueue {
		c.JSON(http.StatusOK, a.queueBulkInflight(c.Request.Context(), blnk.InflightActionCommit, items))
		return
	}

	outcomes, err := a.blnk.BulkInflightUpdate(c.Request.Context(), blnk.BulkInflightCommit, items, bulkInflightMaxWorkers)
	if err != nil {
		respondError(c, err, withDefault(apierror.ErrGenBadRequest))
		return
	}
	resp, _ := toAPIResults(outcomes)
	c.JSON(http.StatusOK, resp)
}
