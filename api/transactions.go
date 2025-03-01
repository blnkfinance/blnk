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
	"fmt"
	"net/http"

	"github.com/sirupsen/logrus"

	model2 "github.com/jerry-enebeli/blnk/api/model"
	"github.com/jerry-enebeli/blnk/model"

	"github.com/gin-gonic/gin"
)

// transformTransaction prepares a transaction for API response by ensuring
// that metadata fields are properly represented as first-class fields.
// This maintains backward compatibility while keeping responses clean.
func transformTransaction(txn *model.Transaction) *model.Transaction {
	// Create a copy to avoid modifying the original
	result := *txn

	// Check if metadata exists and has inflight information
	if result.MetaData != nil {
		// Check for inflight flag in metadata and move it to the main field
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
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
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
		logrus.Error(err)
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid input"})
		return
	}

	// Validate the transaction data
	err := newTransaction.ValidateRecordTransaction()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	// Queue the transaction using the Blnk service
	resp, err := a.blnk.QueueTransaction(c.Request.Context(), newTransaction.ToTransaction())
	if err != nil {
		logrus.Error(err)
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
	transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, 0, 1, false, a.blnk.GetRefundableTransactionsByParentID, a.blnk.RefundWorker)
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

	status := req.Status
	if status == "commit" {
		transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, req.Amount, 1, false, a.blnk.GetInflightTransactionsByParentID, a.blnk.CommitWorker)
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
		transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, req.Amount, 1, false, a.blnk.GetInflightTransactionsByParentID, a.blnk.VoidWorker)
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
// It binds the incoming JSON request containing a list of transactions, assigns a batch ID,
// and processes each transaction as inflight.
//
// If atomic is true: Any failure will cause all transactions to be rolled back
// If atomic is false: Failures will be reported but previous transactions remain unaffected
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or queuing any transaction.
// - 201 Created: If all transactions are successfully queued in the batch.
func (a Api) CreateBulkTransactions(c *gin.Context) {
	var req struct {
		Transactions []*model.Transaction `json:"transactions"`
		Inflight     bool                 `json:"inflight"`
		Atomic       bool                 `json:"atomic"`
	}

	if err := c.BindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Generate batch ID (parent transaction ID)
	batchID := model.GenerateUUIDWithSuffix("bulk")
	logrus.Infof("Creating bulk transaction batch %s with %d transactions (atomic: %v, inflight: %v)",
		batchID, len(req.Transactions), req.Atomic, req.Inflight)

	// Process transactions in batch
	if err := a.processBulkTransactions(c.Request.Context(), req.Transactions, batchID, req.Inflight); err != nil {
		// Handle failure based on atomicity setting and transaction type
		a.handleBulkTransactionFailure(c, err, batchID, req.Atomic, req.Inflight)
		return
	}

	// Determine status based on transaction type (Go doesn't have ternary operators)
	var status string
	if req.Inflight {
		status = "inflight"
	} else {
		status = "applied"
	}

	// Success response
	c.JSON(http.StatusCreated, gin.H{
		"batch_id":          batchID,
		"status":            status,
		"transaction_count": len(req.Transactions),
	})
}

// processBulkTransactions prepares and queues all transactions in a batch with the given batch ID
func (a Api) processBulkTransactions(ctx context.Context, transactions []*model.Transaction, batchID string, inflight bool) error {
	for i, txn := range transactions {
		// Set transaction properties
		txn.Inflight = inflight
		txn.SkipQueue = true
		txn.ParentTransaction = batchID

		// Add sequence number to metadata
		if txn.MetaData == nil {
			txn.MetaData = make(map[string]interface{})
		}
		txn.MetaData["sequence"] = i + 1

		// Queue the transaction
		if _, err := a.blnk.QueueTransaction(ctx, txn); err != nil {
			return fmt.Errorf("failed to queue transaction %d: %w", i, err)
		}
	}
	return nil
}

// handleBulkTransactionFailure handles failures in bulk transaction processing
// based on whether the operation was atomic or not, and whether transactions are inflight
func (a Api) handleBulkTransactionFailure(c *gin.Context, err error, batchID string, isAtomic bool, isInflight bool) {
	logrus.Errorf("Bulk transaction error for batch %s: %s", batchID, err.Error())

	if isAtomic {
		var rollbackErr error
		var action string

		if isInflight {
			// Void all inflight transactions in this batch
			_, rollbackErr = a.blnk.ProcessTransactionInBatches(
				context.Background(),
				batchID,
				0,
				1,
				false,
				a.blnk.GetInflightTransactionsByParentID,
				a.blnk.VoidWorker,
			)
			action = "voided"
		} else {
			// Refund all non-inflight transactions in this batch
			_, rollbackErr = a.blnk.ProcessTransactionInBatches(
				context.Background(),
				batchID,
				0,
				1,
				false,
				a.blnk.GetRefundableTransactionsByParentID,
				a.blnk.RefundWorker,
			)
			action = "refunded"
		}

		if rollbackErr != nil {
			logrus.Errorf("Failed to rollback batch transactions for %s: %s", batchID, rollbackErr.Error())
		} else {
			logrus.Infof("Successfully rolled back atomic batch %s (%s)", batchID, action)
		}

		c.JSON(http.StatusBadRequest, gin.H{
			"error":    fmt.Sprintf("%s. All transactions in this batch have been %s.", err.Error(), action),
			"batch_id": batchID,
		})
	} else {
		// If not atomic, just return the error without rollback
		c.JSON(http.StatusBadRequest, gin.H{
			"error":    fmt.Sprintf("%s. Previous transactions were not rolled back.", err.Error()),
			"batch_id": batchID,
		})
	}
}
