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

package blnk

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

func (l *Blnk) processBulkTransactions(ctx context.Context, transactions []*model.Transaction, batchID string, inflight bool, skipQueue bool) error {
	for i, txn := range transactions {
		// Set transaction properties
		txn.Inflight = inflight
		txn.SkipQueue = skipQueue // Process synchronously within the batch context first
		txn.ParentTransaction = batchID

		// Add sequence number to metadata
		if txn.MetaData == nil {
			txn.MetaData = make(map[string]interface{})
		}
		txn.MetaData["sequence"] = i + 1

		// Queue the transaction (which will record it if SkipQueue is true)
		if _, err := l.QueueTransaction(ctx, txn); err != nil {
			// Create a more descriptive error that includes transaction reference details
			return fmt.Errorf("failed to queue transaction %d (Reference: %s, Source: %s, Destination: %s, Amount: %.2f): %w",
				i+1, txn.Reference, txn.Source, txn.Destination, txn.Amount, err)
		}
	}
	return nil
}

// rollbackBatchTransactions performs a rollback of transactions in a batch
// Returns the action performed (voided/refunded) and any error that occurred
func (l *Blnk) rollbackBatchTransactions(ctx context.Context, batchID string, isInflight bool) (string, error) {
	var action string
	var rollbackErr error

	if isInflight {
		action, rollbackErr = l.voidInflightBatchTransactions(ctx, batchID)
	} else {
		action, rollbackErr = l.refundNonInflightBatchTransactions(ctx, batchID)
	}

	l.logRollbackResult(batchID, action, rollbackErr)
	return action, rollbackErr
}

// voidInflightBatchTransactions voids all inflight transactions in a batch
func (l *Blnk) voidInflightBatchTransactions(ctx context.Context, batchID string) (string, error) {
	_, err := l.ProcessTransactionInBatches(
		ctx,
		batchID,
		big.NewInt(0),
		1, // Assuming 1 worker is sufficient for rollback, adjust if needed
		false,
		l.GetInflightTransactionsByParentID,
		l.VoidWorker,
	)
	return "voided", err
}

// refundNonInflightBatchTransactions refunds all non-inflight transactions in a batch
func (l *Blnk) refundNonInflightBatchTransactions(ctx context.Context, batchID string) (string, error) {
	_, err := l.ProcessTransactionInBatches(
		ctx,
		batchID,
		big.NewInt(0),
		1, // Assuming 1 worker is sufficient for rollback, adjust if needed
		false,
		l.GetRefundableTransactionsByParentID,
		l.RefundWorker,
	)
	return "refunded", err
}

// logRollbackResult logs the outcome of a rollback operation
func (l *Blnk) logRollbackResult(batchID string, action string, err error) {
	if err != nil {
		logrus.WithError(err).WithField("batch_id", batchID).Error("failed to rollback batch transactions")
	} else {
		logrus.WithFields(logrus.Fields{
			"batch_id": batchID,
			"action":   action,
		}).Info("successfully rolled back atomic batch")
	}
}

// sendBulkTransactionWebhook sends a webhook notification for a bulk transaction result
func (l *Blnk) sendBulkTransactionWebhook(batchID, status, errorMsg string, transactionCount int) {
	// Create payload with or without error info depending on status
	payload := map[string]interface{}{
		"batch_id":  batchID,
		"status":    status,
		"timestamp": time.Now(),
	}

	// Only include transaction count for success cases
	if status != "failed" {
		payload["transaction_count"] = transactionCount
	}

	// Include error details for failure cases
	if status == "failed" && errorMsg != "" {
		payload["error"] = errorMsg
	}

	err := l.SendWebhook(NewWebhook{
		Event:   "bulk_transaction." + status,
		Payload: payload,
	})
	if err != nil {
		logrus.WithError(err).WithField("batch_id", batchID).Error("failed to send webhook notification")
	}
}

// handleAsyncBulkTransactionFailure handles failures in asynchronous processing
// and builds a detailed error message including rollback status
func (l *Blnk) handleAsyncBulkTransactionFailure(ctx context.Context, err error, batchID string, isAtomic bool, isInflight bool) {
	logrus.WithError(err).WithField("batch_id", batchID).Error("async bulk transaction error")

	var errorMessage string

	if isAtomic {
		action, rollbackErr := l.rollbackBatchTransactions(ctx, batchID, isInflight)

		if rollbackErr != nil {
			errorMessage = fmt.Sprintf("%s. Failed to roll back all transactions: %s", err.Error(), rollbackErr.Error())
			logrus.WithError(rollbackErr).WithField("batch_id", batchID).Error("failed to roll back batch")
		} else {
			errorMessage = fmt.Sprintf("%s. All transactions in this batch have been %s.", err.Error(), action)
			logrus.WithFields(logrus.Fields{
				"batch_id": batchID,
				"action":   action,
			}).Info("successfully rolled back async batch")
		}
	} else {
		// If not atomic, just include the original error and note about no rollback
		errorMessage = fmt.Sprintf("%s. Previous transactions were not rolled back.", err.Error())
	}

	// Send webhook with the complete error message including rollback status
	l.sendBulkTransactionWebhook(batchID, "failed", errorMessage, 0) // 0 count for failed batch
}

// CreateBulkTransactions handles the creation of multiple transactions in a batch.
// If atomic is true: Any failure will cause all transactions to be rolled back (or voided if inflight).
// If atomic is false: Failures will stop processing but previous transactions remain unaffected.
// If run_async is true: Processing happens in background with webhook notifications.
func (l *Blnk) CreateBulkTransactions(ctx context.Context, req *model.BulkTransactionRequest) (*model.BulkTransactionResult, error) {
	ctx, span := tracer.Start(ctx, "Blnk.CreateBulkTransactions")
	defer span.End()

	// Generate batch ID (parent transaction ID)
	batchID := model.GenerateUUIDWithSuffix("bulk")
	span.SetAttributes(attribute.String("batch.id", batchID))

	// Check if this should be run asynchronously
	if req.RunAsync {
		if !asyncBulkSemaphore.TryAcquire(1) {
			return nil, apierror.NewAPIError(
				apierror.ErrRateLimited,
				"too many async bulk operations in progress, try again later",
				nil,
			)
		}

		// processBulkTransactions mutates each transaction (status, metadata,
		// parent); clone them so the background goroutine never races the caller
		// still holding the request's transactions.
		asyncTxns := cloneTransactionsForAsync(req.Transactions)

		// Start processing in background
		go func() {
			defer asyncBulkSemaphore.Release(1)

			// Create a background context with timeout
			bgCtx, cancel := context.WithTimeout(context.Background(), 30*time.Minute)
			defer cancel()

			logrus.Infof("Starting async bulk transaction batch %s with %d transactions (atomic: %v, inflight: %v)",
				batchID, len(asyncTxns), req.Atomic, req.Inflight)

			// Process transactions in batch
			err := l.processBulkTransactions(bgCtx, asyncTxns, batchID, req.Inflight, req.SkipQueue)

			if err != nil {
				// Handle failure (rollback if atomic, send webhook)
				l.handleAsyncBulkTransactionFailure(bgCtx, err, batchID, req.Atomic, req.Inflight)
			} else {
				// Send webhook notification for success
				status := "inflight"
				if !req.Inflight {
					status = "applied"
				}
				l.sendBulkTransactionWebhook(batchID, status, "", len(req.Transactions))
				logrus.Infof("Completed async bulk transaction batch %s successfully", batchID)
			}
		}()

		// Return immediate response indicating async processing started
		return &model.BulkTransactionResult{
			BatchID: batchID,
			Status:  "processing", // Indicate that it's running in the background
		}, nil
	}

	// Synchronous processing
	logrus.Infof("Starting sync bulk transaction batch %s with %d transactions (atomic: %v, inflight: %v)",
		batchID, len(req.Transactions), req.Atomic, req.Inflight)

	// Process transactions in batch
	if err := l.processBulkTransactions(ctx, req.Transactions, batchID, req.Inflight, req.SkipQueue); err != nil {
		span.RecordError(err)
		logrus.WithError(err).WithField("batch_id", batchID).Error("sync bulk transaction error")

		var responseError string
		if req.Atomic {
			action, rollbackErr := l.rollbackBatchTransactions(ctx, batchID, req.Inflight)
			if rollbackErr != nil {
				responseError = fmt.Sprintf("%s. Failed to roll back all transactions: %s", err.Error(), rollbackErr.Error())
			} else {
				responseError = fmt.Sprintf("%s. All transactions in this batch have been %s.", err.Error(), action)
			}
		} else {
			responseError = fmt.Sprintf("%s. Previous transactions were not rolled back.", err.Error())
		}

		// Return error result for synchronous failure
		return &model.BulkTransactionResult{
			BatchID: batchID,
			Status:  "failed",
			Error:   responseError,
		}, errors.New(responseError) // Return the error itself as well
	}

	// Synchronous success
	status := "inflight"
	if !req.Inflight {
		status = "applied"
	}

	logrus.Infof("Completed sync bulk transaction batch %s successfully", batchID)
	return &model.BulkTransactionResult{
		BatchID:          batchID,
		Status:           status,
		TransactionCount: len(req.Transactions),
	}, nil
}
