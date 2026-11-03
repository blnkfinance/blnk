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
	"strings"
	"time"

	"github.com/blnkfinance/blnk/internal/apierror"
	redlock "github.com/blnkfinance/blnk/internal/lock"
	"github.com/blnkfinance/blnk/model"
	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/attribute"
)

// BulkTransactionError reports the failure of a synchronous bulk batch.
//
// Message is the client-facing sentence: the failing item's error plus what
// happened to the rest of the batch (rolled back, or left in place). cause is
// the error the item itself raised, kept so callers can unwrap to it — the
// API layer resolves the response's error code from there, which is how a
// missing balance inside a batch reports BAL_NOT_FOUND rather than being
// text-matched into TXN_NOT_FOUND.
type BulkTransactionError struct {
	Message string
	cause   error
}

func (e *BulkTransactionError) Error() string { return e.Message }

func (e *BulkTransactionError) Unwrap() error { return e.cause }

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
			// Create a more descriptive error that includes transaction reference details.
			// The index is 0-based to match the position reported by the request
			// validation errors for the same batch ("transactions[0]", and the
			// "index" field of error_detail.details), so a client pointing at the
			// offending item does not have to know which layer rejected it.
			return fmt.Errorf("failed to queue transactions[%d] (Reference: %s, Source: %s, Destination: %s, Amount: %.2f): %w",
				i, txn.Reference, txn.Source, txn.Destination, txn.Amount, err)
		}
	}
	return nil
}

// rollbackBatchTransactions performs a rollback of transactions in a batch.
// Voids and refunds are applied before it returns. It returns the action
// performed (voided/refunded), how many transactions it rolled back, and any
// error that occurred.
func (l *Blnk) rollbackBatchTransactions(ctx context.Context, batchID string, isInflight bool) (string, int, error) {
	action := "refunded"
	var count int
	var rollbackErr error

	if isInflight {
		action = "voided"
		count, rollbackErr = l.voidInflightBatchTransactions(ctx, batchID)
	} else {
		count, rollbackErr = l.refundBatchTransactions(ctx, batchID)
	}

	l.logRollbackResult(batchID, action, rollbackErr)
	return action, count, rollbackErr
}

// voidInflightBatchTransactions voids all inflight transactions in a batch
// and returns how many it voided.
func (l *Blnk) voidInflightBatchTransactions(ctx context.Context, batchID string) (int, error) {
	voided, err := l.ProcessTransactionInBatches(
		ctx,
		batchID,
		big.NewInt(0),
		1, // Assuming 1 worker is sufficient for rollback, adjust if needed
		false,
		l.GetInflightTransactionsByParentID,
		l.VoidWorker,
	)
	return len(voided), err
}

// refundBatchTransactions refunds the applied transactions of a batch and
// returns how many of them are refunded. The refunds are applied here rather
// than queued, so the balances are back when it returns. Each refund keeps the
// metadata of what it refunds, QUEUED_PARENT_TRANSACTION included, so it is
// listed under the batch with it. The lookup leaves the refunds out, and a
// transaction refunded by an earlier, incomplete pass counts as refunded and
// is not refunded again, so running it twice changes nothing.
func (l *Blnk) refundBatchTransactions(ctx context.Context, batchID string) (int, error) {
	// Collect the applied transactions before refunding any, so the pages do
	// not shift while the refunds are written.
	batchSize := l.Config().Transaction.BatchSize
	if batchSize <= 0 {
		batchSize = 100
	}
	var applied []*model.Transaction
	for offset := int64(0); ; offset += int64(batchSize) {
		page, err := l.GetRefundableTransactionsByParentID(ctx, batchID, batchSize, offset)
		if err != nil {
			return 0, err
		}
		applied = append(applied, page...)
		if len(page) < batchSize {
			break
		}
	}

	var refunded int
	var errs []error
	for _, txn := range applied {
		_, err := l.RefundTransactionWithOptions(ctx, txn.TransactionID, RefundOptions{SkipQueue: true})
		if err != nil && !isTransactionAlreadyRefundedError(err) {
			errs = append(errs, err)
			continue
		}
		refunded++
	}
	return refunded, errors.Join(errs...)
}

// batchFailureMessage says why an atomic batch failed and what its rollback
// did. It is built after the rollback, so it reports refunds and voids that
// have been applied, not ones still to come.
func batchFailureMessage(err error, isInflight bool, count int, rollbackErr error) string {
	action, kind, nothing := "refunded", "applied", "Nothing from this batch had been applied, so nothing was refunded"
	if isInflight {
		action, kind, nothing = "voided", "inflight", "Nothing from this batch was inflight yet, so nothing was voided"
	}
	switch {
	case rollbackErr != nil:
		return fmt.Sprintf("%s. Failed to roll back all transactions: %s", err.Error(), rollbackErr.Error())
	case count == 0:
		return fmt.Sprintf("%s. %s; the remaining transactions will not be processed.", err.Error(), nothing)
	default:
		return fmt.Sprintf("%s. %d %s transaction(s) from this batch have been %s; the remaining transactions will not be processed.", err.Error(), count, kind, action)
	}
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
	err := l.SendWebhook(bulkTransactionWebhook(batchID, status, errorMsg, transactionCount))
	if err != nil {
		logrus.WithError(err).WithField("batch_id", batchID).Error("failed to send webhook notification")
	}
}

// bulkTransactionWebhook builds the bulk_transaction.<status> notification.
func bulkTransactionWebhook(batchID, status, errorMsg string, transactionCount int) NewWebhook {
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

	return NewWebhook{
		Event:   "bulk_transaction." + status,
		Payload: payload,
	}
}

// handleAsyncBulkTransactionFailure handles failures in asynchronous processing
// and builds a detailed error message including rollback status
func (l *Blnk) handleAsyncBulkTransactionFailure(ctx context.Context, err error, batchID string, isAtomic bool, isInflight bool) {
	logrus.WithError(err).WithField("batch_id", batchID).Error("async bulk transaction error")

	var errorMessage string

	if isAtomic {
		_, count, rollbackErr := l.rollbackBatchTransactions(ctx, batchID, isInflight)
		errorMessage = batchFailureMessage(err, isInflight, count, rollbackErr)
	} else {
		// If not atomic, just include the original error and note about no rollback
		errorMessage = fmt.Sprintf("%s. Previous transactions were not rolled back.", err.Error())
	}

	// Send webhook with the complete error message including rollback status
	l.sendBulkTransactionWebhook(batchID, "failed", errorMessage, 0) // 0 count for failed batch
}

// ErrAtomicBatchFailed is the rejection reason of a queued item whose atomic
// batch had already failed when the worker reached it.
var ErrAtomicBatchFailed = errors.New("another transaction in this atomic batch was rejected")

// atomicBatchMarkTTL is how long the marks of a failed queued atomic batch
// are kept. It has to outlast the batch's items still waiting in the queue,
// retries included.
const atomicBatchMarkTTL = 7 * 24 * time.Hour

func atomicBatchLockKey(batchID string) string     { return "atomic-batch:" + batchID }
func atomicBatchFailedKey(batchID string) string   { return "atomic-batch-failed:" + batchID }
func atomicBatchReportedKey(batchID string) string { return "atomic-batch-reported:" + batchID }
func atomicBatchProgressKey(batchID string) string { return "atomic-batch-progress:" + batchID }
func atomicBatchPendingKey(batchID string) string  { return "atomic-batch-pending:" + batchID }

// queuedAtomicBatchID returns the batch id of txn when it is a queued item of
// an atomic bulk request. A refund of such an item copies its metadata, but
// the rollback applies its refunds directly, and a queued refund gets its own
// QUEUED_PARENT_TRANSACTION, so a refund is not taken for a batch item.
func queuedAtomicBatchID(txn *model.Transaction) (string, bool) {
	if !txn.Atomic || txn.MetaData == nil {
		return "", false
	}
	batchID, _ := txn.MetaData["QUEUED_PARENT_TRANSACTION"].(string)
	if !strings.HasPrefix(batchID, "bulk_") {
		return "", false
	}
	return batchID, true
}

// WithQueuedAtomicBatch runs process for a queued item of an atomic bulk
// request while holding the batch lock, and tells it whether the batch has
// already failed. The items of one batch are thus applied one at a time: when
// one is rejected, the rollback sees every item applied before it, and no item
// is applied after it. When the item has landed, it reports the batch if that
// was the last item an async request was waiting for. Any other transaction
// runs process directly.
func (l *Blnk) WithQueuedAtomicBatch(ctx context.Context, txn *model.Transaction, process func(batchFailed bool) error) error {
	batchID, ok := queuedAtomicBatchID(txn)
	if !ok {
		return process(false)
	}

	locker := redlock.NewLocker(l.redis, atomicBatchLockKey(batchID), model.GenerateUUIDWithSuffix("loc"))
	lockCfg := l.Config().Transaction
	var err error
	if lockCfg.LockWaitTimeout > 0 {
		err = locker.WaitLock(ctx, lockCfg.LockDuration, lockCfg.LockWaitTimeout)
	} else {
		err = locker.Lock(ctx, lockCfg.LockDuration)
	}
	if err != nil {
		return fmt.Errorf("failed to acquire lock for atomic batch %s: %w", batchID, err)
	}
	defer l.releaseSingleLock(ctx, locker)

	failed, err := l.redis.Exists(ctx, atomicBatchFailedKey(batchID)).Result()
	if err != nil {
		return fmt.Errorf("failed to check atomic batch %s: %w", batchID, err)
	}
	if err := process(failed > 0); err != nil || failed > 0 {
		return err
	}
	return l.reportLandedQueuedAtomicBatch(ctx, batchID, txn)
}

// trackQueuedAtomicBatch records the size of an async atomic batch before its
// items are queued. The worker reports such a batch itself, once all of its
// items have landed.
func (l *Blnk) trackQueuedAtomicBatch(ctx context.Context, batchID string, size int) error {
	key := atomicBatchProgressKey(batchID)
	pipe := l.redis.TxPipeline()
	pipe.HSet(ctx, key, "size", size)
	pipe.Expire(ctx, key, atomicBatchMarkTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to track atomic batch %s: %w", batchID, err)
	}
	return nil
}

// registerQueuedAtomicBatchItem records the tasks an item of a tracked batch
// is about to enqueue: the item itself, or one per split leg. It runs before
// they are enqueued, so no task lands before the batch knows of it, and it
// adds the tasks before it counts the item as queued.
func (l *Blnk) registerQueuedAtomicBatchItem(ctx context.Context, item *model.Transaction, tasks []*model.Transaction) error {
	batchID, ok := queuedAtomicBatchID(item)
	if !ok {
		return nil
	}
	progressKey := atomicBatchProgressKey(batchID)
	tracked, err := l.redis.Exists(ctx, progressKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check atomic batch %s: %w", batchID, err)
	}
	if tracked == 0 {
		return nil
	}

	if len(tasks) == 0 {
		tasks = []*model.Transaction{item}
	}
	refs := make([]interface{}, len(tasks))
	for i, task := range tasks {
		refs[i] = task.Reference
	}
	pendingKey := atomicBatchPendingKey(batchID)
	pipe := l.redis.TxPipeline()
	pipe.SAdd(ctx, pendingKey, refs...)
	pipe.Expire(ctx, pendingKey, atomicBatchMarkTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("failed to register item of atomic batch %s: %w", batchID, err)
	}
	if err := l.redis.HIncrBy(ctx, progressKey, "queued", 1).Err(); err != nil {
		return fmt.Errorf("failed to register item of atomic batch %s: %w", batchID, err)
	}
	return nil
}

// reportLandedQueuedAtomicBatch counts txn as landed and, when every item of
// a tracked batch has been queued and every task of it has landed, reports
// the batch with bulk_transaction.applied (or inflight). It reads the queued
// count before the pending tasks, so the tasks of every counted item are in
// the set it reads. An item rejected in process failed the batch instead,
// and the rollback reports that. Both reports take the same mark, so a batch
// gets one of them, once.
func (l *Blnk) reportLandedQueuedAtomicBatch(ctx context.Context, batchID string, txn *model.Transaction) error {
	failed, err := l.redis.Exists(ctx, atomicBatchFailedKey(batchID)).Result()
	if err != nil {
		return fmt.Errorf("failed to check atomic batch %s: %w", batchID, err)
	}
	if failed > 0 {
		return nil
	}

	var progress struct {
		Size   int `redis:"size"`
		Queued int `redis:"queued"`
	}
	if err := l.redis.HMGet(ctx, atomicBatchProgressKey(batchID), "size", "queued").Scan(&progress); err != nil {
		return fmt.Errorf("failed to check atomic batch %s: %w", batchID, err)
	}
	if progress.Size == 0 {
		return nil
	}
	pendingKey := atomicBatchPendingKey(batchID)
	if err := l.redis.SRem(ctx, pendingKey, txn.Reference).Err(); err != nil {
		return fmt.Errorf("failed to update atomic batch %s: %w", batchID, err)
	}
	if progress.Queued < progress.Size {
		return nil
	}
	pending, err := l.redis.SCard(ctx, pendingKey).Result()
	if err != nil {
		return fmt.Errorf("failed to check atomic batch %s: %w", batchID, err)
	}
	if pending > 0 {
		return nil
	}

	reportedKey := atomicBatchReportedKey(batchID)
	first, err := l.redis.SetNX(ctx, reportedKey, time.Now().UTC().Format(time.RFC3339), atomicBatchMarkTTL).Result()
	if err != nil {
		return fmt.Errorf("failed to mark atomic batch %s as reported: %w", batchID, err)
	}
	if !first {
		return nil
	}
	status := "applied"
	if txn.Inflight {
		status = "inflight"
	}
	if err := l.SendWebhook(bulkTransactionWebhook(batchID, status, "", progress.Size)); err != nil {
		if delErr := l.redis.Del(ctx, reportedKey).Err(); delErr != nil {
			logrus.WithError(delErr).WithField("batch_id", batchID).Error("failed to clear the report mark of atomic batch")
		}
		return fmt.Errorf("failed to report atomic batch %s: %w", batchID, err)
	}
	logrus.Infof("Completed async bulk transaction batch %s successfully", batchID)
	return nil
}

// markQueuedAtomicBatchFailed marks a queued atomic batch as failed, so the
// worker rejects its remaining items instead of applying them. Marking a batch
// that is already marked is not an error.
func (l *Blnk) markQueuedAtomicBatchFailed(ctx context.Context, batchID string) error {
	err := l.redis.SetNX(ctx, atomicBatchFailedKey(batchID), time.Now().UTC().Format(time.RFC3339), atomicBatchMarkTTL).Err()
	if err != nil {
		return fmt.Errorf("failed to mark atomic batch %s as failed: %w", batchID, err)
	}
	return nil
}

// failQueuedAtomicBatch rolls back a queued atomic batch that has been marked
// as failed, then reports it with bulk_transaction.failed. Every rejected item
// of the batch runs it until the batch is reported. The rollback skips what is
// already refunded or voided, so an item that runs it after an incomplete
// rollback (a retry, or the next item of the batch) finishes it. A rollback
// error is returned, so the worker retries the item. The webhook goes out once
// per batch, after the rollback is complete.
func (l *Blnk) failQueuedAtomicBatch(ctx context.Context, err error, batchID string, isInflight bool) error {
	// A batch is reported only once its rollback is complete, and no item is
	// applied after the batch is marked, so a reported batch has nothing left
	// to roll back.
	reportedKey := atomicBatchReportedKey(batchID)
	reported, existsErr := l.redis.Exists(ctx, reportedKey).Result()
	if existsErr != nil {
		return fmt.Errorf("failed to check atomic batch %s: %w", batchID, existsErr)
	}
	if reported > 0 {
		return nil
	}

	count, rollbackErr := l.rollBackQueuedAtomicBatch(ctx, batchID, isInflight)
	action := "refunded"
	if isInflight {
		action = "voided"
	}
	l.logRollbackResult(batchID, action, rollbackErr)
	if rollbackErr != nil {
		return fmt.Errorf("failed to roll back atomic batch %s: %w", batchID, rollbackErr)
	}

	first, markErr := l.redis.SetNX(ctx, reportedKey, time.Now().UTC().Format(time.RFC3339), atomicBatchMarkTTL).Result()
	if markErr != nil {
		return fmt.Errorf("failed to mark atomic batch %s as reported: %w", batchID, markErr)
	}
	if !first {
		return nil
	}
	logrus.WithError(err).WithField("batch_id", batchID).Error("queued atomic bulk transaction rejected")
	webhook := bulkTransactionWebhook(batchID, "failed", batchFailureMessage(err, isInflight, count, nil), 0)
	if sendErr := l.SendWebhook(webhook); sendErr != nil {
		// Leave the report to the retry.
		if delErr := l.redis.Del(ctx, reportedKey).Err(); delErr != nil {
			logrus.WithError(delErr).WithField("batch_id", batchID).Error("failed to clear the report mark of atomic batch")
		}
		return fmt.Errorf("failed to report atomic batch %s: %w", batchID, sendErr)
	}
	return nil
}

// rollBackQueuedAtomicBatch reverses what a queued atomic batch has applied
// and returns how many of its transactions are reversed. Refunds and voids are
// applied here rather than queued, so the balances are back when it returns.
// It can run more than once: what an earlier pass reversed is counted, not
// reversed again.
func (l *Blnk) rollBackQueuedAtomicBatch(ctx context.Context, batchID string, isInflight bool) (int, error) {
	if !isInflight {
		return l.refundBatchTransactions(ctx, batchID)
	}

	// Only what is already inflight. With nothing inflight the lookup falls
	// back to the batch's QUEUED rows; those the worker will now reject.
	inflightOnly := func(ctx context.Context, parentID string, batchSize int, offset int64) ([]*model.Transaction, error) {
		txns, err := l.GetInflightTransactionsByParentID(ctx, parentID, batchSize, offset)
		if err != nil || len(txns) == 0 || txns[0].Status != StatusInflight {
			return nil, err
		}
		return txns, nil
	}
	// A voided item keeps its INFLIGHT row, so a later pass finds it again.
	// The void reference is derived from the batch id, so voiding it again
	// returns the earlier void instead of failing.
	voided, err := l.ProcessTransactionInBatches(ctx, batchID, big.NewInt(0), 1, false, inflightOnly, l.InflightWorker(InflightActionVoid, batchID))
	return len(voided), err
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

	if req.Atomic && !req.SkipQueue {
		for _, txn := range req.Transactions {
			txn.Atomic = true
		}
	}

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

			// The items of a queued atomic batch are still queued when this
			// returns; the worker reports the batch once they have landed.
			queuedAtomic := req.Atomic && !req.SkipQueue
			if queuedAtomic {
				if err := l.trackQueuedAtomicBatch(bgCtx, batchID, len(asyncTxns)); err != nil {
					l.handleAsyncBulkTransactionFailure(bgCtx, err, batchID, req.Atomic, req.Inflight)
					return
				}
			}

			// Process transactions in batch
			err := l.processBulkTransactions(bgCtx, asyncTxns, batchID, req.Inflight, req.SkipQueue)

			if err != nil {
				// Handle failure (rollback if atomic, send webhook)
				l.handleAsyncBulkTransactionFailure(bgCtx, err, batchID, req.Atomic, req.Inflight)
			} else if queuedAtomic {
				logrus.Infof("Queued async bulk transaction batch %s", batchID)
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
			_, count, rollbackErr := l.rollbackBatchTransactions(ctx, batchID, req.Inflight)
			responseError = batchFailureMessage(err, req.Inflight, count, rollbackErr)
		} else {
			responseError = fmt.Sprintf("%s. Previous transactions were not rolled back.", err.Error())
		}

		// Return error result for synchronous failure. The returned error
		// carries the same text as result.Error but keeps the failing item's
		// error chain underneath, so the caller can still recover the typed
		// error the item actually raised (errors.As / errors.Is) rather than
		// having to pattern-match the assembled sentence.
		return &model.BulkTransactionResult{
			BatchID: batchID,
			Status:  "failed",
			Error:   responseError,
		}, &BulkTransactionError{Message: responseError, cause: err}
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
