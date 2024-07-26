package blnk

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	redlock "github.com/jerry-enebeli/blnk/internal/lock"
	"github.com/jerry-enebeli/blnk/internal/notification"
	"go.opentelemetry.io/otel/trace"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"

	"github.com/jerry-enebeli/blnk/model"
)

var (
	tracer = otel.Tracer("Queue transaction")
)

const (
	StatusQueued    = "QUEUED"
	StatusApplied   = "APPLIED"
	StatusScheduled = "SCHEDULED"
	StatusInflight  = "INFLIGHT"
	StatusVoid      = "VOID"
	StatusCommit    = "COMMIT"
	StatusRejected  = "REJECTED"
)

type getTxns func(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error)

type transactionWorker func(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64)

type BatchJobResult struct {
	RefundTxn *model.Transaction
	Error     error
}

func getEventFromStatus(status string) string {
	switch strings.ToLower(status) {
	case strings.ToLower(StatusQueued):
		return "transaction.queued"
	case strings.ToLower(StatusApplied):
		return "transaction.applied"
	case strings.ToLower(StatusScheduled):
		return "transaction.scheduled"
	case strings.ToLower(StatusInflight):
		return "transaction.inflight"
	case strings.ToLower(StatusVoid):
		return "transaction.void"
	case strings.ToLower(StatusRejected):
		return "transaction.rejected"
	default:
		return "transaction.unknown"
	}
}

func (l *Blnk) getSourceAndDestination(transaction *model.Transaction) (source *model.Balance, destination *model.Balance, err error) {
	var sourceBalance, destinationBalance *model.Balance

	// Check if Source starts with "@"
	if strings.HasPrefix(transaction.Source, "@") {
		sourceBalance, err = l.getOrCreateBalanceByIndicator(transaction.Source, transaction.Currency)
		if err != nil {
			logrus.Errorf("source error %v", err)
			return nil, nil, err
		}
		// Update transaction source with the balance ID
		transaction.Source = sourceBalance.BalanceID
	} else {
		sourceBalance, err = l.datasource.GetBalanceByIDLite(transaction.Source)
		if err != nil {
			logrus.Errorf("source error %v", err)
			return nil, nil, err
		}
	}

	// Check if Destination starts with "@"
	if strings.HasPrefix(transaction.Destination, "@") {
		destinationBalance, err = l.getOrCreateBalanceByIndicator(transaction.Destination, transaction.Currency)
		if err != nil {
			logrus.Errorf("destination error %v", err)
			return nil, nil, err
		}
		// Update transaction destination with the balance ID
		transaction.Destination = destinationBalance.BalanceID
	} else {
		destinationBalance, err = l.datasource.GetBalanceByIDLite(transaction.Destination)
		if err != nil {
			logrus.Errorf("destination error %v", err)
			return nil, nil, err
		}
	}
	return sourceBalance, destinationBalance, nil
}

func (l *Blnk) acquireLock(ctx context.Context, transaction *model.Transaction) (*redlock.Locker, error) {
	locker := redlock.NewLocker(l.redis, transaction.Source, model.GenerateUUIDWithSuffix("loc"))
	err := locker.Lock(ctx, time.Minute*30)
	if err != nil {
		return nil, err
	}
	return locker, nil
}

func (l *Blnk) updateTransactionDetails(transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) *model.Transaction {
	transaction.Source = sourceBalance.BalanceID
	transaction.Destination = destinationBalance.BalanceID
	applicableStatus := map[string]string{StatusQueued: StatusApplied, StatusScheduled: StatusApplied, StatusCommit: StatusApplied, StatusVoid: StatusVoid}
	transaction.Status = applicableStatus[transaction.Status]
	if transaction.Inflight {
		transaction.Status = StatusInflight
	}
	return transaction
}

func (l *Blnk) persistTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		logrus.Errorf("ERROR saving transaction to db. %s", err)
		return nil, err
	}
	return transaction, nil
}

func (l *Blnk) postTransactionActions(_ context.Context, transaction *model.Transaction) {
	go func() {
		l.queue.queueIndexData(transaction.TransactionID, "transactions", transaction)
		err := SendWebhook(NewWebhook{
			Event:   getEventFromStatus(transaction.Status),
			Payload: transaction,
		})
		if err != nil {
			notification.NotifyError(err)
		}
	}()
}

func (l *Blnk) updateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	var wg sync.WaitGroup
	if err := l.datasource.UpdateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return err
	}
	wg.Add(2)
	go func() {
		defer wg.Done()
		l.checkBalanceMonitors(sourceBalance)
		l.queue.queueIndexData(sourceBalance.BalanceID, "balances", sourceBalance)
	}()
	go func() {
		defer wg.Done()
		l.checkBalanceMonitors(destinationBalance)
		l.queue.queueIndexData(destinationBalance.BalanceID, "balances", destinationBalance)
	}()
	wg.Wait()

	return nil
}

func (l *Blnk) validateTxn(cxt context.Context, transaction *model.Transaction) error {
	cxt, span := tracer.Start(cxt, "Validating transaction reference")
	defer span.End()
	txn, err := l.datasource.TransactionExistsByRef(cxt, transaction.Reference)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}

	if txn {
		return fmt.Errorf("reference %s has already been used", transaction.Reference)
	}

	return nil
}

func (l *Blnk) applyTransactionToBalances(span trace.Span, balances []*model.Balance, transaction *model.Transaction) error {
	span.AddEvent("calculating new balances")
	defer span.End()

	if transaction.Status == StatusCommit {
		balances[0].CommitInflightDebit(transaction)
		balances[1].CommitInflightCredit(transaction)
		return nil
	}

	if transaction.Status == StatusVoid {
		//TODO: Implement RollbackInflightDebit and RollbackInflightCredit
		balances[0].RollbackInflightDebit(int64(transaction.PreciseAmount))
		balances[1].RollbackInflightCredit(int64(transaction.PreciseAmount))
		return nil
	}

	err := model.UpdateBalances(transaction, balances[0], balances[1])
	if err != nil {
		return err
	}
	return nil
}

func (l *Blnk) GetInflightTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	return l.datasource.GetInflightTransactionsByParentID(ctx, parentTransactionID, batchSize, offset)
}

func (l *Blnk) GetRefundableTransactionsByParentID(ctx context.Context, parentTransactionID string, batchSize int, offset int64) ([]*model.Transaction, error) {
	return l.datasource.GetRefundableTransactionsByParentID(ctx, parentTransactionID, batchSize, offset)
}

func (l *Blnk) ProcessTransactionInBatches(ctx context.Context, parentTransactionID string, amount float64, gt getTxns, tw transactionWorker) ([]*model.Transaction, error) {
	const (
		batchSize    = 100
		maxWorkers   = 5
		maxQueueSize = 1000
	)
	var allRefundTxns []*model.Transaction
	var allErrors []error

	// Create a buffered channel to queue work
	jobs := make(chan *model.Transaction, maxQueueSize)
	results := make(chan BatchJobResult, maxQueueSize)

	// Create a wait group to wait for all goroutines to finish
	var wg sync.WaitGroup

	// Start worker pool
	for w := 1; w <= maxWorkers; w++ {
		wg.Add(1)
		go tw(ctx, jobs, results, &wg, amount)
	}

	// Start a goroutine to close the results channel when all workers are done
	go func() {
		wg.Wait()
		close(results)
	}()

	// Create a wait group for processing results
	var resultsWg sync.WaitGroup
	resultsWg.Add(1)

	// Start a goroutine to process results
	go func() {
		defer resultsWg.Done()
		for result := range results {
			if result.Error != nil {
				allErrors = append(allErrors, result.Error)
			} else if result.RefundTxn != nil {
				allRefundTxns = append(allRefundTxns, result.RefundTxn)
			}
		}
	}()

	// Fetch and process transactions in batches
	var offset int64 = 0
	for {
		txns, err := gt(ctx, parentTransactionID, batchSize, offset)
		if err != nil {
			return allRefundTxns, err
		}
		if len(txns) == 0 {
			break // No more transactions to process
		}

		// Queue jobs
		for _, txn := range txns {
			jobs <- txn
		}

		offset += int64(len(txns))
	}

	// Close the jobs channel to signal workers to stop
	close(jobs)

	// Wait for all worker goroutines to finish
	wg.Wait()

	// Wait for the results processing goroutine to finish
	resultsWg.Wait()

	if len(allErrors) > 0 {
		// Log errors and return a combined error
		for _, err := range allErrors {
			log.Printf("Error during processing: %v", err)
		}
		return allRefundTxns, allErrors[0]
	}

	return allRefundTxns, nil
}

func (l *Blnk) RefundWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
	fmt.Println("refund got called")
	defer wg.Done()
	for originalTxn := range jobs {
		queuedRefundTxn, err := l.RefundTransaction(originalTxn.TransactionID)
		if err != nil {
			results <- BatchJobResult{Error: err}
		}
		fmt.Println("refund txn", queuedRefundTxn)
		results <- BatchJobResult{RefundTxn: queuedRefundTxn}
	}
}

func (l *Blnk) CommitWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
	defer wg.Done()
	for originalTxn := range jobs {
		if originalTxn.Status != StatusInflight {
			results <- BatchJobResult{Error: fmt.Errorf("transaction is not in inflight status")}
		}
		queuedRefundTxn, err := l.CommitInflightTransaction(ctx, originalTxn.TransactionID, amount)
		if err != nil {
			results <- BatchJobResult{Error: err}
		}
		results <- BatchJobResult{RefundTxn: queuedRefundTxn}
	}
}

func (l *Blnk) VoidWorker(ctx context.Context, jobs <-chan *model.Transaction, results chan<- BatchJobResult, wg *sync.WaitGroup, amount float64) {
	defer wg.Done()
	for originalTxn := range jobs {
		if originalTxn.Status != StatusInflight {
			results <- BatchJobResult{Error: fmt.Errorf("transaction is not in inflight status")}
		}
		queuedRefundTxn, err := l.VoidInflightTransaction(ctx, originalTxn.TransactionID)
		if err != nil {
			results <- BatchJobResult{Error: err}
		}
		results <- BatchJobResult{RefundTxn: queuedRefundTxn}
	}
}

func (l *Blnk) RecordTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "Recording transaction")
	defer span.End()

	return l.executeWithLock(ctx, transaction, func(ctx context.Context) (*model.Transaction, error) {
		sourceBalance, destinationBalance, err := l.validateAndPrepareTransaction(ctx, span, transaction)
		if err != nil {
			return nil, err
		}

		if err := l.processBalances(ctx, span, transaction, sourceBalance, destinationBalance); err != nil {
			return nil, err
		}

		transaction, err = l.finalizeTransaction(ctx, span, transaction, sourceBalance, destinationBalance)
		if err != nil {
			return nil, err
		}

		l.postTransactionActions(ctx, transaction)

		return transaction, nil
	})
}

func (l *Blnk) executeWithLock(ctx context.Context, transaction *model.Transaction, fn func(context.Context) (*model.Transaction, error)) (*model.Transaction, error) {
	locker, err := l.acquireLock(ctx, transaction)
	if err != nil {
		return nil, fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer l.releaseLock(ctx, locker)

	return fn(ctx)
}

func (l *Blnk) validateAndPrepareTransaction(ctx context.Context, span trace.Span, transaction *model.Transaction) (*model.Balance, *model.Balance, error) {
	if err := l.validateTxn(ctx, transaction); err != nil {
		return nil, nil, l.logAndRecordError(span, "transaction validation failed", err)
	}

	sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
	if err != nil {
		return nil, nil, l.logAndRecordError(span, "failed to get source and destination balances", err)
	}

	transaction.Source = sourceBalance.BalanceID
	transaction.Destination = destinationBalance.BalanceID

	return sourceBalance, destinationBalance, nil
}

func (l *Blnk) processBalances(ctx context.Context, span trace.Span, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) error {
	if err := l.applyTransactionToBalances(span, []*model.Balance{sourceBalance, destinationBalance}, transaction); err != nil {
		return l.logAndRecordError(span, "failed to apply transaction to balances", err)
	}

	if err := l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return l.logAndRecordError(span, "failed to update balances", err)
	}

	return nil
}

func (l *Blnk) finalizeTransaction(ctx context.Context, span trace.Span, transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) (*model.Transaction, error) {
	transaction = l.updateTransactionDetails(transaction, sourceBalance, destinationBalance)

	transaction, err := l.persistTransaction(ctx, transaction)
	if err != nil {
		return nil, l.logAndRecordError(span, "failed to persist transaction", err)
	}

	return transaction, nil
}

func (l *Blnk) releaseLock(ctx context.Context, locker *redlock.Locker) {
	if err := locker.Unlock(ctx); err != nil {
		logrus.Error("failed to release lock", err)
	}
}

func (l *Blnk) logAndRecordError(span trace.Span, msg string, err error) error {
	span.RecordError(err)
	logrus.Error(msg, err)
	return fmt.Errorf("%s: %w", msg, err)
}

func (l *Blnk) RejectTransaction(ctx context.Context, transaction *model.Transaction, reason string) (*model.Transaction, error) {
	transaction.Status = StatusRejected
	if transaction.MetaData == nil {
		transaction.MetaData = make(map[string]interface{})
	}
	transaction.MetaData["blnk_rejection_reason"] = reason

	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		logrus.Errorf("ERROR saving transaction to db. %s", err)
	}

	err = SendWebhook(NewWebhook{
		Event:   "transaction.applied",
		Payload: transaction,
	})
	if err != nil {
		notification.NotifyError(err)
	}

	return transaction, nil
}

func (l *Blnk) CommitInflightTransaction(ctx context.Context, transactionID string, amount float64) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "Committing inflight transaction")
	defer span.End()

	transaction, err := l.fetchAndValidateInflightTransaction(ctx, span, transactionID)
	if err != nil {
		return nil, err
	}

	if err := l.validateAndUpdateAmount(ctx, span, transaction, amount); err != nil {
		return nil, err
	}

	return l.finalizeCommitment(ctx, span, transaction)

}

func (l *Blnk) validateAndUpdateAmount(_ context.Context, span trace.Span, transaction *model.Transaction, amount float64) error {
	committedAmount, err := l.datasource.GetTotalCommittedTransactions(transaction.TransactionID)
	if err != nil {
		return l.logAndRecordError(span, "error fetching committed amount", err)
	}

	originalAmount := transaction.PreciseAmount
	amountLeft := originalAmount - committedAmount

	if amount != 0 {
		transaction.Amount = amount
		transaction.PreciseAmount = 0
	} else {
		transaction.Amount = float64(amountLeft) / transaction.Precision
	}

	if amountLeft < model.ApplyPrecision(transaction) {
		return fmt.Errorf("can not commit %s %.2f. You can only commit an amount between 1.00 - %s%.2f",
			transaction.Currency, amount, transaction.Currency, float64(amountLeft)/transaction.Precision)
	} else if amountLeft == 0 {
		return fmt.Errorf("can not commit %s %.2f. Transaction already committed with amount of - %s%.2f",
			transaction.Currency, amount, transaction.Currency, float64(committedAmount)/transaction.Precision)
	}

	return nil
}

func (l *Blnk) finalizeCommitment(ctx context.Context, span trace.Span, transaction *model.Transaction) (*model.Transaction, error) {
	transaction.Status = StatusCommit
	transaction.ParentTransaction = transaction.TransactionID
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	transaction.Hash = transaction.HashTxn()
	transaction, err := l.QueueTransaction(ctx, transaction)
	if err != nil {
		return nil, l.logAndRecordError(span, "saving transaction to db error", err)
	}

	return transaction, nil
}

func (l *Blnk) VoidInflightTransaction(ctx context.Context, transactionID string) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "Voiding inflight transaction")
	defer span.End()

	transaction, err := l.fetchAndValidateInflightTransaction(ctx, span, transactionID)
	if err != nil {
		return nil, err

	}

	amountLeft, err := l.calculateRemainingAmount(ctx, span, transaction)
	if err != nil {
		return nil, err
	}

	return l.finalizeVoidTransaction(ctx, span, transaction, amountLeft)

}

func (l *Blnk) fetchAndValidateInflightTransaction(_ context.Context, span trace.Span, transactionID string) (*model.Transaction, error) {
	var transaction *model.Transaction
	dbTransaction, err := l.datasource.GetTransaction(transactionID)
	if err == sql.ErrNoRows {
		queuedTxn, err := l.queue.GetTransactionFromQueue(transactionID)
		log.Println("found inflight transaction in queue using it for commit/void", transactionID, queuedTxn.TransactionID)
		if err != nil {
			return &model.Transaction{}, err
		}
		if queuedTxn == nil {
			return nil, fmt.Errorf("transaction not found")
		}
		transaction = queuedTxn
	} else if err == nil {
		transaction = dbTransaction
	} else {
		return &model.Transaction{}, err
	}

	if transaction.Status != StatusInflight {
		return nil, l.logAndRecordError(span, "invalid transaction status", fmt.Errorf("transaction is not in inflight status"))
	}

	parentVoided, err := l.datasource.IsParentTransactionVoid(transactionID)
	if err != nil {
		return nil, l.logAndRecordError(span, "error checking parent transaction status", err)
	}

	if parentVoided {
		return nil, l.logAndRecordError(span, "Error voiding transaction", fmt.Errorf("transaction has already been voided"))
	}

	return transaction, nil
}

func (l *Blnk) calculateRemainingAmount(_ context.Context, span trace.Span, transaction *model.Transaction) (int64, error) {
	committedAmount, err := l.datasource.GetTotalCommittedTransactions(transaction.TransactionID)
	if err != nil {
		return 0, l.logAndRecordError(span, "error fetching committed amount", err)
	}

	return transaction.PreciseAmount - committedAmount, nil
}

func (l *Blnk) finalizeVoidTransaction(ctx context.Context, span trace.Span, transaction *model.Transaction, amountLeft int64) (*model.Transaction, error) {
	transaction.Status = StatusVoid
	transaction.Amount = float64(amountLeft) / transaction.Precision
	transaction.PreciseAmount = amountLeft
	transaction.ParentTransaction = transaction.TransactionID
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	transaction.Hash = transaction.HashTxn()
	transaction, err := l.QueueTransaction(ctx, transaction)
	if err != nil {
		return nil, l.logAndRecordError(span, "saving transaction to db error", err)
	}

	return transaction, nil
}

func (l *Blnk) QueueTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(ctx, "Queuing transaction")
	defer span.End()

	if err := l.validateTxn(ctx, transaction); err != nil {
		return nil, err
	}

	setTransactionStatus(transaction)
	setTransactionMetadata(transaction)

	transactions, err := transaction.SplitTransaction()
	if err != nil {
		return nil, err
	}

	if err := enqueueTransactions(ctx, l.queue, transaction, transactions); err != nil {
		return nil, err
	}

	return transaction, nil
}

func setTransactionStatus(transaction *model.Transaction) {
	if !transaction.ScheduledFor.IsZero() {
		transaction.Status = StatusScheduled
	} else if transaction.Inflight {
		transaction.Status = StatusInflight
	} else if transaction.Status == "" {
		transaction.Status = StatusQueued
	}
}

func setTransactionMetadata(transaction *model.Transaction) {
	transaction.SkipBalanceUpdate = true
	transaction.CreatedAt = time.Now()
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Hash = transaction.HashTxn()
	transaction.PreciseAmount = int64(transaction.Amount * transaction.Precision)
}

func enqueueTransactions(ctx context.Context, queue *Queue, originalTransaction *model.Transaction, splitTransactions []*model.Transaction) error {
	transactionsToEnqueue := splitTransactions
	if len(transactionsToEnqueue) == 0 {
		transactionsToEnqueue = []*model.Transaction{originalTransaction}
	}

	for _, txn := range transactionsToEnqueue {
		if err := queue.Enqueue(ctx, txn); err != nil {
			notification.NotifyError(err)
			logrus.Errorf("Error queuing transaction: %v", err)
			return err
		}
	}

	return nil
}

func (l *Blnk) GetTransaction(TransactionID string) (*model.Transaction, error) {
	return l.datasource.GetTransaction(TransactionID)
}

func (l *Blnk) GetAllTransactions() ([]model.Transaction, error) {
	return l.datasource.GetAllTransactions()
}

func (l *Blnk) GetTransactionByRef(cxt context.Context, reference string) (model.Transaction, error) {
	return l.datasource.GetTransactionByRef(cxt, reference)
}

func (l *Blnk) UpdateTransactionStatus(id string, status string) error {
	return l.datasource.UpdateTransactionStatus(id, status)
}

func (l *Blnk) RefundTransaction(transactionID string) (*model.Transaction, error) {
	originalTxn, err := l.GetTransaction(transactionID)
	if err != nil {
		// Check if the error is due to no row found
		if err == sql.ErrNoRows {
			// Check the queue for the transaction
			queuedTxn, err := l.queue.GetTransactionFromQueue(transactionID)
			log.Println("found transaction in queue using it for refund", transactionID, queuedTxn.TransactionID)
			if err != nil {
				return &model.Transaction{}, err
			}
			if queuedTxn == nil {
				return nil, fmt.Errorf("transaction not found")
			}
			originalTxn = queuedTxn
		} else {
			return &model.Transaction{}, err
		}
	}

	if originalTxn.Status == StatusRejected {
		return nil, fmt.Errorf("transaction is not in a state that can be refunded")
	}

	if originalTxn.Status == StatusVoid {
		originalTxn.Inflight = true
	} else {
		originalTxn.Status = ""
	}

	newTransaction := *originalTxn
	newTransaction.Reference = model.GenerateUUIDWithSuffix("ref")
	newTransaction.ParentTransaction = originalTxn.TransactionID
	newTransaction.Source = originalTxn.Destination
	newTransaction.Destination = originalTxn.Source
	newTransaction.AllowOverdraft = true
	refundTxn, err := l.QueueTransaction(context.Background(), &newTransaction)
	if err != nil {
		return &model.Transaction{}, err
	}

	return refundTxn, nil
}
