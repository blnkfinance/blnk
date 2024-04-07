package blnk

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	redlock "github.com/jerry-enebeli/blnk/internal/lock"
	"github.com/jerry-enebeli/blnk/internal/notification"
	"go.opentelemetry.io/otel/trace"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/jerry-enebeli/blnk/model"
)

var (
	tracer = otel.Tracer("Queue transaction")
	meter  = otel.Meter("rolldice")
	txnCnt metric.Int64Counter
)

func init() {
	var err error
	txnCnt, err = meter.Int64Counter("txn.commits",
		metric.WithDescription("The number of commit by transition count value"),
		metric.WithUnit("{txn}"))
	if err != nil {
		panic(err)
	}
}

const (
	StatusQueued    = "QUEUED"
	StatusApplied   = "APPLIED"
	StatusScheduled = "SCHEDULED"
	StatusInflight  = "INFLIGHT"
	StatusVoid      = "VOID"
)

func (l Blnk) getSourceAndDestination(transaction *model.Transaction) (source *model.Balance, destination *model.Balance, err error) {

	var sourceBalance, destinationBalance *model.Balance

	// Check if Source starts with "@"
	if strings.HasPrefix(transaction.Source, "@") {
		sourceBalance, err = l.getOrCreateBalanceByIndicator(transaction.Source)
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
		destinationBalance, err = l.getOrCreateBalanceByIndicator(transaction.Destination)
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
func (l Blnk) updateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	if err := l.datasource.UpdateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return err
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		l.checkBalanceMonitors(sourceBalance)
	}()
	go func() {
		defer wg.Done()
		l.checkBalanceMonitors(destinationBalance)
	}()
	wg.Wait()

	return nil
}

func logAndRecordError(span trace.Span, message string, err error) error {
	wrappedErr := fmt.Errorf("%s: %w", message, err)
	span.RecordError(wrappedErr)
	logrus.Error(wrappedErr)
	return wrappedErr
}

func (l Blnk) validateTxn(cxt context.Context, transaction *model.Transaction) error {
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

func (l Blnk) applyTransactionToBalances(span trace.Span, balances []*model.Balance, transaction *model.Transaction) error {
	span.AddEvent("calculating new balances")
	defer span.End()

	err := model.UpdateBalances(transaction, balances[0], balances[1])
	if err != nil {
		return err
	}
	return nil
}

func (l Blnk) RecordTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	cxt, span := tracer.Start(ctx, "Recording transaction")
	defer span.End()

	locker := redlock.NewLocker(l.redis, transaction.Source, model.GenerateUUIDWithSuffix("loc"))
	err := locker.Lock(ctx, time.Minute*30)
	if err != nil {
		fmt.Printf("Error %s. Pushing to retry Queue", err.Error())
	}
	defer locker.Unlock(ctx)

	emptyTransaction := &model.Transaction{}
	err = l.validateTxn(cxt, transaction)
	if err != nil {
		return emptyTransaction, err
	}

	sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
	if err != nil {
		err = fmt.Errorf("source and balance error %v", err)
		span.RecordError(err)
		logrus.Error(err)
		return nil, err
	}

	transaction.Source = sourceBalance.BalanceID
	transaction.Destination = destinationBalance.BalanceID

	if err = l.applyTransactionToBalances(span, []*model.Balance{sourceBalance, destinationBalance}, transaction); err != nil {
		err = SendWebhook(NewWebhook{
			Event:   "transaction.rejected",
			Payload: transaction,
		})
		return transaction, logAndRecordError(span, "applyTransactionToBalances error", err)
	}

	if err = l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		logrus.Error("commit balance error", err)
		return emptyTransaction, err
	}

	if transaction.Status == StatusQueued {
		transaction.Status = StatusApplied
	}
	transaction, err = l.datasource.RecordTransaction(cxt, transaction)
	if err != nil {
		logrus.Errorf("ERROR saying transaction to db. %s", err)
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

func (l Blnk) CommitInflightTransaction(ctx context.Context, transactionID string, amount int64) (*model.Transaction, error) {
	cxt, span := tracer.Start(ctx, "Committing inflight transaction")
	defer span.End()

	// Fetch the transaction
	transaction, err := l.datasource.GetTransaction(transactionID)
	if err != nil {
		return nil, logAndRecordError(span, "fetch transaction error", err)
	}

	// Verify it's an inflight transaction
	if transaction.Status != StatusInflight {
		err = fmt.Errorf("transaction is not in inflight status")
		span.RecordError(err)
		return nil, err
	}

	// Locking around the transaction source to prevent concurrent modifications
	locker := redlock.NewLocker(l.redis, transaction.Source, "lock")
	if err := locker.Lock(ctx, 30*time.Minute); err != nil {
		fmt.Printf("Error acquiring lock: %s. Pushing to retry queue", err.Error())
		return nil, err
	}
	defer locker.Unlock(ctx)

	// Get source and destination balances
	sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
	if err != nil {
		err = fmt.Errorf("source and destination balance error: %v", err)
		span.RecordError(err)
		return nil, err
	}

	// Commit inflight balances
	sourceBalance.CommitInflightDebit(transaction.Amount)       // For the source
	destinationBalance.CommitInflightCredit(transaction.Amount) // For the destination

	// Update balances in the database
	if err = l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return nil, logAndRecordError(span, "update balances error", err)
	}

	// create a new transaction with status applied
	transaction.Status = StatusApplied
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction, err = l.datasource.RecordTransaction(cxt, transaction)
	if err != nil {
		return nil, logAndRecordError(span, "saving transaction to db error", err)
	}

	return transaction, nil
}

func (l Blnk) VoidInflightTransaction(ctx context.Context, transactionID string) (*model.Transaction, error) {
	cxt, span := tracer.Start(ctx, "Rolling back inflight transaction")
	defer span.End()

	// Fetch the transaction
	transaction, err := l.datasource.GetTransaction(transactionID)
	if err != nil {
		return nil, logAndRecordError(span, "fetch transaction error", err)
	}

	// Verify it's an inflight transaction
	if transaction.Status != StatusInflight {
		err = fmt.Errorf("transaction is not in inflight status")
		span.RecordError(err)
		return nil, err
	}

	// Locking around the transaction source to prevent concurrent modifications
	locker := redlock.NewLocker(l.redis, transaction.Source, "lock")
	if err := locker.Lock(ctx, 30*time.Minute); err != nil {
		fmt.Printf("Error acquiring lock: %s. Pushing to retry queue", err.Error())
		return nil, err
	}
	defer locker.Unlock(ctx)

	sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
	if err != nil {
		err = fmt.Errorf("source and destination balance error: %v", err)
		span.RecordError(err)
		return nil, err
	}

	sourceBalance.RollbackInflightDebit(transaction.Amount)
	destinationBalance.RollbackInflightCredit(transaction.Amount)

	if err = l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return nil, logAndRecordError(span, "update balances error", err)
	}

	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Status = StatusVoid
	transaction, err = l.datasource.RecordTransaction(cxt, transaction)
	if err != nil {
		return nil, logAndRecordError(span, "saving transaction to db error", err)
	}

	return transaction, nil
}

func (l Blnk) QueueTransaction(cxt context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(cxt, "Queuing transaction")
	defer span.End()
	emptyTransaction := &model.Transaction{}
	err := l.validateTxn(cxt, transaction)
	if err != nil {
		return emptyTransaction, err
	}

	transaction.Status = StatusQueued
	transaction.SkipBalanceUpdate = true
	if !transaction.ScheduledFor.IsZero() {
		transaction.Status = StatusScheduled
	}

	if transaction.Inflight {
		transaction.Status = StatusInflight
	}

	transactions, err := transaction.SplitTransaction()
	if err != nil {
		return nil, err
	}

	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	hash := transaction.HashTxn()
	transaction.Hash = hash

	if len(transactions) > 0 {
		for _, txn := range transactions {
			err = l.queue.Enqueue(ctx, &txn)
			if err != nil {
				notification.NotifyError(err)
				logrus.Errorf("Error queuing transaction: %v", err)
				return emptyTransaction, err
			}

		}

	} else {
		err = l.queue.Enqueue(ctx, transaction)
		if err != nil {
			notification.NotifyError(err)
			logrus.Errorf("Error queuing transaction: %v", err)
			return emptyTransaction, err
		}

	}
	return transaction, nil
}

func (l Blnk) GetTransaction(TransactionID string) (*model.Transaction, error) {
	return l.datasource.GetTransaction(TransactionID)
}

func (l Blnk) GetAllTransactions() ([]model.Transaction, error) {
	return l.datasource.GetAllTransactions()
}

func (l Blnk) GetTransactionByRef(cxt context.Context, reference string) (model.Transaction, error) {
	return l.datasource.GetTransactionByRef(cxt, reference)
}

func (l Blnk) UpdateTransactionStatus(id string, status string) error {
	return l.datasource.UpdateTransactionStatus(id, status)
}

func (l Blnk) RefundTransaction(transactionID string) (*model.Transaction, error) {
	originalTxn, err := l.GetTransaction(transactionID)
	if err != nil {
		return &model.Transaction{}, err
	}

	originalTxn.Reference = model.GenerateUUIDWithSuffix("ref")

	// Record the new inverse transaction
	refundTxn, err := l.QueueTransaction(context.Background(), originalTxn)
	if err != nil {
		return &model.Transaction{}, err
	}

	return refundTxn, nil
}
