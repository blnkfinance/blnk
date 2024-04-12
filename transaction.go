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
	StatusRejected  = "REJECTED"
)

func (l Blnk) getSourceAndDestination(transaction *model.Transaction) (source *model.Balance, destination *model.Balance, err error) {

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

	locker, err := l.acquireLock(cxt, transaction)
	if err != nil {
		return nil, err
	}
	defer locker.Unlock(cxt)

	if err := l.validateTxn(cxt, transaction); err != nil {
		return nil, err
	}

	sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
	if err != nil {
		return nil, logAndRecordError(span, "source and balance error", err)
	}

	if err = l.applyTransactionToBalances(span, []*model.Balance{sourceBalance, destinationBalance}, transaction); err != nil {
		return nil, logAndRecordError(span, "Error applying transaction to balances: ", err)
	}

	if err = l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return nil, logAndRecordError(span, "commit balance error", err)
	}

	transaction = l.updateTransactionDetails(transaction, sourceBalance, destinationBalance)
	transaction, err = l.persistTransaction(cxt, transaction)
	if err != nil {
		return nil, err
	}

	l.postTransactionActions(ctx, transaction)

	return transaction, nil
}

func (l Blnk) acquireLock(ctx context.Context, transaction *model.Transaction) (*redlock.Locker, error) {
	locker := redlock.NewLocker(l.redis, transaction.Source, model.GenerateUUIDWithSuffix("loc"))
	err := locker.Lock(ctx, time.Minute*30)
	if err != nil {
		return nil, err
	}
	return locker, nil
}

func (l Blnk) updateTransactionDetails(transaction *model.Transaction, sourceBalance, destinationBalance *model.Balance) *model.Transaction {
	fmt.Println(sourceBalance.BalanceID, destinationBalance.BalanceID)
	transaction.Source = sourceBalance.BalanceID
	transaction.Destination = destinationBalance.BalanceID
	if transaction.Status == StatusQueued {
		transaction.Status = StatusApplied
	}
	return transaction
}

func (l Blnk) persistTransaction(ctx context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	transaction, err := l.datasource.RecordTransaction(ctx, transaction)
	if err != nil {
		logrus.Errorf("ERROR saving transaction to db. %s", err)
		return nil, err
	}
	return transaction, nil
}

func (l Blnk) postTransactionActions(ctx context.Context, transaction *model.Transaction) {
	err := SendWebhook(NewWebhook{
		Event:   "transaction.applied",
		Payload: transaction,
	})
	if err != nil {
		notification.NotifyError(err)
	}

}

func logAndRecordError(span trace.Span, msg string, err error) error {
	span.RecordError(err)
	logrus.Error(msg, err)
	return err
}

func (l Blnk) RejectTransaction(ctx context.Context, transaction *model.Transaction, reason string) (*model.Transaction, error) {
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

func (l Blnk) CommitInflightTransaction(ctx context.Context, transactionID string, amount float64) (*model.Transaction, error) {
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

	if amount != 0 {
		transaction.Amount = amount
		transaction.PreciseAmount = 0
	}

	// Commit inflight balances
	sourceBalance.CommitInflightDebit(transaction)       // For the source
	destinationBalance.CommitInflightCredit(transaction) // For the destination

	// Update balances in the database
	if err = l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return nil, logAndRecordError(span, "update balances error", err)
	}

	// create a new transaction with status applied
	transaction.Status = StatusApplied
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	transaction.Hash = transaction.HashTxn()
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

	sourceBalance.RollbackInflightDebit(transaction.PreciseAmount)
	destinationBalance.RollbackInflightCredit(transaction.PreciseAmount)

	if err = l.updateBalances(ctx, sourceBalance, destinationBalance); err != nil {
		return nil, logAndRecordError(span, "update balances error", err)
	}
	transaction.Status = StatusVoid
	transaction.TransactionID = model.GenerateUUIDWithSuffix("txn")
	transaction.Reference = model.GenerateUUIDWithSuffix("ref")
	transaction.Hash = transaction.HashTxn()
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
	fmt.Println(transactions)
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
	originalTxn.Source = originalTxn.Destination
	originalTxn.Destination = originalTxn.Source
	refundTxn, err := l.QueueTransaction(context.Background(), originalTxn)
	if err != nil {
		return &model.Transaction{}, err
	}

	return refundTxn, nil
}
