package blnk

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/otel/attribute"

	"github.com/jerry-enebeli/blnk/internal/notification"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/sirupsen/logrus"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/jerry-enebeli/blnk/database"
)

var (
	tracer = otel.Tracer("Queue transaction")
	meter  = otel.Meter("rolldice")
	txnCnt metric.Int64Counter
)

var ()

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

	fmt.Println("here>>>", source, destination)

	return sourceBalance, destinationBalance, nil
}

//func (l Blnk) validateCurrencyMatch(transaction *model.Transaction) error {
//	sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
//	if err != nil {
//		return err
//	}
//	if (sourceBalance.Currency != destinationBalance.Currency) && sourceBalance.Currency != transaction.Currency {
//		return fmt.Errorf("transaction %s currency %s does not match the source %s and destination balance %s currency %s. Please ensure they are consistent", transaction.TransactionID, transaction.Currency, transaction.Source, transaction.Destination, transaction.Currency)
//	}
//	return nil
//}

func (l Blnk) updateBalances(ctx context.Context, sourceBalance, destinationBalance *model.Balance) error {
	err := l.datasource.UpdateBalances(ctx, sourceBalance, destinationBalance)
	if err != nil {
		return err
	}

	//todo refactor
	go func() {
		l.checkBalanceMonitors(sourceBalance)

	}()
	go func() {
		l.checkBalanceMonitors(destinationBalance)

	}()

	return nil
}

func (l Blnk) ApplyBalanceToQueuedTransaction(context context.Context, transaction *model.Transaction) error {
	ctx, span := tracer.Start(context, "commit")
	defer span.End()
	sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
	if err != nil {
		err := fmt.Errorf("source and balance error %v", err)
		span.RecordError(err)
		logrus.Error(err)
		return err
	}

	err = l.applyTransactionToBalances(span, []*model.Balance{sourceBalance, destinationBalance}, transaction)
	if err != nil {
		err := fmt.Errorf("applyTransactionToBalances error %s", err)
		span.RecordError(err)
		logrus.Error(err)
		return err
	}

	txn, err := l.GetTransactionByRef(ctx, transaction.Reference)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		err := fmt.Errorf("GetTransactionByRef error %s", err)
		span.RecordError(err)
		logrus.Error(err)
		return err
	}

	if txn.Status == StatusQueued {
		err = l.updateBalances(ctx, sourceBalance, destinationBalance)
		if err != nil {
			logrus.Error("commit balance error", err)
			return err
		}
	}

	go func() {
		if len(transaction.GroupIds) > 0 {
			for _, id := range transaction.GroupIds {
				//updates the transaction status from QUEUED to SUCCESSFUL
				err = l.datasource.UpdateTransactionStatus(id, StatusApplied)
				if err != nil {
					logrus.Error("UpdateTransactionStatus in groups error", err)
				}
			}
		} else {
			err = l.datasource.UpdateTransactionStatus(transaction.TransactionID, StatusApplied)
			if err != nil {
				logrus.Error("UpdateTransactionStatus single error", err)
			}
		}
	}()

	rollValueAttr := attribute.Int("commit.value", 1)
	span.SetAttributes(rollValueAttr)
	txnCnt.Add(ctx, 1, metric.WithAttributes(rollValueAttr))
	logrus.Infof("committed transaction to balance %s %s", sourceBalance.BalanceID, destinationBalance.BalanceID)

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

func (l Blnk) recordTransaction(cxt context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	transaction, err := l.datasource.RecordTransaction(cxt, transaction)
	if err != nil {
		return transaction, err
	}
	return transaction, nil
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

func (l Blnk) RecordTransaction(cxt context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	cxt, span := tracer.Start(cxt, "Preparing transaction for db")
	defer span.End()

	emptyTransaction := &model.Transaction{}
	err := l.validateTxn(cxt, transaction)
	if err != nil {
		return emptyTransaction, err
	}

	sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
	if err != nil {
		err := fmt.Errorf("source and balance error %v", err)
		span.RecordError(err)
		logrus.Error(err)
		return nil, err
	}

	riskScore := l.ApplyFraudScore(transaction, sourceBalance, destinationBalance)
	if riskScore >= transaction.RiskToleranceThreshold && transaction.RiskToleranceThreshold > 0 {
		return emptyTransaction, fmt.Errorf("this transaction has been flagged as a high risk")
	}

	transaction.RiskScore = riskScore
	if transaction.Status == "" {
		transaction.Status = StatusApplied
	}

	transaction.Source = sourceBalance.BalanceID
	transaction.Destination = destinationBalance.BalanceID

	transaction, err = l.recordTransaction(cxt, transaction)
	if err != nil {
		logrus.Errorf("ERROR saying transaction to db. %s", err)
	}

	return transaction, nil
}

func (l Blnk) QueueTransaction(cxt context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	ctx, span := tracer.Start(cxt, "Queuing transaction")
	defer span.End()

	transaction.Status = StatusQueued
	transaction.SkipBalanceUpdate = true
	if !transaction.ScheduledFor.IsZero() {
		transaction.Status = StatusScheduled
	}
	//does not apply transaction to the balance
	transaction, err := l.RecordTransaction(ctx, transaction) //saves transaction to db
	if err != nil {
		return &model.Transaction{}, err
	}

	err = l.queue.Enqueue(ctx, transaction)
	if err != nil {
		notification.NotifyError(err)
		logrus.Errorf("Error queuing transaction: %v", err)
	}

	return transaction, nil
}

func (l Blnk) GetTransaction(TransactionID string) (model.Transaction, error) {
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
	// Retrieve the original transaction by its ID
	originalTxn, err := l.GetTransaction(transactionID)
	if err != nil {
		return &model.Transaction{}, err
	}

	originalTxn.Reference = database.GenerateUUIDWithSuffix("ref")
	// Record the new inverse transaction
	refundTxn, err := l.QueueTransaction(context.Background(), &originalTxn)
	if err != nil {
		return &model.Transaction{}, err
	}

	return refundTxn, nil
}
