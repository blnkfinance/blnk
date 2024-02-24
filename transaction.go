package blnk

import (
	"context"
	"database/sql"
	"errors"
	"fmt"

	"github.com/jerry-enebeli/blnk/internal/notification"
	"go.opentelemetry.io/otel/trace"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/sirupsen/logrus"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/jerry-enebeli/blnk/database"
)

var (
	tracer    = otel.Tracer("Queue transaction")
	meter     = otel.Meter("Queue transaction")
	commitCnt metric.Int64Counter
)

func init() {
	var err error
	commitCnt, err = meter.Int64Counter("transaction.commits",
		metric.WithDescription("The number of transactions committed"),
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
	sourceBalance, err := l.datasource.GetBalanceByIDLite(transaction.Source)
	emptyBalance := &model.Balance{}
	if err != nil {
		logrus.Errorf("source error %v", err)
		return emptyBalance, emptyBalance, err
	}
	destinationBalance, err := l.datasource.GetBalanceByIDLite(transaction.Destination)
	if err != nil {
		logrus.Errorf("destination error %v", err)
		return emptyBalance, emptyBalance, err
	}

	return sourceBalance, destinationBalance, nil
}

func (l Blnk) validateBlnMatch(transaction *model.Transaction) error {
	sourceBalance, destinationBalance, err := l.getSourceAndDestination(transaction)
	if err != nil {
		return err
	}
	if (sourceBalance.Currency != destinationBalance.Currency) && sourceBalance.Currency != transaction.Currency {
		return fmt.Errorf("transaction %s currency %s does not match the source %s and destination balance %s currency %s. Please ensure they are consistent", transaction.TransactionID, transaction.Currency, transaction.Source, transaction.Destination, transaction.Currency)
	}
	return nil
}

// Helper function to get the opposite DRCR value
func inverseDRCR(drcr string) string {
	if drcr == "Debit" {
		return "Credit"
	}
	return "Debit"
}

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

	if len(transaction.GroupIds) > 0 {
		for _, id := range transaction.GroupIds {
			//updates the transaction status from QUEUED to SUCCESSFUL
			err = l.datasource.UpdateTransactionStatus(id, StatusApplied)
			if err != nil {
				logrus.Error("UpdateTransactionStatus in groups error", err)
				return err
			}
		}
	} else {
		err = l.datasource.UpdateTransactionStatus(transaction.TransactionID, StatusApplied)
		if err != nil {
			logrus.Error("UpdateTransactionStatus single error", err)
			return err
		}
	}

	logrus.Infof("committed transaction to balance %s %s", sourceBalance.BalanceID, destinationBalance.BalanceID)

	return nil
}

func (l Blnk) validateTxn(cxt context.Context, transaction *model.Transaction) (model.Transaction, error) {
	cxt, span := tracer.Start(cxt, "Validating transaction reference")
	defer span.End()
	txn, err := l.GetTransactionByRef(cxt, transaction.Reference)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return model.Transaction{}, err
	}

	if errors.Is(err, nil) && txn.TransactionID != "" {
		return model.Transaction{}, fmt.Errorf("this reference has already been %s . discarding transaction", txn.Status)
	}

	return txn, nil
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
	for i, _ := range balances {
		err := balances[i].UpdateBalances(transaction, i)
		if err != nil {
			return err
		}
	}
	return nil
}

func (l Blnk) RecordTransaction(cxt context.Context, transaction *model.Transaction) (*model.Transaction, error) {
	cxt, span := tracer.Start(cxt, "Preparing transaction for db")
	defer span.End()

	emptyTransaction := &model.Transaction{}
	_, err := l.datasource.TransactionExistsByRef(cxt, transaction.Reference)
	if err != nil {
		return emptyTransaction, err
	}

	//riskScore := l.ApplyFraudScore(transaction)
	//if riskScore >= transaction.RiskToleranceThreshold && transaction.RiskToleranceThreshold > 0 {
	//	return emptyTransaction, fmt.Errorf("this transaction has been flagged as a high risk")
	//}
	//
	//transaction.RiskScore = riskScore
	//if transaction.Status == "" {
	//	transaction.Status = StatusApplied
	//}

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
		logrus.Error("Error queuing transaction: %v", err)
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

// todo rewrite
func (l Blnk) RefundTransaction(transactionID string) (*model.Transaction, error) {
	// Retrieve the original transaction by its ID
	originalTxn, err := l.GetTransaction(transactionID)
	if err != nil {
		return &model.Transaction{}, err
	}

	// Create a new inverse transaction with the opposite "drcr" value
	refundTxn := &model.Transaction{
		Reference: database.GenerateUUIDWithSuffix("ref"),
		Amount:    originalTxn.Amount, // Inverse amount
		Currency:  originalTxn.Currency,
		MetaData:  map[string]interface{}{"refunded_transaction_id": transactionID},
	}

	// Record the new inverse transaction
	refundTxn, err = l.RecordTransaction(context.Background(), refundTxn)
	if err != nil {
		return &model.Transaction{}, err
	}

	return refundTxn, nil
}
