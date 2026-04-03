package metrics

import (
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"
	"log"
)

// meter is the package-level OTel meter used to create all instruments.
var meter = otel.Meter("blnk")

func init() {
	if err := Init(); err != nil {
		log.Fatalf("failed to initialize metrics instruments: %v", err)
	}
}

// TransactionTotal counts transactions that reach a terminal or significant state.
// Attributes: status (APPLIED, REJECTED, INFLIGHT, VOID, COMMIT), currency
var TransactionTotal metric.Int64Counter

// TransactionDuration records the wall-clock time of RecordTransaction().
// Attributes: status
var TransactionDuration metric.Float64Histogram

// TransactionRejectedTotal counts rejected transactions broken down by reason.
// Attributes: reason (insufficient_funds, overdraft_limit, lock_contention, max_retries)
var TransactionRejectedTotal metric.Int64Counter

// QueueEnqueuedTotal counts transactions enqueued for async processing.
// Attributes: queue_name
var QueueEnqueuedTotal metric.Int64Counter

// QueueProcessingDuration records the time spent processing a transaction in the worker.
// Attributes: result (success, error, retry)
var QueueProcessingDuration metric.Float64Histogram

// BalanceCreatedTotal counts newly created balances.
var BalanceCreatedTotal metric.Int64Counter

// InflightCommitTotal counts committed inflight transactions.
var InflightCommitTotal metric.Int64Counter

// InflightVoidTotal counts voided inflight transactions.
var InflightVoidTotal metric.Int64Counter

// TransactionBatchSize records the number of transactions in a coalesced batch.
var TransactionBatchSize metric.Int64Histogram

// TransactionBatchTotal counts coalescing attempts.
// Attributes: result (success, failure, skipped)
var TransactionBatchTotal metric.Int64Counter

// HotpairsContentionTotal counts lock contention events.
var HotpairsContentionTotal metric.Int64Counter

// HotpairsLaneRoutedTotal counts transactions routed to queue lanes.
// Attributes: lane (normal, hot)
var HotpairsLaneRoutedTotal metric.Int64Counter

// WorkerRetriesTotal counts worker retry events.
// Attributes: reason (insufficient_funds, lock_contention, other)
var WorkerRetriesTotal metric.Int64Counter

// Init creates all metric instruments. It should be called once during application
// startup. Safe to call even when observability is disabled
func Init() error {
	var err error

	TransactionTotal, err = meter.Int64Counter("blnk.transaction.total",
		metric.WithDescription("Total number of transactions by status and currency"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	TransactionDuration, err = meter.Float64Histogram("blnk.transaction.duration",
		metric.WithDescription("Duration of RecordTransaction processing"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	TransactionRejectedTotal, err = meter.Int64Counter("blnk.transaction.rejected.total",
		metric.WithDescription("Total number of rejected transactions by reason"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	QueueEnqueuedTotal, err = meter.Int64Counter("blnk.queue.enqueued.total",
		metric.WithDescription("Total number of transactions enqueued for processing"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	QueueProcessingDuration, err = meter.Float64Histogram("blnk.queue.processing.duration",
		metric.WithDescription("Duration of worker transaction processing"),
		metric.WithUnit("s"),
	)
	if err != nil {
		return err
	}

	BalanceCreatedTotal, err = meter.Int64Counter("blnk.balance.created.total",
		metric.WithDescription("Total number of balances created"),
		metric.WithUnit("{balance}"),
	)
	if err != nil {
		return err
	}

	InflightCommitTotal, err = meter.Int64Counter("blnk.inflight.commit.total",
		metric.WithDescription("Total number of inflight transactions committed"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	InflightVoidTotal, err = meter.Int64Counter("blnk.inflight.void.total",
		metric.WithDescription("Total number of inflight transactions voided"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	TransactionBatchSize, err = meter.Int64Histogram("blnk.transaction.batch.size",
		metric.WithDescription("Number of transactions in a coalesced batch"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	TransactionBatchTotal, err = meter.Int64Counter("blnk.transaction.batch.total",
		metric.WithDescription("Total number of batch coalescing attempts by result"),
		metric.WithUnit("{batch}"),
	)
	if err != nil {
		return err
	}

	HotpairsContentionTotal, err = meter.Int64Counter("blnk.hotpairs.contention.total",
		metric.WithDescription("Total number of lock contention events"),
		metric.WithUnit("{event}"),
	)
	if err != nil {
		return err
	}

	HotpairsLaneRoutedTotal, err = meter.Int64Counter("blnk.hotpairs.lane.routed.total",
		metric.WithDescription("Total number of transactions routed to queue lanes"),
		metric.WithUnit("{transaction}"),
	)
	if err != nil {
		return err
	}

	WorkerRetriesTotal, err = meter.Int64Counter("blnk.worker.retries.total",
		metric.WithDescription("Total number of worker retry events by reason"),
		metric.WithUnit("{retry}"),
	)
	if err != nil {
		return err
	}

	return nil
}
