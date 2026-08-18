package blnk

import (
	"context"
	"math/big"
	"regexp"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/model"
	"github.com/go-redis/redismock/v9"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	previewSourceID      = "bln_preview_source"
	previewDestinationID = "bln_preview_destination"
)

// newPreviewTestBlnk builds a Blnk whose datasource and redis are both mocks.
//
// This is what makes the "changes no ledger state" assertions mechanical rather
// than aspirational: sqlmock fails on any query that was not explicitly
// expected, so an INSERT or UPDATE reaching the datasource fails the test by
// itself, without anyone having to assert its absence.
func newPreviewTestBlnk(t *testing.T, enableQueuedChecks bool) (*Blnk, sqlmock.Sqlmock, redismock.ClientMock) {
	t.Helper()

	cnf := &config.Configuration{
		Redis:              config.RedisConfig{Dns: "localhost:6379"},
		Server:             config.ServerConfig{SecretKey: "some-secret"},
		TokenizationSecret: "12345678901234567890123456789012",
		Queue: config.QueueConfig{
			WebhookQueue:     "webhook_queue",
			TransactionQueue: "transaction_queue",
			IndexQueue:       "index_queue",
			NumberOfQueues:   1,
		},
		Transaction: config.TransactionConfig{
			LockDuration:       30 * time.Second,
			IndexQueuePrefix:   "test_index",
			EnableQueuedChecks: enableQueuedChecks,
		},
	}
	config.ConfigStore.Store(cnf)

	datasource, dbMock, err := newTestDataSource()
	require.NoError(t, err)
	dbMock.MatchExpectationsInOrder(false)

	blnk, err := NewBlnk(datasource)
	require.NoError(t, err)

	redisClient, redisMock := redismock.NewClientMock()
	blnk.redis = redisClient
	blnk.config = cnf

	return blnk, dbMock, redisMock
}

// expectBalanceLite queues the read getSourceAndDestination performs when
// queued checks are off.
func expectBalanceLite(dbMock sqlmock.Sqlmock, balanceID, currency string, balance, credit, debit, inflightDebit int64) {
	rows := sqlmock.NewRows([]string{
		"balance_id", "indicator", "currency", "ledger_id", "balance", "credit_balance", "debit_balance",
		"inflight_balance", "inflight_credit_balance", "inflight_debit_balance", "created_at", "version",
		"track_fund_lineage", "allocation_strategy", "identity_id", "meta_data",
	}).AddRow(
		balanceID, nil, currency, "general_ledger_id", int64ToString(balance), int64ToString(credit), int64ToString(debit),
		"0", "0", int64ToString(inflightDebit), time.Now(), 3, false, "FIFO", "", nil,
	)

	dbMock.ExpectQuery(regexp.QuoteMeta(`SELECT balance_id, indicator, currency, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version, track_fund_lineage, COALESCE(allocation_strategy, 'FIFO') as allocation_strategy, COALESCE(identity_id, '') as identity_id, meta_data`)).
		WithArgs(balanceID).
		WillReturnRows(rows)
}

func int64ToString(v int64) string {
	return big.NewInt(v).String()
}

// allowLockRoundTrip permits the redis traffic a lock acquire/release performs
// without asserting on its exact shape.
func allowLockRoundTrip(redisMock redismock.ClientMock) {
	redisMock.Regexp().ExpectSetNX(`.*`, `.*`, 30*time.Second).SetVal(true)
	redisMock.Regexp().ExpectSetNX(`.*`, `.*`, 30*time.Second).SetVal(true)
	redisMock.Regexp().ExpectGet(`.*`).SetVal("")
	redisMock.Regexp().ExpectGet(`.*`).SetVal("")
}

func previewTransaction(amount float64) *model.Transaction {
	return &model.Transaction{
		Reference:   "preview_ref_1",
		Source:      previewSourceID,
		Destination: previewDestinationID,
		Amount:      amount,
		Precision:   100,
		Currency:    "USD",
	}
}

// TestPreviewTransactionProjectsWithoutWriting is the core invariant: a
// successful projection reports the resulting balances and issues no write.
// Any INSERT or UPDATE would surface as an unexpected sqlmock query.
func TestPreviewTransactionProjectsWithoutWriting(t *testing.T) {
	blnk, dbMock, redisMock := newPreviewTestBlnk(t, false)

	dbMock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)`)).
		WithArgs("preview_ref_1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	expectBalanceLite(dbMock, previewSourceID, "USD", 50000, 50000, 0, 0)
	expectBalanceLite(dbMock, previewDestinationID, "USD", 0, 0, 0, 0)
	allowLockRoundTrip(redisMock)

	preview, err := blnk.PreviewTransaction(context.Background(), previewTransaction(100))
	require.NoError(t, err)

	assert.True(t, preview.DryRun)
	assert.True(t, preview.WouldApply)
	assert.Nil(t, preview.Rejection)
	assert.Equal(t, "10000", preview.PreciseAmount)
	require.Len(t, preview.Balances, 2)

	source := preview.Balances[0]
	assert.Equal(t, model.PreviewRoleSource, source.Role)
	assert.Equal(t, "50000", source.CurrentBalance)
	assert.Equal(t, "40000", source.ResultingBalance)
	assert.Equal(t, "10000", source.ResultingDebitBalance)

	destination := preview.Balances[1]
	assert.Equal(t, model.PreviewRoleDestination, destination.Role)
	assert.Equal(t, "0", destination.CurrentBalance)
	assert.Equal(t, "10000", destination.ResultingBalance)

	assert.NoError(t, dbMock.ExpectationsWereMet())
}

// TestPreviewTransactionDoesNotMutateCallerTransaction guards the clone: the
// apply path writes PreciseAmount onto whatever transaction it is handed.
func TestPreviewTransactionDoesNotMutateCallerTransaction(t *testing.T) {
	blnk, dbMock, redisMock := newPreviewTestBlnk(t, false)

	dbMock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)`)).
		WithArgs("preview_ref_1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	expectBalanceLite(dbMock, previewSourceID, "USD", 50000, 50000, 0, 0)
	expectBalanceLite(dbMock, previewDestinationID, "USD", 0, 0, 0, 0)
	allowLockRoundTrip(redisMock)

	transaction := previewTransaction(100)
	_, err := blnk.PreviewTransaction(context.Background(), transaction)
	require.NoError(t, err)

	assert.Nil(t, transaction.PreciseAmount, "preview must not write PreciseAmount onto the caller's transaction")
	assert.Empty(t, transaction.Status, "preview must not assign a status to the caller's transaction")
}

// TestPreviewTransactionRejectsInsufficientFunds checks the projection reports
// the rejection using the same reason a real rejection is recorded under.
func TestPreviewTransactionRejectsInsufficientFunds(t *testing.T) {
	blnk, dbMock, redisMock := newPreviewTestBlnk(t, false)

	dbMock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)`)).
		WithArgs("preview_ref_1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	expectBalanceLite(dbMock, previewSourceID, "USD", 25000, 25000, 0, 0)
	expectBalanceLite(dbMock, previewDestinationID, "USD", 0, 0, 0, 0)
	allowLockRoundTrip(redisMock)

	preview, err := blnk.PreviewTransaction(context.Background(), previewTransaction(999))
	require.NoError(t, err)

	assert.False(t, preview.WouldApply)
	require.NotNil(t, preview.Rejection)
	assert.Equal(t, "insufficient_funds", preview.Rejection.Reason)
	assert.Contains(t, preview.Rejection.Message, "insufficient funds")

	// A rejected projection reports the balances unchanged: nothing would move.
	assert.Equal(t, "25000", preview.Balances[0].CurrentBalance)
	assert.Equal(t, "25000", preview.Balances[0].ResultingBalance)

	assert.NoError(t, dbMock.ExpectationsWereMet())
}

// TestPreviewTransactionZeroAmountIsRejected pins that a zero amount is a
// rejection, not a silent no-op: validate() runs inside UpdateBalances and
// errors before the zero-amount discard further down the real path is reached.
func TestPreviewTransactionZeroAmountIsRejected(t *testing.T) {
	blnk, dbMock, redisMock := newPreviewTestBlnk(t, false)

	dbMock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)`)).
		WithArgs("preview_ref_1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	expectBalanceLite(dbMock, previewSourceID, "USD", 50000, 50000, 0, 0)
	expectBalanceLite(dbMock, previewDestinationID, "USD", 0, 0, 0, 0)
	allowLockRoundTrip(redisMock)

	preview, err := blnk.PreviewTransaction(context.Background(), previewTransaction(0))
	require.NoError(t, err)

	assert.False(t, preview.WouldApply)
	require.NotNil(t, preview.Rejection)
	assert.Contains(t, preview.Rejection.Message, "must be positive")
}

// TestPreviewTransactionAvailabilityHonoursInflight checks the available figure
// is computed the way canProcessTransaction computes it, so the preview agrees
// with what enforcement will do.
func TestPreviewTransactionAvailabilityHonoursInflight(t *testing.T) {
	blnk, dbMock, redisMock := newPreviewTestBlnk(t, false)

	dbMock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)`)).
		WithArgs("preview_ref_1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	// 500.00 on the balance, but 400.00 of it is held inflight.
	expectBalanceLite(dbMock, previewSourceID, "USD", 50000, 50000, 0, 40000)
	expectBalanceLite(dbMock, previewDestinationID, "USD", 0, 0, 0, 0)
	allowLockRoundTrip(redisMock)

	// 200.00 is below the raw balance but above what is actually available.
	preview, err := blnk.PreviewTransaction(context.Background(), previewTransaction(200))
	require.NoError(t, err)

	assert.Equal(t, "10000", preview.Balances[0].CurrentAvailable)
	assert.False(t, preview.WouldApply, "a transfer above available funds must not be projected as applying")
	require.NotNil(t, preview.Rejection)
	assert.Equal(t, "insufficient_funds", preview.Rejection.Reason)
}

// TestPreviewTransactionVirtualIndicatorIsNotCreated covers the promise that a
// preview never creates an @indicator balance: the lookup misses, and the
// projection runs against a zeroed stand-in instead of an INSERT.
func TestPreviewTransactionVirtualIndicatorIsNotCreated(t *testing.T) {
	blnk, dbMock, redisMock := newPreviewTestBlnk(t, false)

	dbMock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)`)).
		WithArgs("preview_ref_1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	dbMock.ExpectQuery(regexp.QuoteMeta(`SELECT balance_id, indicator, currency, ledger_id, balance, credit_balance, debit_balance, inflight_balance, inflight_credit_balance, inflight_debit_balance, created_at, version`)).
		WithArgs("@NewRevenue", "USD").
		WillReturnError(sqlmock.ErrCancelled)
	expectBalanceLite(dbMock, previewDestinationID, "USD", 0, 0, 0, 0)
	allowLockRoundTrip(redisMock)

	transaction := previewTransaction(100)
	transaction.Source = "@NewRevenue"
	transaction.AllowOverdraft = true

	preview, err := blnk.PreviewTransaction(context.Background(), transaction)
	require.NoError(t, err)

	require.Len(t, preview.Balances, 2)
	assert.True(t, preview.Balances[0].Virtual, "an indicator with no balance yet must be reported as virtual")
	assert.Equal(t, "0", preview.Balances[0].CurrentBalance)
	assert.Equal(t, "-10000", preview.Balances[0].ResultingBalance)

	// No CreateBalance INSERT was expected; if one ran, this fails.
	assert.NoError(t, dbMock.ExpectationsWereMet())
}

// TestPreviewTransactionNotesReferenceInUse checks that an already-used
// reference is surfaced as advice rather than failing the projection.
func TestPreviewTransactionNotesReferenceInUse(t *testing.T) {
	blnk, dbMock, redisMock := newPreviewTestBlnk(t, false)

	dbMock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)`)).
		WithArgs("preview_ref_1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(true))
	expectBalanceLite(dbMock, previewSourceID, "USD", 50000, 50000, 0, 0)
	expectBalanceLite(dbMock, previewDestinationID, "USD", 0, 0, 0, 0)
	allowLockRoundTrip(redisMock)

	preview, err := blnk.PreviewTransaction(context.Background(), previewTransaction(100))
	require.NoError(t, err)

	assert.True(t, preview.WouldApply)
	require.NotEmpty(t, preview.Notes)
	assert.Contains(t, preview.Notes[0], "reference is already in use")
}

// TestPreviewTransactionNotesCurrencyMismatch pins that the preview mirrors the
// ledger rather than being stricter than it: the apply path performs no
// currency check, so a mismatch is projected with a warning, not rejected.
func TestPreviewTransactionNotesCurrencyMismatch(t *testing.T) {
	blnk, dbMock, redisMock := newPreviewTestBlnk(t, false)

	dbMock.ExpectQuery(regexp.QuoteMeta(`SELECT EXISTS(SELECT 1 FROM blnk.transactions WHERE reference = $1)`)).
		WithArgs("preview_ref_1").
		WillReturnRows(sqlmock.NewRows([]string{"exists"}).AddRow(false))
	expectBalanceLite(dbMock, previewSourceID, "EUR", 50000, 50000, 0, 0)
	expectBalanceLite(dbMock, previewDestinationID, "USD", 0, 0, 0, 0)
	allowLockRoundTrip(redisMock)

	preview, err := blnk.PreviewTransaction(context.Background(), previewTransaction(100))
	require.NoError(t, err)

	assert.True(t, preview.WouldApply, "the ledger applies mismatched currencies, so the preview must too")
	require.NotEmpty(t, preview.Notes)
	assert.Contains(t, preview.Notes[0], "currency mismatch")
}

// TestNormalizePreviewStatus covers the status a preview transaction carries
// into the apply path, which a real create would have picked up from the queue.
func TestNormalizePreviewStatus(t *testing.T) {
	tests := []struct {
		name     string
		txn      *model.Transaction
		expected string
	}{
		{"unset becomes applied", &model.Transaction{}, StatusApplied},
		{"inflight becomes inflight", &model.Transaction{Inflight: true}, StatusInflight},
		{"commit is preserved", &model.Transaction{Status: StatusCommit}, StatusCommit},
		{"void is preserved", &model.Transaction{Status: StatusVoid}, StatusVoid},
		{"queued becomes applied", &model.Transaction{Status: StatusQueued}, StatusApplied},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			normalizePreviewStatus(tt.txn)
			assert.Equal(t, tt.expected, tt.txn.Status)
		})
	}
}

func TestPreviewTransactionRequiresTransaction(t *testing.T) {
	blnk, _, _ := newPreviewTestBlnk(t, false)

	_, err := blnk.PreviewTransaction(context.Background(), nil)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "transaction is required")
}

// TestAvailableBalanceMatchesEnforcement pins the availability formula against
// canProcessTransaction's, including the queued-debit term that is only present
// when queued checks are enabled.
func TestAvailableBalanceMatchesEnforcement(t *testing.T) {
	t.Run("subtracts inflight debits", func(t *testing.T) {
		balance := &model.Balance{Balance: big.NewInt(1000), InflightDebitBalance: big.NewInt(400)}
		balance.InitializeBalanceFields()
		assert.Equal(t, big.NewInt(600), availableBalance(balance))
	})

	t.Run("subtracts queued debits when present", func(t *testing.T) {
		balance := &model.Balance{
			Balance:              big.NewInt(1000),
			InflightDebitBalance: big.NewInt(400),
			QueuedDebitBalance:   big.NewInt(100),
		}
		balance.InitializeBalanceFields()
		assert.Equal(t, big.NewInt(500), availableBalance(balance))
	})
}
