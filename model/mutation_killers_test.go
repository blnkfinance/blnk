package model

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// These tests were written to kill mutants that survived a gremlins run:
// each one pins an exact comparison boundary in money-handling code where a
// one-character operator change (> vs >=, <= vs <) previously went unnoticed.

func TestCompare_ExactBoundaries(t *testing.T) {
	ten := big.NewInt(10)
	tests := []struct {
		name      string
		value     int64
		condition string
		want      bool
	}{
		// strict > : equal must NOT satisfy
		{"gt above", 11, ">", true},
		{"gt equal", 10, ">", false},
		{"gt below", 9, ">", false},
		// strict < : equal must NOT satisfy
		{"lt below", 9, "<", true},
		{"lt equal", 10, "<", false},
		{"lt above", 11, "<", false},
		// >= : equal MUST satisfy
		{"gte equal", 10, ">=", true},
		{"gte below", 9, ">=", false},
		// <= : equal MUST satisfy
		{"lte equal", 10, "<=", true},
		{"lte above", 11, "<=", false},
		// equality operators
		{"eq equal", 10, "==", true},
		{"eq differs", 9, "==", false},
		{"ne differs", 9, "!=", true},
		{"ne equal", 10, "!=", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.want, compare(big.NewInt(tt.value), tt.condition, ten))
		})
	}
}

func TestMonitorCheckCondition_FiresOnlyPastThreshold(t *testing.T) {
	monitor := &BalanceMonitor{
		Condition: AlertCondition{Field: "balance", Operator: ">", PreciseValue: big.NewInt(1000)},
	}

	atThreshold := &Balance{Balance: big.NewInt(1000)}
	pastThreshold := &Balance{Balance: big.NewInt(1001)}
	atThreshold.InitializeBalanceFields()
	pastThreshold.InitializeBalanceFields()

	assert.False(t, monitor.CheckCondition(atThreshold), "a strict > monitor must not fire at exactly the threshold")
	assert.True(t, monitor.CheckCondition(pastThreshold))
}

func TestCommitInflight_ExactFullAmountSettles(t *testing.T) {
	txn := &Transaction{Amount: 100, Precision: 100}

	t.Run("debit", func(t *testing.T) {
		b := &Balance{InflightDebitBalance: big.NewInt(10000)}
		b.InitializeBalanceFields()
		require.NoError(t, b.CommitInflightDebit(txn))
		assert.Equal(t, "0", b.InflightDebitBalance.String(), "committing exactly the inflight amount must drain it")
		assert.Equal(t, "10000", b.DebitBalance.String(), "the full amount must land in the committed debit balance")
	})

	t.Run("credit", func(t *testing.T) {
		b := &Balance{InflightCreditBalance: big.NewInt(10000)}
		b.InitializeBalanceFields()
		require.NoError(t, b.CommitInflightCredit(txn))
		assert.Equal(t, "0", b.InflightCreditBalance.String())
		assert.Equal(t, "10000", b.CreditBalance.String())
	})

	t.Run("over-commit errors and moves nothing", func(t *testing.T) {
		over := &Transaction{Amount: 100.01, Precision: 100}
		b := &Balance{InflightDebitBalance: big.NewInt(10000)}
		b.InitializeBalanceFields()
		require.Error(t, b.CommitInflightDebit(over), "committing more than inflight must fail")
		assert.Equal(t, "10000", b.InflightDebitBalance.String(), "committing more than inflight must not move anything")
		assert.Equal(t, "0", b.DebitBalance.String())
	})
}

func TestRollbackInflight_ExactFullAmount(t *testing.T) {
	t.Run("credit", func(t *testing.T) {
		b := &Balance{InflightCreditBalance: big.NewInt(500)}
		b.InitializeBalanceFields()
		require.NoError(t, b.RollbackInflightCredit(big.NewInt(500)))
		assert.Equal(t, "0", b.InflightCreditBalance.String(), "rolling back exactly the inflight amount must drain it")
	})

	t.Run("debit", func(t *testing.T) {
		b := &Balance{InflightDebitBalance: big.NewInt(500)}
		b.InitializeBalanceFields()
		require.NoError(t, b.RollbackInflightDebit(big.NewInt(500)))
		assert.Equal(t, "0", b.InflightDebitBalance.String())
	})

	t.Run("over-rollback errors and moves nothing", func(t *testing.T) {
		b := &Balance{InflightCreditBalance: big.NewInt(500)}
		b.InitializeBalanceFields()
		require.Error(t, b.RollbackInflightCredit(big.NewInt(501)))
		assert.Equal(t, "500", b.InflightCreditBalance.String())
	})
}

func TestApplyPrecision_ZeroPreciseAmountFallsThroughToAmount(t *testing.T) {
	// A zero PreciseAmount must not be treated as authoritative: the amount
	// is recomputed from Amount, otherwise a positive transaction would be
	// recorded as zero minor units.
	txn := &Transaction{PreciseAmount: big.NewInt(0), Amount: 5, Precision: 100}
	got := ApplyPrecision(txn)
	assert.Equal(t, "500", got.String())
}

func TestValidate_ZeroAmountRejected(t *testing.T) {
	txn := &Transaction{Amount: 0, PreciseAmount: nil}
	err := txn.validate()
	require.Error(t, err, "a zero amount with no precise amount must be rejected, not just negative ones")

	positive := &Transaction{Amount: 0.01}
	assert.NoError(t, positive.validate())
}

func TestSplit_ExactlyHundredPercentAllowed(t *testing.T) {
	txn := &Transaction{
		Amount:        100,
		Precision:     100,
		PreciseAmount: big.NewInt(10000),
		Sources: []Distribution{
			{Identifier: "a", Distribution: "60%"},
			{Identifier: "b", Distribution: "40%"},
		},
	}
	_, err := txn.SplitTransactionPrecise(context.Background())
	require.NoError(t, err, "distributions summing to exactly 100%% must be accepted")
}

func TestSplit_FixedAmountEqualToRemainingAllowed(t *testing.T) {
	txn := &Transaction{
		Amount:        100,
		Precision:     100,
		PreciseAmount: big.NewInt(10000),
		Sources: []Distribution{
			{Identifier: "a", Distribution: "100"},
		},
	}
	_, err := txn.SplitTransactionPrecise(context.Background())
	require.NoError(t, err, "a fixed distribution exactly equal to the full amount must be accepted")
}
