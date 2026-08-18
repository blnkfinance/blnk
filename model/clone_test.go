package model

import (
	"math/big"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newFullBalance builds a balance with every *big.Int field populated with a
// distinct value, so an aliasing bug on any single field is detectable.
func newFullBalance() *Balance {
	return &Balance{
		BalanceID:             "bln_clone_test",
		Currency:              "USD",
		LedgerID:              "ldg_1",
		Version:               7,
		Balance:               big.NewInt(1000),
		InflightBalance:       big.NewInt(2000),
		CreditBalance:         big.NewInt(3000),
		InflightCreditBalance: big.NewInt(4000),
		DebitBalance:          big.NewInt(5000),
		InflightDebitBalance:  big.NewInt(6000),
		QueuedDebitBalance:    big.NewInt(7000),
		QueuedCreditBalance:   big.NewInt(8000),
		MetaData:              map[string]interface{}{"owner": "acme"},
	}
}

// TestBalanceCloneDoesNotAlias is the tripwire for the whole dry-run feature:
// big.Int's Add/Sub mutate their receiver, so if Clone shared any *big.Int
// with the original, applying a transaction to the clone would rewrite the
// original and a "before" snapshot would silently show "after" values.
func TestBalanceCloneDoesNotAlias(t *testing.T) {
	original := newFullBalance()
	clone := original.Clone()

	fields := []struct {
		name           string
		original, copy func(*Balance) *big.Int
	}{
		{"Balance", func(b *Balance) *big.Int { return b.Balance }, func(b *Balance) *big.Int { return b.Balance }},
		{"InflightBalance", func(b *Balance) *big.Int { return b.InflightBalance }, func(b *Balance) *big.Int { return b.InflightBalance }},
		{"CreditBalance", func(b *Balance) *big.Int { return b.CreditBalance }, func(b *Balance) *big.Int { return b.CreditBalance }},
		{"InflightCreditBalance", func(b *Balance) *big.Int { return b.InflightCreditBalance }, func(b *Balance) *big.Int { return b.InflightCreditBalance }},
		{"DebitBalance", func(b *Balance) *big.Int { return b.DebitBalance }, func(b *Balance) *big.Int { return b.DebitBalance }},
		{"InflightDebitBalance", func(b *Balance) *big.Int { return b.InflightDebitBalance }, func(b *Balance) *big.Int { return b.InflightDebitBalance }},
		{"QueuedDebitBalance", func(b *Balance) *big.Int { return b.QueuedDebitBalance }, func(b *Balance) *big.Int { return b.QueuedDebitBalance }},
		{"QueuedCreditBalance", func(b *Balance) *big.Int { return b.QueuedCreditBalance }, func(b *Balance) *big.Int { return b.QueuedCreditBalance }},
	}

	for _, f := range fields {
		assert.NotSame(t, f.original(original), f.copy(clone), "%s must not be shared with the original", f.name)
	}

	// Mutating every field on the clone the way the apply path does must leave
	// the original untouched.
	snapshot := newFullBalance()
	clone.addDebit(big.NewInt(500), false)
	clone.addDebit(big.NewInt(500), true)
	clone.addCredit(big.NewInt(500), false)
	clone.addCredit(big.NewInt(500), true)
	clone.computeBalance(false)
	clone.computeBalance(true)
	clone.QueuedDebitBalance.Add(clone.QueuedDebitBalance, big.NewInt(500))
	clone.QueuedCreditBalance.Add(clone.QueuedCreditBalance, big.NewInt(500))

	assert.Equal(t, snapshot.Balance, original.Balance)
	assert.Equal(t, snapshot.InflightBalance, original.InflightBalance)
	assert.Equal(t, snapshot.CreditBalance, original.CreditBalance)
	assert.Equal(t, snapshot.InflightCreditBalance, original.InflightCreditBalance)
	assert.Equal(t, snapshot.DebitBalance, original.DebitBalance)
	assert.Equal(t, snapshot.InflightDebitBalance, original.InflightDebitBalance)
	assert.Equal(t, snapshot.QueuedDebitBalance, original.QueuedDebitBalance)
	assert.Equal(t, snapshot.QueuedCreditBalance, original.QueuedCreditBalance)
}

func TestBalanceCloneCopiesScalarsAndMetaData(t *testing.T) {
	original := newFullBalance()
	clone := original.Clone()

	assert.Equal(t, original.BalanceID, clone.BalanceID)
	assert.Equal(t, original.Currency, clone.Currency)
	assert.Equal(t, original.Version, clone.Version)

	clone.MetaData["owner"] = "changed"
	assert.Equal(t, "acme", original.MetaData["owner"], "MetaData must not be shared with the original")
}

// TestBalanceClonePreservesNil guards the distinction InitializeBalanceFields
// relies on: an unset *big.Int must stay nil rather than becoming zero.
func TestBalanceClonePreservesNil(t *testing.T) {
	original := &Balance{BalanceID: "bln_sparse"}
	clone := original.Clone()
	require.NotNil(t, clone)

	assert.Nil(t, clone.Balance)
	assert.Nil(t, clone.InflightBalance)
	assert.Nil(t, clone.CreditBalance)
	assert.Nil(t, clone.InflightCreditBalance)
	assert.Nil(t, clone.DebitBalance)
	assert.Nil(t, clone.InflightDebitBalance)
	assert.Nil(t, clone.QueuedDebitBalance)
	assert.Nil(t, clone.QueuedCreditBalance)
	assert.Nil(t, clone.MetaData)

	// A cloned sparse balance must still initialize exactly like the original.
	clone.InitializeBalanceFields()
	assert.Equal(t, big.NewInt(0), clone.Balance)
	assert.Nil(t, original.Balance, "initializing the clone must not touch the original")
}

func TestBalanceCloneNil(t *testing.T) {
	var balance *Balance
	assert.Nil(t, balance.Clone())
}

// TestTransactionCloneDoesNotAlias covers the second half of the projection's
// safety: UpdateBalances writes PreciseAmount onto the transaction it is given.
func TestTransactionCloneDoesNotAlias(t *testing.T) {
	effectiveDate := time.Date(2026, 8, 9, 12, 0, 0, 0, time.UTC)
	original := &Transaction{
		TransactionID: "txn_clone_test",
		Amount:        100,
		Precision:     100,
		PreciseAmount: big.NewInt(10000),
		Currency:      "USD",
		EffectiveDate: &effectiveDate,
		MetaData:      map[string]interface{}{"reason": "goodwill"},
		Sources:       []Distribution{{Identifier: "@Revenue", Distribution: "60%"}},
		Destinations:  []Distribution{{Identifier: "bln_1", Distribution: "left"}},
		GroupIds:      []string{"grp_1"},
	}

	clone := original.Clone()

	assert.NotSame(t, original.PreciseAmount, clone.PreciseAmount)
	assert.NotSame(t, original.EffectiveDate, clone.EffectiveDate)

	clone.PreciseAmount.Add(clone.PreciseAmount, big.NewInt(1))
	clone.Amount = 999
	clone.MetaData["reason"] = "changed"
	clone.Sources[0].Identifier = "@Changed"
	clone.Destinations[0].Identifier = "bln_changed"
	clone.GroupIds[0] = "grp_changed"
	*clone.EffectiveDate = clone.EffectiveDate.Add(time.Hour)

	assert.Equal(t, big.NewInt(10000), original.PreciseAmount)
	assert.Equal(t, float64(100), original.Amount)
	assert.Equal(t, "goodwill", original.MetaData["reason"])
	assert.Equal(t, "@Revenue", original.Sources[0].Identifier)
	assert.Equal(t, "bln_1", original.Destinations[0].Identifier)
	assert.Equal(t, "grp_1", original.GroupIds[0])
	assert.Equal(t, effectiveDate, *original.EffectiveDate)
}

func TestTransactionClonePreservesNil(t *testing.T) {
	original := &Transaction{TransactionID: "txn_sparse"}
	clone := original.Clone()
	require.NotNil(t, clone)

	assert.Nil(t, clone.PreciseAmount)
	assert.Nil(t, clone.EffectiveDate)
	assert.Nil(t, clone.MetaData)
	assert.Nil(t, clone.Sources)
	assert.Nil(t, clone.Destinations)
	assert.Nil(t, clone.GroupIds)
}

func TestTransactionCloneNil(t *testing.T) {
	var transaction *Transaction
	assert.Nil(t, transaction.Clone())
}

// TestCloneSurvivesUpdateBalances is the end-to-end statement of why Clone
// exists: applying a real transaction to cloned balances must leave the
// originals — the "before" snapshot — completely untouched.
func TestCloneSurvivesUpdateBalances(t *testing.T) {
	source := &Balance{BalanceID: "bln_src", Currency: "USD", Balance: big.NewInt(50000), CreditBalance: big.NewInt(50000), DebitBalance: big.NewInt(0)}
	destination := &Balance{BalanceID: "bln_dst", Currency: "USD", Balance: big.NewInt(0), CreditBalance: big.NewInt(0), DebitBalance: big.NewInt(0)}

	sourceSnapshot, destinationSnapshot := source.Clone(), destination.Clone()

	transaction := &Transaction{Amount: 100, Precision: 100, Currency: "USD"}
	require.NoError(t, UpdateBalances(transaction.Clone(), source.Clone(), destination.Clone()))

	assert.Equal(t, sourceSnapshot.Balance, source.Balance)
	assert.Equal(t, sourceSnapshot.DebitBalance, source.DebitBalance)
	assert.Equal(t, destinationSnapshot.Balance, destination.Balance)
	assert.Equal(t, destinationSnapshot.CreditBalance, destination.CreditBalance)
}
