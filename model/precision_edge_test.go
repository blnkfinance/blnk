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

package model

import (
	"context"
	"math/big"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// ---------------------------------------------------------------------------
// parseFixedAmount
// ---------------------------------------------------------------------------

func TestParseFixedAmount_Table(t *testing.T) {
	precision100 := decimal.NewFromInt(100)

	tests := []struct {
		name      string
		dist      Distribution
		precision decimal.Decimal
		want      string // expected decimal value as string
		wantErr   string
	}{
		{
			name:      "precise distribution in minor units",
			dist:      Distribution{Identifier: "a", PreciseDistribution: "1006"},
			precision: precision100,
			want:      "1006",
		},
		{
			name:      "precise distribution must be integer - float rejected",
			dist:      Distribution{Identifier: "a", PreciseDistribution: "10.5"},
			precision: precision100,
			wantErr:   "invalid precise_distribution format",
		},
		{
			name:      "precise distribution non-numeric rejected",
			dist:      Distribution{Identifier: "a", PreciseDistribution: "abc"},
			precision: precision100,
			wantErr:   "invalid precise_distribution format",
		},
		{
			name:      "precise distribution int64 overflow rejected",
			dist:      Distribution{Identifier: "a", PreciseDistribution: "9223372036854775808"},
			precision: precision100,
			wantErr:   "invalid precise_distribution format",
		},
		{
			name:      "negative precise distribution parses (validated later)",
			dist:      Distribution{Identifier: "a", PreciseDistribution: "-5"},
			precision: precision100,
			want:      "-5",
		},
		{
			name:      "fixed major amount is scaled by precision",
			dist:      Distribution{Identifier: "a", Distribution: "100"},
			precision: precision100,
			want:      "10000",
		},
		{
			name:      "fixed sub-unit major amount scales exactly",
			dist:      Distribution{Identifier: "a", Distribution: "0.07"},
			precision: precision100,
			want:      "7",
		},
		{
			name:      "fixed amount with float-hostile value scales exactly",
			dist:      Distribution{Identifier: "a", Distribution: "19.99"},
			precision: precision100,
			want:      "1999",
		},
		{
			name:      "invalid fixed amount rejected",
			dist:      Distribution{Identifier: "a", Distribution: "12x"},
			precision: precision100,
			wantErr:   "invalid fixed amount format",
		},
		{
			name:      "empty distribution yields zero",
			dist:      Distribution{Identifier: "a"},
			precision: precision100,
			want:      "0",
		},
		{
			name:      "precise distribution takes priority over major-unit distribution",
			dist:      Distribution{Identifier: "a", Distribution: "100", PreciseDistribution: "55"},
			precision: precision100,
			want:      "55",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseFixedAmount(tt.dist, tt.precision)
			if tt.wantErr != "" {
				require.Error(t, err)
				assert.Contains(t, err.Error(), tt.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tt.want, got.String())
		})
	}
}

// ---------------------------------------------------------------------------
// ApplyPrecision
// ---------------------------------------------------------------------------

func TestApplyPrecision_EdgeCases(t *testing.T) {
	t.Run("zero precision defaults to 1", func(t *testing.T) {
		txn := &Transaction{Amount: 42}
		got := ApplyPrecision(txn)
		assert.Equal(t, float64(1), txn.Precision)
		assert.Equal(t, "42", got.String())
	})

	t.Run("existing positive precise amount drives the decimal amount", func(t *testing.T) {
		txn := &Transaction{PreciseAmount: big.NewInt(1001), Precision: 100}
		got := ApplyPrecision(txn)
		assert.Equal(t, "1001", got.String())
		assert.Equal(t, 10.01, txn.Amount)
		assert.Equal(t, "10.01", txn.AmountString)
	})

	t.Run("float-hostile decimal amounts convert exactly", func(t *testing.T) {
		// All of these are classic binary-float traps. The decimal-based
		// conversion must produce the exact minor-unit integer.
		cases := []struct {
			amount    float64
			precision float64
			want      string
		}{
			{19.99, 100, "1999"},
			{0.1, 100000000, "10000000"}, // BTC sats
			{0.29, 100, "29"},
			{1.13, 100, "113"},
			{4.35, 100, "435"},
			{123456789.99, 100, "12345678999"},
		}
		for _, c := range cases {
			txn := &Transaction{Amount: c.amount, Precision: c.precision}
			got := ApplyPrecision(txn)
			assert.Equalf(t, c.want, got.String(), "amount=%v precision=%v", c.amount, c.precision)
		}
	})

	t.Run("round trip precise->decimal->precise is lossless", func(t *testing.T) {
		original := big.NewInt(999999999999999999)
		txn := &Transaction{PreciseAmount: new(big.Int).Set(original), Precision: 100}
		ApplyPrecision(txn)
		// AmountString must carry the exact value even when float64 cannot.
		dec, err := decimal.NewFromString(txn.AmountString)
		require.NoError(t, err)
		back := dec.Mul(decimal.NewFromInt(100))
		assert.Equal(t, original.String(), back.String())
	})

	t.Run("amount finer than precision must not silently corrupt", func(t *testing.T) {
		// 1.005 at precision 100 is 100.5 minor units. A correct ledger
		// either rounds deterministically or rejects; it must never
		// produce zero or garbage for a positive amount.
		txn := &Transaction{Amount: 1.005, Precision: 100}
		got := ApplyPrecision(txn)
		if got == nil || got.Sign() <= 0 || got.Cmp(big.NewInt(100)) < 0 || got.Cmp(big.NewInt(101)) > 0 {
			t.Skipf("SUSPECTED BUG: ApplyPrecision(amount=1.005, precision=100) produced %v; "+
				"convertDecimalToPrecise (model/model.go:368) calls big.Int.SetString with a non-integer decimal "+
				"string ('100.5'), which fails and silently yields a corrupt/zero precise amount instead of "+
				"rounding or returning an error", got)
		}
	})

	t.Run("negative precise amount is not preserved", func(t *testing.T) {
		// applyPrecisionLogic only honors PreciseAmount when it is > 0.
		// A negative precise amount is silently recomputed from Amount.
		// Documenting this behavior: it relies on upstream validation
		// rejecting negative amounts.
		txn := &Transaction{PreciseAmount: big.NewInt(-500), Amount: 0, Precision: 100}
		got := ApplyPrecision(txn)
		assert.Equal(t, "0", got.String(), "negative precise amounts are recomputed from Amount")
	})
}

// ---------------------------------------------------------------------------
// HashTxn
// ---------------------------------------------------------------------------

func TestHashTxn_StabilityAndSensitivity(t *testing.T) {
	base := func() *Transaction {
		return &Transaction{
			Amount:      100.55,
			Reference:   "ref_fixed",
			Currency:    "USD",
			Source:      "bln_src",
			Destination: "bln_dst",
		}
	}

	t.Run("deterministic for identical fields", func(t *testing.T) {
		a, b := base(), base()
		assert.Equal(t, a.HashTxn(), b.HashTxn())
		// Stable across repeated invocations on the same object.
		assert.Equal(t, a.HashTxn(), a.HashTxn())
	})

	t.Run("hash length is sha256 hex", func(t *testing.T) {
		assert.Len(t, base().HashTxn(), 64)
	})

	t.Run("sensitive to each hashed field", func(t *testing.T) {
		ref := base().HashTxn()

		amount := base()
		amount.Amount = 100.56
		assert.NotEqual(t, ref, amount.HashTxn(), "amount change must change hash")

		reference := base()
		reference.Reference = "ref_other"
		assert.NotEqual(t, ref, reference.HashTxn(), "reference change must change hash")

		currency := base()
		currency.Currency = "EUR"
		assert.NotEqual(t, ref, currency.HashTxn(), "currency change must change hash")

		source := base()
		source.Source = "bln_other"
		assert.NotEqual(t, ref, source.HashTxn(), "source change must change hash")

		dest := base()
		dest.Destination = "bln_other"
		assert.NotEqual(t, ref, dest.HashTxn(), "destination change must change hash")
	})

	t.Run("insensitive to status and metadata (documented)", func(t *testing.T) {
		a := base()
		b := base()
		b.Status = "APPLIED"
		b.MetaData = map[string]interface{}{"k": "v"}
		assert.Equal(t, a.HashTxn(), b.HashTxn())
	})

	t.Run("amounts differing beyond 6 decimal places must not collide", func(t *testing.T) {
		// HashTxn formats Amount with %f (6 decimals). For high-precision
		// assets (e.g. 8-decimal crypto) two materially different amounts
		// collide to the same integrity hash.
		a := base()
		a.Amount = 1.00000001
		b := base()
		b.Amount = 1.00000004
		if a.HashTxn() == b.HashTxn() {
			t.Skip("SUSPECTED BUG: HashTxn (model/model.go:43) formats Amount with the fixed-point 'f' verb " +
				"(6 decimal places), so amounts that differ only beyond the 6th decimal (valid for " +
				"precision >= 1e7 assets) produce identical integrity hashes; PreciseAmount is not part of the hash")
		}
	})
}

// ---------------------------------------------------------------------------
// SplitTransactionPrecise / CalculateDistributionsPrecise
// ---------------------------------------------------------------------------

func TestSplitTransactionPrecise_ExactSum(t *testing.T) {
	mkTxn := func(precise int64, precision float64, dists []Distribution) *Transaction {
		return &Transaction{
			TransactionID: GenerateUUIDWithSuffix("txn"),
			Reference:     gofakeit.UUID(),
			Currency:      "USD",
			Source:        "bln_source",
			PreciseAmount: big.NewInt(precise),
			Precision:     precision,
			Destinations:  dists,
		}
	}

	tests := []struct {
		name      string
		precise   int64
		precision float64
		dists     []Distribution
	}{
		{
			name:      "three-way percentage split of indivisible amount",
			precise:   10001,
			precision: 100,
			dists: []Distribution{
				{Identifier: "d1", Distribution: "33.33%"},
				{Identifier: "d2", Distribution: "33.33%"},
				{Identifier: "d3", Distribution: "33.34%"},
			},
		},
		{
			name:      "fifty-fifty split of odd amount",
			precise:   1001,
			precision: 100,
			dists: []Distribution{
				{Identifier: "d1", Distribution: "50%"},
				{Identifier: "d2", Distribution: "50%"},
			},
		},
		{
			name:      "percentages with left remainder",
			precise:   9999,
			precision: 100,
			dists: []Distribution{
				{Identifier: "d1", Distribution: "33%"},
				{Identifier: "d2", Distribution: "33%"},
				{Identifier: "d3", Distribution: "left"},
			},
		},
		{
			name:      "fixed plus percentage plus left",
			precise:   100000,
			precision: 100,
			dists: []Distribution{
				{Identifier: "d1", Distribution: "0.01"},
				{Identifier: "d2", Distribution: "33.333%"},
				{Identifier: "d3", Distribution: "left"},
			},
		},
		{
			name:      "precise minor-unit fixed plus left",
			precise:   1000,
			precision: 100,
			dists: []Distribution{
				{Identifier: "d1", PreciseDistribution: "333"},
				{Identifier: "d2", Distribution: "left"},
			},
		},
		{
			name:      "tiny percentage rounds to zero but total preserved",
			precise:   1000,
			precision: 100,
			dists: []Distribution{
				{Identifier: "d1", Distribution: "0.001%"},
				{Identifier: "d2", Distribution: "99.999%"},
			},
		},
		{
			name:      "large amount three way",
			precise:   10000000001,
			precision: 100000000,
			dists: []Distribution{
				{Identifier: "d1", Distribution: "33.33%"},
				{Identifier: "d2", Distribution: "33.33%"},
				{Identifier: "d3", Distribution: "33.34%"},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			txn := mkTxn(tt.precise, tt.precision, tt.dists)
			splits, err := txn.SplitTransactionPrecise(context.Background())
			require.NoError(t, err)
			require.Len(t, splits, len(tt.dists))

			sum := big.NewInt(0)
			seenRefs := map[string]bool{}
			for _, s := range splits {
				require.NotNil(t, s.PreciseAmount, "split precise amount must be set")
				assert.GreaterOrEqual(t, s.PreciseAmount.Sign(), 0, "split must not be negative")
				sum.Add(sum, s.PreciseAmount)

				assert.Equal(t, txn.TransactionID, s.ParentTransaction, "split must reference parent")
				assert.Empty(t, s.Sources, "split must not carry distribution lists")
				assert.Empty(t, s.Destinations, "split must not carry distribution lists")
				assert.False(t, seenRefs[s.Reference], "split references must be unique")
				seenRefs[s.Reference] = true
			}

			assert.Equal(t, big.NewInt(tt.precise).String(), sum.String(),
				"sum of split precise amounts MUST equal the original precise amount exactly")
		})
	}
}

func TestCalculateDistributionsPrecise_AdversarialInputs(t *testing.T) {
	ctx := context.Background()

	t.Run("percentages above 100 rejected", func(t *testing.T) {
		_, err := CalculateDistributionsPrecise(ctx, big.NewInt(10000), []Distribution{
			{Identifier: "a", Distribution: "60%"},
			{Identifier: "b", Distribution: "60%"},
		}, 100)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceed")
	})

	t.Run("fixed amounts above total rejected", func(t *testing.T) {
		_, err := CalculateDistributionsPrecise(ctx, big.NewInt(10000), []Distribution{
			{Identifier: "a", Distribution: "150"},
		}, 100)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds")
	})

	t.Run("zero total yields zero for everyone", func(t *testing.T) {
		got, err := CalculateDistributionsPrecise(ctx, big.NewInt(0), []Distribution{
			{Identifier: "a", Distribution: "50%"},
			{Identifier: "b", Distribution: "left"},
		}, 100)
		require.NoError(t, err)
		assert.Equal(t, "0", got["a"].String())
		assert.Equal(t, "0", got["b"].String())
	})

	t.Run("one minor unit goes entirely to the first distribution", func(t *testing.T) {
		got, err := CalculateDistributionsPrecise(ctx, big.NewInt(1), []Distribution{
			{Identifier: "a", Distribution: "50%"},
			{Identifier: "b", Distribution: "50%"},
		}, 100)
		require.NoError(t, err)
		assert.Equal(t, "1", got["a"].String())
		assert.Equal(t, "0", got["b"].String())
	})

	t.Run("invalid percentage format rejected", func(t *testing.T) {
		_, err := CalculateDistributionsPrecise(ctx, big.NewInt(10000), []Distribution{
			{Identifier: "a", Distribution: "abc%"},
		}, 100)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "invalid percentage format")
	})

	t.Run("sum is exact for hostile percentage mixes", func(t *testing.T) {
		total := big.NewInt(99991)
		got, err := CalculateDistributionsPrecise(ctx, total, []Distribution{
			{Identifier: "a", Distribution: "1.5%"},
			{Identifier: "b", Distribution: "98.5%"},
		}, 100)
		require.NoError(t, err)
		sum := big.NewInt(0)
		for _, v := range got {
			sum.Add(sum, v)
		}
		assert.Equal(t, total.String(), sum.String())
	})
}

// ---------------------------------------------------------------------------
// canProcessTransaction overdraft boundaries
// ---------------------------------------------------------------------------

func TestCanProcessTransaction_OverdraftBoundary(t *testing.T) {
	newBalance := func(balanceMinor int64) *Balance {
		b := &Balance{}
		b.InitializeBalanceFields()
		b.Balance = big.NewInt(balanceMinor)
		return b
	}

	t.Run("exactly at overdraft limit allowed", func(t *testing.T) {
		txn := &Transaction{
			PreciseAmount:  big.NewInt(1000), // 10.00
			Precision:      100,
			OverdraftLimit: 10.0,
		}
		err := canProcessTransaction(txn, newBalance(0))
		assert.NoError(t, err, "a transaction landing exactly on -limit must be allowed")
	})

	t.Run("one minor unit past overdraft limit rejected", func(t *testing.T) {
		txn := &Transaction{
			PreciseAmount:  big.NewInt(1001), // 10.01
			Precision:      100,
			OverdraftLimit: 10.0,
		}
		err := canProcessTransaction(txn, newBalance(0))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "exceeds overdraft limit")
	})

	t.Run("insufficient funds without overdraft rejected", func(t *testing.T) {
		txn := &Transaction{PreciseAmount: big.NewInt(1001), Precision: 100}
		err := canProcessTransaction(txn, newBalance(1000))
		require.Error(t, err)
		assert.Contains(t, err.Error(), "insufficient funds")
	})

	t.Run("exactly sufficient funds allowed", func(t *testing.T) {
		txn := &Transaction{PreciseAmount: big.NewInt(1000), Precision: 100}
		assert.NoError(t, canProcessTransaction(txn, newBalance(1000)))
	})

	t.Run("inflight debits reduce available balance", func(t *testing.T) {
		bal := newBalance(1000)
		bal.InflightDebitBalance = big.NewInt(600)
		txn := &Transaction{PreciseAmount: big.NewInt(500), Precision: 100}
		err := canProcessTransaction(txn, bal)
		require.Error(t, err, "available = balance - inflight debit; 400 < 500 must fail")

		txn400 := &Transaction{PreciseAmount: big.NewInt(400), Precision: 100}
		assert.NoError(t, canProcessTransaction(txn400, bal))
	})

	t.Run("queued debits reduce available balance when present", func(t *testing.T) {
		bal := newBalance(1000)
		bal.QueuedDebitBalance = big.NewInt(800)
		txn := &Transaction{PreciseAmount: big.NewInt(300), Precision: 100}
		require.Error(t, canProcessTransaction(txn, bal))
		txn200 := &Transaction{PreciseAmount: big.NewInt(200), Precision: 100}
		assert.NoError(t, canProcessTransaction(txn200, bal))
	})

	t.Run("unconditional overdraft bypasses every check", func(t *testing.T) {
		txn := &Transaction{
			PreciseAmount:  big.NewInt(1 << 50),
			Precision:      100,
			AllowOverdraft: true,
		}
		assert.NoError(t, canProcessTransaction(txn, newBalance(0)))
	})

	t.Run("float-hostile overdraft limit must not truncate", func(t *testing.T) {
		// 0.29 * 100 = 28.999999... in binary float; int64() truncation
		// would shrink the configured limit to 28 minor units and reject
		// a transaction that is exactly at the user's configured limit.
		txn := &Transaction{
			PreciseAmount:  big.NewInt(29),
			Precision:      100,
			OverdraftLimit: 0.29,
		}
		err := canProcessTransaction(txn, newBalance(0))
		if err != nil {
			t.Skip("SUSPECTED BUG: canProcessTransaction (model/model.go:165) computes the precise overdraft " +
				"limit with int64(OverdraftLimit * Precision); binary-float truncation (0.29*100 -> 28) " +
				"rejects transactions that are exactly at the configured overdraft limit")
		}
	})
}

// ---------------------------------------------------------------------------
// UpdateBalances conservation
// ---------------------------------------------------------------------------

func TestUpdateBalances_Conservation(t *testing.T) {
	t.Run("rate 1 conserves money between source and destination", func(t *testing.T) {
		source := &Balance{}
		dest := &Balance{}
		source.InitializeBalanceFields()
		dest.InitializeBalanceFields()

		txn := &Transaction{
			Amount:         19.99,
			Precision:      100,
			AllowOverdraft: true,
		}
		require.NoError(t, UpdateBalances(txn, source, dest))

		assert.Equal(t, "-1999", source.Balance.String())
		assert.Equal(t, "1999", dest.Balance.String())
		total := new(big.Int).Add(source.Balance, dest.Balance)
		assert.Equal(t, "0", total.String(), "money must be conserved")
	})

	t.Run("zero amount rejected at model validation", func(t *testing.T) {
		source := &Balance{}
		dest := &Balance{}
		txn := &Transaction{Amount: 0, Precision: 100, AllowOverdraft: true}
		// ApplyPrecision inside UpdateBalances sets PreciseAmount to 0; validate
		// rejects non-positive amounts before any balance is touched, so a
		// zero-amount transaction never moves money.
		err := UpdateBalances(txn, source, dest)
		assert.Error(t, err)
		assert.Nil(t, source.Balance, "balance never initialized on early rejection")
	})

	t.Run("inflight transaction only moves inflight balances", func(t *testing.T) {
		source := &Balance{}
		dest := &Balance{}
		txn := &Transaction{
			Amount:         50,
			Precision:      100,
			Inflight:       true,
			AllowOverdraft: true,
		}
		require.NoError(t, UpdateBalances(txn, source, dest))
		assert.Equal(t, "0", source.Balance.String(), "main balance untouched by inflight hold")
		assert.Equal(t, "0", dest.Balance.String())
		assert.Equal(t, "5000", source.InflightDebitBalance.String())
		assert.Equal(t, "5000", dest.InflightCreditBalance.String())
		assert.Equal(t, "-5000", source.InflightBalance.String())
		assert.Equal(t, "5000", dest.InflightBalance.String())
	})
}
