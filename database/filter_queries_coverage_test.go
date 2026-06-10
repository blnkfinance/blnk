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

package database

import (
	"context"
	"database/sql"
	"math/big"
	"os"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/internal/filter"
	"github.com/blnkfinance/blnk/model"
	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const defaultRealTestDSN = "postgres://postgres:@localhost:5432/blnk?sslmode=disable"

// openRealTestDB opens a direct connection to the shared test Postgres
// instance and wraps it in a Datasource. It bypasses the package singleton in
// db.go so tests control their own connection lifecycle. Tests are skipped if
// the database is unreachable.
func openRealTestDB(t *testing.T) Datasource {
	t.Helper()
	dsn := os.Getenv("TEST_DATABASE_URL")
	if dsn == "" {
		dsn = defaultRealTestDSN
	}
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	if err := db.Ping(); err != nil {
		_ = db.Close()
		t.Skipf("real database unavailable at %s: %v", dsn, err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return Datasource{Conn: db}
}

func eqFilter(field string, value interface{}) *filter.QueryFilterSet {
	return &filter.QueryFilterSet{Filters: []filter.QueryFilter{
		{Field: field, Operator: filter.OpEqual, Value: value},
	}}
}

// ---------------------------------------------------------------------------
// Ledgers
// ---------------------------------------------------------------------------

func TestGetAllLedgersWithFilterAndOptions_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	marker := gofakeit.UUID()
	nameA := "cov-a-" + marker
	nameB := "cov-b-" + marker
	nameC := "cov-c-" + marker
	decoyName := "cov-decoy-" + gofakeit.UUID()

	for _, name := range []string{nameA, nameB, nameC, decoyName} {
		_, err := ds.CreateLedger(model.Ledger{Name: name, MetaData: map[string]interface{}{"marker": marker}})
		require.NoError(t, err)
	}

	likeMarker := &filter.QueryFilterSet{Filters: []filter.QueryFilter{
		{Field: "name", Operator: filter.OpLike, Value: "%" + marker},
	}}

	ledgerNames := func(ledgers []model.Ledger) []string {
		names := make([]string, 0, len(ledgers))
		for _, l := range ledgers {
			names = append(names, l.Name)
		}
		return names
	}

	t.Run("like matches only marker rows and excludes decoy", func(t *testing.T) {
		ledgers, total, err := ds.GetAllLedgersWithFilterAndOptions(ctx, likeMarker, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{nameA, nameB, nameC}, ledgerNames(ledgers))
		assert.NotContains(t, ledgerNames(ledgers), decoyName)
		assert.Nil(t, total, "count must not be computed unless requested")
	})

	t.Run("eq matches exactly one row", func(t *testing.T) {
		ledgers, _, err := ds.GetAllLedgersWithFilterAndOptions(ctx, eqFilter("name", nameB), nil, 100, 0)
		require.NoError(t, err)
		require.Len(t, ledgers, 1)
		assert.Equal(t, nameB, ledgers[0].Name)
		assert.Equal(t, marker, ledgers[0].MetaData["marker"])
	})

	t.Run("ilike is case-insensitive", func(t *testing.T) {
		ledgers, _, err := ds.GetAllLedgersWithFilterAndOptions(ctx, &filter.QueryFilterSet{Filters: []filter.QueryFilter{
			{Field: "name", Operator: filter.OpILike, Value: "COV-B-" + marker},
		}}, nil, 100, 0)
		require.NoError(t, err)
		require.Len(t, ledgers, 1)
		assert.Equal(t, nameB, ledgers[0].Name)
	})

	t.Run("in matches the listed rows only", func(t *testing.T) {
		ledgers, _, err := ds.GetAllLedgersWithFilterAndOptions(ctx, &filter.QueryFilterSet{Filters: []filter.QueryFilter{
			{Field: "name", Operator: filter.OpIn, Values: []interface{}{nameA, nameC}},
		}}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{nameA, nameC}, ledgerNames(ledgers))
	})

	t.Run("include_count returns full total despite limit", func(t *testing.T) {
		ledgers, total, err := ds.GetAllLedgersWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{IncludeCount: true, SortBy: "name", SortOrder: filter.SortAsc}, 2, 0)
		require.NoError(t, err)
		require.Len(t, ledgers, 2)
		require.NotNil(t, total)
		assert.Equal(t, int64(3), *total)
	})

	t.Run("sort name asc then desc are actually ordered", func(t *testing.T) {
		asc, _, err := ds.GetAllLedgersWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{SortBy: "name", SortOrder: filter.SortAsc}, 100, 0)
		require.NoError(t, err)
		assert.Equal(t, []string{nameA, nameB, nameC}, ledgerNames(asc))

		desc, _, err := ds.GetAllLedgersWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{SortBy: "name", SortOrder: filter.SortDesc}, 100, 0)
		require.NoError(t, err)
		assert.Equal(t, []string{nameC, nameB, nameA}, ledgerNames(desc))
	})

	t.Run("limit and offset slice the sorted window", func(t *testing.T) {
		page, _, err := ds.GetAllLedgersWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{SortBy: "name", SortOrder: filter.SortAsc}, 1, 1)
		require.NoError(t, err)
		require.Len(t, page, 1)
		assert.Equal(t, nameB, page[0].Name)

		// Offset past the end yields an empty (non-nil) result.
		empty, _, err := ds.GetAllLedgersWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{SortBy: "name", SortOrder: filter.SortAsc}, 10, 50)
		require.NoError(t, err)
		assert.Empty(t, empty)
	})

	t.Run("injection probe in value is parameterized and matches nothing", func(t *testing.T) {
		probes := []string{
			"' OR '1'='1",
			nameA + "' OR '1'='1",
			"'; DROP TABLE blnk.ledgers; --",
		}
		for _, probe := range probes {
			ledgers, _, err := ds.GetAllLedgersWithFilterAndOptions(ctx, eqFilter("name", probe), nil, 100, 0)
			require.NoError(t, err)
			assert.Empty(t, ledgers, "probe %q must match nothing", probe)
		}
		// The table must still exist after the probes.
		var n int
		require.NoError(t, ds.Conn.QueryRow("SELECT COUNT(*) FROM blnk.ledgers WHERE name = $1", nameA).Scan(&n))
		assert.Equal(t, 1, n)
	})

	t.Run("hostile filter field is rejected with bad request", func(t *testing.T) {
		_, _, err := ds.GetAllLedgersWithFilterAndOptions(ctx, eqFilter("name; DROP TABLE blnk.ledgers", "x"), nil, 100, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid filter")
	})

	t.Run("hostile sort_by is rejected with bad request", func(t *testing.T) {
		_, _, err := ds.GetAllLedgersWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{SortBy: "name; DROP TABLE blnk.ledgers--"}, 100, 0)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "Invalid sort_by")
	})

	t.Run("delegate GetAllLedgersWithFilter behaves identically", func(t *testing.T) {
		ledgers, err := ds.GetAllLedgersWithFilter(ctx, likeMarker, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{nameA, nameB, nameC}, ledgerNames(ledgers))
	})
}

// ---------------------------------------------------------------------------
// Balances
// ---------------------------------------------------------------------------

func TestGetAllBalancesWithFilterAndOptions_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	marker := gofakeit.UUID()
	ledger, err := ds.CreateLedger(model.Ledger{Name: "cov-bal-ledger-" + marker})
	require.NoError(t, err)
	decoyLedger, err := ds.CreateLedger(model.Ledger{Name: "cov-bal-decoy-" + gofakeit.UUID()})
	require.NoError(t, err)

	ind1 := "cov-ind1-" + marker
	ind2 := "cov-ind2-" + marker
	ind3 := "cov-ind3-" + marker

	mkBalance := func(ledgerID, currency, indicator string, amount int64) model.Balance {
		b, err := ds.CreateBalance(model.Balance{
			Currency:      currency,
			LedgerID:      ledgerID,
			Indicator:     indicator,
			Balance:       big.NewInt(amount),
			CreditBalance: big.NewInt(amount),
			DebitBalance:  big.NewInt(0),
			MetaData:      map[string]interface{}{"marker": marker},
		})
		require.NoError(t, err)
		require.NotEmpty(t, b.BalanceID)
		return b
	}

	b1 := mkBalance(ledger.LedgerID, "USX", ind1, 100)
	b2 := mkBalance(ledger.LedgerID, "USX", ind2, 200)
	b3 := mkBalance(ledger.LedgerID, "EUX", ind3, 300)
	decoy := mkBalance(decoyLedger.LedgerID, "USX", "cov-ind-decoy-"+gofakeit.UUID(), 100)

	balanceIDs := func(balances []model.Balance) []string {
		ids := make([]string, 0, len(balances))
		for _, b := range balances {
			ids = append(ids, b.BalanceID)
		}
		return ids
	}

	ledgerEq := eqFilter("ledger_id", ledger.LedgerID)

	t.Run("eq ledger_id returns exactly my fixtures and not the decoy", func(t *testing.T) {
		balances, _, err := ds.GetAllBalancesWithFilterAndOptions(ctx, ledgerEq, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{b1.BalanceID, b2.BalanceID, b3.BalanceID}, balanceIDs(balances))
		assert.NotContains(t, balanceIDs(balances), decoy.BalanceID)
	})

	t.Run("AND of ledger_id and currency narrows the set", func(t *testing.T) {
		balances, _, err := ds.GetAllBalancesWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			LogicalOperator: filter.LogicalAnd,
			Filters: []filter.QueryFilter{
				{Field: "ledger_id", Operator: filter.OpEqual, Value: ledger.LedgerID},
				{Field: "currency", Operator: filter.OpEqual, Value: "USX"},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{b1.BalanceID, b2.BalanceID}, balanceIDs(balances))
	})

	t.Run("OR of two unique indicators returns their union", func(t *testing.T) {
		balances, _, err := ds.GetAllBalancesWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			LogicalOperator: filter.LogicalOr,
			Filters: []filter.QueryFilter{
				{Field: "indicator", Operator: filter.OpEqual, Value: ind1},
				{Field: "indicator", Operator: filter.OpEqual, Value: ind3},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{b1.BalanceID, b3.BalanceID}, balanceIDs(balances))
	})

	t.Run("between on numeric balance respects bounds", func(t *testing.T) {
		balances, _, err := ds.GetAllBalancesWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				{Field: "ledger_id", Operator: filter.OpEqual, Value: ledger.LedgerID},
				{Field: "balance", Operator: filter.OpBetween, Values: []interface{}{int64(150), int64(350)}},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{b2.BalanceID, b3.BalanceID}, balanceIDs(balances))
	})

	t.Run("isnull identity_id matches fixtures created without identity", func(t *testing.T) {
		balances, _, err := ds.GetAllBalancesWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				{Field: "ledger_id", Operator: filter.OpEqual, Value: ledger.LedgerID},
				{Field: "identity_id", Operator: filter.OpIsNull},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.Len(t, balances, 3)
	})

	t.Run("include_count with pagination", func(t *testing.T) {
		balances, total, err := ds.GetAllBalancesWithFilterAndOptions(ctx, ledgerEq,
			&filter.QueryOptions{IncludeCount: true, SortBy: "balance", SortOrder: filter.SortAsc}, 2, 0)
		require.NoError(t, err)
		require.Len(t, balances, 2)
		require.NotNil(t, total)
		assert.Equal(t, int64(3), *total)
		assert.Equal(t, []string{b1.BalanceID, b2.BalanceID}, balanceIDs(balances))
	})

	t.Run("sort by balance asc and desc actually order rows", func(t *testing.T) {
		asc, _, err := ds.GetAllBalancesWithFilterAndOptions(ctx, ledgerEq,
			&filter.QueryOptions{SortBy: "balance", SortOrder: filter.SortAsc}, 100, 0)
		require.NoError(t, err)
		assert.Equal(t, []string{b1.BalanceID, b2.BalanceID, b3.BalanceID}, balanceIDs(asc))

		desc, _, err := ds.GetAllBalancesWithFilterAndOptions(ctx, ledgerEq,
			&filter.QueryOptions{SortBy: "balance", SortOrder: filter.SortDesc}, 100, 0)
		require.NoError(t, err)
		assert.Equal(t, []string{b3.BalanceID, b2.BalanceID, b1.BalanceID}, balanceIDs(desc))
	})

	t.Run("big int balances round-trip through scan", func(t *testing.T) {
		balances, _, err := ds.GetAllBalancesWithFilterAndOptions(ctx, eqFilter("indicator", ind2), nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, balances, 1)
		assert.Equal(t, 0, balances[0].Balance.Cmp(big.NewInt(200)))
		assert.Equal(t, 0, balances[0].CreditBalance.Cmp(big.NewInt(200)))
		assert.Equal(t, ind2, balances[0].Indicator)
		assert.Equal(t, marker, balances[0].MetaData["marker"])
	})

	t.Run("injection probe value matches nothing", func(t *testing.T) {
		balances, _, err := ds.GetAllBalancesWithFilterAndOptions(ctx,
			eqFilter("indicator", ind1+"' OR '1'='1"), nil, 100, 0)
		require.NoError(t, err)
		assert.Empty(t, balances)
	})

	t.Run("delegate GetAllBalancesWithFilter behaves identically", func(t *testing.T) {
		balances, err := ds.GetAllBalancesWithFilter(ctx, ledgerEq, 100, 0)
		require.NoError(t, err)
		assert.Len(t, balances, 3)
	})
}

// ---------------------------------------------------------------------------
// Accounts
// ---------------------------------------------------------------------------

func TestGetAllAccountsWithFilterAndOptions_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	marker := gofakeit.UUID()
	ledger, err := ds.CreateLedger(model.Ledger{Name: "cov-acc-ledger-" + marker})
	require.NoError(t, err)
	identity, err := ds.CreateIdentity(model.Identity{
		FirstName: "Acc", LastName: "Owner", EmailAddress: "acc-" + marker + "@example.com",
		IdentityType: "individual",
	})
	require.NoError(t, err)
	balance, err := ds.CreateBalance(model.Balance{Currency: "USD", LedgerID: ledger.LedgerID})
	require.NoError(t, err)

	mkAccount := func(name, number, bank string) model.Account {
		acc, err := ds.CreateAccount(model.Account{
			Name:       name,
			Number:     number,
			BankName:   bank,
			Currency:   "USD",
			LedgerID:   ledger.LedgerID,
			IdentityID: identity.IdentityID,
			BalanceID:  balance.BalanceID,
			MetaData:   map[string]interface{}{"marker": marker},
		})
		require.NoError(t, err)
		return acc
	}

	accA := mkAccount("cov-acc-a-"+marker, "num-a-"+marker, "First Bank")
	accB := mkAccount("cov-acc-b-"+marker, "num-b-"+marker, "Second Bank")
	accC := mkAccount("cov-acc-c-"+marker, "num-c-"+marker, "First Bank")
	decoy := mkAccount("cov-acc-decoy-"+gofakeit.UUID(), "num-decoy-"+gofakeit.UUID(), "First Bank")

	accountIDs := func(accounts []model.Account) []string {
		ids := make([]string, 0, len(accounts))
		for _, a := range accounts {
			ids = append(ids, a.AccountID)
		}
		return ids
	}

	likeMarker := &filter.QueryFilterSet{Filters: []filter.QueryFilter{
		{Field: "name", Operator: filter.OpLike, Value: "cov-acc-%" + marker},
	}}

	t.Run("like matches marker accounts and excludes decoy", func(t *testing.T) {
		accounts, _, err := ds.GetAllAccountsWithFilterAndOptions(ctx, likeMarker, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{accA.AccountID, accB.AccountID, accC.AccountID}, accountIDs(accounts))
		assert.NotContains(t, accountIDs(accounts), decoy.AccountID)
	})

	t.Run("eq number matches exactly one account", func(t *testing.T) {
		accounts, _, err := ds.GetAllAccountsWithFilterAndOptions(ctx, eqFilter("number", accB.Number), nil, 100, 0)
		require.NoError(t, err)
		require.Len(t, accounts, 1)
		assert.Equal(t, accB.AccountID, accounts[0].AccountID)
		assert.Equal(t, ledger.LedgerID, accounts[0].LedgerID)
		assert.Equal(t, identity.IdentityID, accounts[0].IdentityID)
		assert.Equal(t, balance.BalanceID, accounts[0].BalanceID)
	})

	t.Run("AND name like + bank_name eq narrows correctly", func(t *testing.T) {
		accounts, _, err := ds.GetAllAccountsWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				{Field: "name", Operator: filter.OpLike, Value: "cov-acc-%" + marker},
				{Field: "bank_name", Operator: filter.OpEqual, Value: "First Bank"},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{accA.AccountID, accC.AccountID}, accountIDs(accounts))
	})

	t.Run("include_count with sorted pagination window", func(t *testing.T) {
		accounts, total, err := ds.GetAllAccountsWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{IncludeCount: true, SortBy: "name", SortOrder: filter.SortAsc}, 1, 1)
		require.NoError(t, err)
		require.Len(t, accounts, 1)
		assert.Equal(t, accB.AccountID, accounts[0].AccountID, "offset 1 of name-asc must be the middle account")
		require.NotNil(t, total)
		assert.Equal(t, int64(3), *total)
	})

	t.Run("sort name desc actually orders", func(t *testing.T) {
		accounts, _, err := ds.GetAllAccountsWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{SortBy: "name", SortOrder: filter.SortDesc}, 100, 0)
		require.NoError(t, err)
		assert.Equal(t, []string{accC.AccountID, accB.AccountID, accA.AccountID}, accountIDs(accounts))
	})

	t.Run("hostile sort_by rejected", func(t *testing.T) {
		_, _, err := ds.GetAllAccountsWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{SortBy: "name'; DROP TABLE blnk.accounts; --"}, 100, 0)
		require.Error(t, err)
	})

	t.Run("delegate GetAllAccountsWithFilter behaves identically", func(t *testing.T) {
		accounts, err := ds.GetAllAccountsWithFilter(ctx, eqFilter("number", accA.Number), 100, 0)
		require.NoError(t, err)
		require.Len(t, accounts, 1)
		assert.Equal(t, accA.AccountID, accounts[0].AccountID)
	})
}

// ---------------------------------------------------------------------------
// Identities
// ---------------------------------------------------------------------------

func TestGetAllIdentitiesWithFilterAndOptions_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	marker := gofakeit.UUID()
	mkIdentity := func(first, last, email, country string) model.Identity {
		idt, err := ds.CreateIdentity(model.Identity{
			FirstName:    first,
			LastName:     last,
			EmailAddress: email,
			Country:      country,
			IdentityType: "individual",
			MetaData:     map[string]interface{}{"marker": marker},
		})
		require.NoError(t, err)
		return idt
	}

	idtA := mkIdentity("cov-ann-"+marker, "Alpha", "ann-"+marker+"@example.com", "NG")
	idtB := mkIdentity("cov-bob-"+marker, "Beta", "bob-"+marker+"@example.com", "GH")
	idtC := mkIdentity("cov-cyn-"+marker, "Gamma", "cyn-"+marker+"@example.com", "NG")
	decoy := mkIdentity("cov-decoy-"+gofakeit.UUID(), "Decoy", "decoy-"+gofakeit.UUID()+"@example.com", "NG")

	identityIDs := func(identities []model.Identity) []string {
		ids := make([]string, 0, len(identities))
		for _, i := range identities {
			ids = append(ids, i.IdentityID)
		}
		return ids
	}

	likeMarker := &filter.QueryFilterSet{Filters: []filter.QueryFilter{
		{Field: "first_name", Operator: filter.OpLike, Value: "cov-%" + marker},
	}}

	t.Run("like matches marker identities and excludes decoy", func(t *testing.T) {
		identities, _, err := ds.GetAllIdentitiesWithFilterAndOptions(ctx, likeMarker, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{idtA.IdentityID, idtB.IdentityID, idtC.IdentityID}, identityIDs(identities))
		assert.NotContains(t, identityIDs(identities), decoy.IdentityID)
	})

	t.Run("eq email_address matches exactly one", func(t *testing.T) {
		identities, _, err := ds.GetAllIdentitiesWithFilterAndOptions(ctx,
			eqFilter("email_address", idtB.EmailAddress), nil, 100, 0)
		require.NoError(t, err)
		require.Len(t, identities, 1)
		assert.Equal(t, idtB.IdentityID, identities[0].IdentityID)
		assert.Equal(t, "cov-bob-"+marker, identities[0].FirstName)
	})

	t.Run("AND first_name like + country eq", func(t *testing.T) {
		identities, _, err := ds.GetAllIdentitiesWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				{Field: "first_name", Operator: filter.OpLike, Value: "cov-%" + marker},
				{Field: "country", Operator: filter.OpEqual, Value: "NG"},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{idtA.IdentityID, idtC.IdentityID}, identityIDs(identities))
	})

	t.Run("OR of two unique emails returns their union", func(t *testing.T) {
		identities, _, err := ds.GetAllIdentitiesWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			LogicalOperator: filter.LogicalOr,
			Filters: []filter.QueryFilter{
				{Field: "email_address", Operator: filter.OpEqual, Value: idtA.EmailAddress},
				{Field: "email_address", Operator: filter.OpEqual, Value: idtC.EmailAddress},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{idtA.IdentityID, idtC.IdentityID}, identityIDs(identities))
	})

	t.Run("in first_name list", func(t *testing.T) {
		identities, _, err := ds.GetAllIdentitiesWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				{Field: "first_name", Operator: filter.OpIn, Values: []interface{}{idtA.FirstName, idtB.FirstName}},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{idtA.IdentityID, idtB.IdentityID}, identityIDs(identities))
	})

	t.Run("include_count and sorted windows", func(t *testing.T) {
		identities, total, err := ds.GetAllIdentitiesWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{IncludeCount: true, SortBy: "first_name", SortOrder: filter.SortAsc}, 2, 0)
		require.NoError(t, err)
		require.Len(t, identities, 2)
		require.NotNil(t, total)
		assert.Equal(t, int64(3), *total)
		assert.Equal(t, []string{idtA.IdentityID, idtB.IdentityID}, identityIDs(identities))

		desc, _, err := ds.GetAllIdentitiesWithFilterAndOptions(ctx, likeMarker,
			&filter.QueryOptions{SortBy: "first_name", SortOrder: filter.SortDesc}, 100, 0)
		require.NoError(t, err)
		assert.Equal(t, []string{idtC.IdentityID, idtB.IdentityID, idtA.IdentityID}, identityIDs(desc))
	})

	t.Run("injection probe in email matches nothing", func(t *testing.T) {
		identities, _, err := ds.GetAllIdentitiesWithFilterAndOptions(ctx,
			eqFilter("email_address", idtA.EmailAddress+"' OR '1'='1"), nil, 100, 0)
		require.NoError(t, err)
		assert.Empty(t, identities)
	})

	t.Run("hostile field rejected", func(t *testing.T) {
		_, _, err := ds.GetAllIdentitiesWithFilterAndOptions(ctx,
			eqFilter("first_name; DROP TABLE blnk.identity", "x"), nil, 100, 0)
		require.Error(t, err)
	})

	t.Run("delegate GetAllIdentitiesWithFilter behaves identically", func(t *testing.T) {
		identities, err := ds.GetAllIdentitiesWithFilter(ctx, likeMarker, 100, 0)
		require.NoError(t, err)
		assert.Len(t, identities, 3)
	})
}

func TestGetAllIdentitiesPaginated_RealDB(t *testing.T) {
	ds := openRealTestDB(t)

	marker := gofakeit.UUID()
	for i := 0; i < 3; i++ {
		_, err := ds.CreateIdentity(model.Identity{
			FirstName:    "cov-page-" + marker,
			LastName:     "Page",
			EmailAddress: gofakeit.UUID() + "@example.com",
			IdentityType: "individual",
		})
		require.NoError(t, err)
	}

	t.Run("returns at most limit rows ordered by created_at desc", func(t *testing.T) {
		identities, err := ds.GetAllIdentitiesPaginated(2, 0)
		require.NoError(t, err)
		assert.LessOrEqual(t, len(identities), 2)
		require.NotEmpty(t, identities)
		for i := 1; i < len(identities); i++ {
			assert.False(t, identities[i].CreatedAt.After(identities[i-1].CreatedAt),
				"rows must be ordered created_at DESC")
		}
	})

	t.Run("pages are disjoint", func(t *testing.T) {
		page1, err := ds.GetAllIdentitiesPaginated(2, 0)
		require.NoError(t, err)
		page2, err := ds.GetAllIdentitiesPaginated(2, 2)
		require.NoError(t, err)

		seen := map[string]bool{}
		for _, idt := range page1 {
			seen[idt.IdentityID] = true
		}
		for _, idt := range page2 {
			assert.False(t, seen[idt.IdentityID], "identity %s appears on both pages", idt.IdentityID)
		}
	})

	t.Run("non-positive and oversized limits fall back to default of 20", func(t *testing.T) {
		for _, limit := range []int{0, -5, 101} {
			identities, err := ds.GetAllIdentitiesPaginated(limit, 0)
			require.NoError(t, err)
			assert.LessOrEqual(t, len(identities), 20, "limit %d must clamp to default 20", limit)
		}
	})
}

// ---------------------------------------------------------------------------
// Transactions (database/transaction_filters.go)
// ---------------------------------------------------------------------------

// insertTestTransaction inserts a raw transaction row so tests fully control
// status, amounts and metadata without going through the queue machinery.
func insertTestTransaction(t *testing.T, ds Datasource, txnID, source, destination, reference string,
	amount float64, preciseAmount int64, currency, status string, createdAt time.Time, metaJSON string) {
	t.Helper()
	_, err := ds.Conn.Exec(`
		INSERT INTO blnk.transactions
			(transaction_id, source, destination, reference, amount, precise_amount, precision, currency, status, description, hash, created_at, meta_data)
		VALUES ($1, $2, $3, $4, $5, $6, 100, $7, $8, 'coverage fixture', 'cov-hash', $9, $10::jsonb)
	`, txnID, source, destination, reference, amount, preciseAmount, currency, status, createdAt, metaJSON)
	require.NoError(t, err)
}

func TestGetAllTransactionsWithFilterAndOptions_RealDB(t *testing.T) {
	ds := openRealTestDB(t)
	ctx := context.Background()

	marker := gofakeit.UUID()
	ledger, err := ds.CreateLedger(model.Ledger{Name: "cov-txn-ledger-" + marker})
	require.NoError(t, err)

	srcIndicator := "cov-txn-src-" + marker
	src, err := ds.CreateBalance(model.Balance{Currency: "CVX", LedgerID: ledger.LedgerID, Indicator: srcIndicator})
	require.NoError(t, err)
	dst, err := ds.CreateBalance(model.Balance{Currency: "CVX", LedgerID: ledger.LedgerID})
	require.NoError(t, err)
	other, err := ds.CreateBalance(model.Balance{Currency: "CVX", LedgerID: ledger.LedgerID})
	require.NoError(t, err)

	now := time.Now()
	metaJSON := `{"batch": "` + marker + `"}`

	tx1 := model.GenerateUUIDWithSuffix("txn")
	tx2 := model.GenerateUUIDWithSuffix("txn")
	tx3 := model.GenerateUUIDWithSuffix("txn")
	txDecoy := model.GenerateUUIDWithSuffix("txn")

	ref1 := "cov-ref-1-" + marker
	ref2 := "cov-ref-2-" + marker
	ref3 := "cov-ref-3-" + marker

	insertTestTransaction(t, ds, tx1, src.BalanceID, dst.BalanceID, ref1, 100, 10000, "CVX", "APPLIED", now.Add(-3*time.Minute), metaJSON)
	insertTestTransaction(t, ds, tx2, src.BalanceID, dst.BalanceID, ref2, 200, 20000, "CVX", "APPLIED", now.Add(-2*time.Minute), metaJSON)
	insertTestTransaction(t, ds, tx3, dst.BalanceID, src.BalanceID, ref3, 300, 30000, "CVX", "INFLIGHT", now.Add(-1*time.Minute), metaJSON)
	insertTestTransaction(t, ds, txDecoy, other.BalanceID, dst.BalanceID, "cov-ref-decoy-"+gofakeit.UUID(),
		100, 10000, "CVX", "APPLIED", now, `{"batch": "other"}`)

	txnIDs := func(txns []model.Transaction) []string {
		ids := make([]string, 0, len(txns))
		for _, tx := range txns {
			ids = append(ids, tx.TransactionID)
		}
		return ids
	}

	batchEq := filter.QueryFilter{Field: "meta_data.batch", Operator: filter.OpEqual, Value: marker}
	batchFilter := &filter.QueryFilterSet{Filters: []filter.QueryFilter{batchEq}}

	t.Run("meta_data JSON containment matches the batch and excludes decoy", func(t *testing.T) {
		txns, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, batchFilter, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{tx1, tx2, tx3}, txnIDs(txns))
		assert.NotContains(t, txnIDs(txns), txDecoy)
	})

	t.Run("AND status eq narrows to applied transactions", func(t *testing.T) {
		txns, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				batchEq,
				{Field: "status", Operator: filter.OpEqual, Value: "APPLIED"},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{tx1, tx2}, txnIDs(txns))
	})

	t.Run("balance_id eq matches source OR destination", func(t *testing.T) {
		// src is the source of tx1/tx2 and the destination of tx3.
		txns, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				batchEq,
				{Field: "balance_id", Operator: filter.OpEqual, Value: src.BalanceID},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{tx1, tx2, tx3}, txnIDs(txns))

		// other is only a party of the decoy transaction.
		txns, _, err = ds.GetAllTransactionsWithFilterAndOptions(ctx,
			eqFilter("balance_id", other.BalanceID), nil, 100, 0)
		require.NoError(t, err)
		assert.Equal(t, []string{txDecoy}, txnIDs(txns))
	})

	t.Run("indicator filter resolves balances through CTE", func(t *testing.T) {
		txns, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				batchEq,
				{Field: "indicator", Operator: filter.OpEqual, Value: srcIndicator},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{tx1, tx2, tx3}, txnIDs(txns))
	})

	t.Run("between on amount respects bounds", func(t *testing.T) {
		txns, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				batchEq,
				{Field: "amount", Operator: filter.OpBetween, Values: []interface{}{int64(150), int64(350)}},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{tx2, tx3}, txnIDs(txns))
	})

	t.Run("reference like matches all fixture refs", func(t *testing.T) {
		txns, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				{Field: "reference", Operator: filter.OpLike, Value: "cov-ref-%-" + marker},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{tx1, tx2, tx3}, txnIDs(txns))
	})

	t.Run("in over references matches the listed subset", func(t *testing.T) {
		txns, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, &filter.QueryFilterSet{
			Filters: []filter.QueryFilter{
				{Field: "reference", Operator: filter.OpIn, Values: []interface{}{ref1, ref3}},
			},
		}, nil, 100, 0)
		require.NoError(t, err)
		assert.ElementsMatch(t, []string{tx1, tx3}, txnIDs(txns))
	})

	t.Run("include_count with limited window reports the full total", func(t *testing.T) {
		txns, total, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, batchFilter,
			&filter.QueryOptions{IncludeCount: true, SortBy: "amount", SortOrder: filter.SortAsc}, 2, 0)
		require.NoError(t, err)
		require.Len(t, txns, 2)
		require.NotNil(t, total)
		assert.Equal(t, int64(3), *total)
		assert.Equal(t, []string{tx1, tx2}, txnIDs(txns))
	})

	t.Run("sort amount asc and desc actually order", func(t *testing.T) {
		asc, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, batchFilter,
			&filter.QueryOptions{SortBy: "amount", SortOrder: filter.SortAsc}, 100, 0)
		require.NoError(t, err)
		assert.Equal(t, []string{tx1, tx2, tx3}, txnIDs(asc))

		desc, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, batchFilter,
			&filter.QueryOptions{SortBy: "amount", SortOrder: filter.SortDesc}, 100, 0)
		require.NoError(t, err)
		assert.Equal(t, []string{tx3, tx2, tx1}, txnIDs(desc))
	})

	t.Run("precise amounts round-trip as big ints", func(t *testing.T) {
		txns, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx,
			eqFilter("reference", ref3), nil, 10, 0)
		require.NoError(t, err)
		require.Len(t, txns, 1)
		assert.Equal(t, 0, txns[0].PreciseAmount.Cmp(big.NewInt(30000)))
		assert.Equal(t, marker, txns[0].MetaData["batch"])
	})

	t.Run("injection probe via reference is parameterized and harmless", func(t *testing.T) {
		txns, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx,
			eqFilter("reference", "'; DROP TABLE blnk.transactions; --"), nil, 100, 0)
		require.NoError(t, err)
		assert.Empty(t, txns)

		var n int
		require.NoError(t, ds.Conn.QueryRow(
			"SELECT COUNT(*) FROM blnk.transactions WHERE transaction_id = $1", tx1).Scan(&n))
		assert.Equal(t, 1, n, "transactions table must survive the injection probe")
	})

	t.Run("hostile sort_by is rejected", func(t *testing.T) {
		_, _, err := ds.GetAllTransactionsWithFilterAndOptions(ctx, batchFilter,
			&filter.QueryOptions{SortBy: "amount; SELECT pg_sleep(10)"}, 100, 0)
		require.Error(t, err)
	})

	t.Run("delegate GetAllTransactionsWithFilter behaves identically", func(t *testing.T) {
		txns, err := ds.GetAllTransactionsWithFilter(ctx, batchFilter, 100, 0)
		require.NoError(t, err)
		assert.Len(t, txns, 3)
	})
}
