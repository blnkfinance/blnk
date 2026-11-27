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

package search

import (
	"context"
	"slices"
	"testing"
	"time"

	"github.com/brianvoe/gofakeit/v6"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/typesense/typesense-go/typesense/api"
)

// Typesense is a hard test dependency, like Postgres and Redis: these tests
// FAIL (not skip) when it is unreachable. Start it with `docker compose up -d
// typesense` (port 8108 published, API key "blnk-api-key").
const (
	testTypesenseHost = "http://localhost:8108"
	testTypesenseKey  = "blnk-api-key"
)

func newTestTypesenseClient(t *testing.T) *TypesenseClient {
	t.Helper()
	client := NewTypesenseClient(testTypesenseKey, []string{testTypesenseHost}, DefaultTimeout)
	_, err := client.Client.Health(context.Background(), 2*time.Second)
	require.NoError(t, err, "Typesense must be running on %s for the search test suite (docker compose up -d typesense)", testTypesenseHost)
	return client
}

// resetCollections drops and recreates all known collections so each test
// starts from a clean, schema-correct state.
func resetCollections(t *testing.T, client *TypesenseClient) {
	t.Helper()
	ctx := context.Background()
	require.NoError(t, client.DropAllCollections(ctx))
	require.NoError(t, client.EnsureCollectionsExist(ctx))
}

func TestEnsureCollectionsExist_CreatesAllAndIsIdempotent(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()
	require.NoError(t, client.DropAllCollections(ctx))

	require.NoError(t, client.EnsureCollectionsExist(ctx))

	// All five collections must exist.
	for _, name := range []string{CollectionLedgers, CollectionBalances, CollectionTransactions, CollectionReconciliations, CollectionIdentities} {
		resp, err := client.Client.Collection(name).Retrieve(ctx)
		require.NoError(t, err, "collection %s should exist", name)
		assert.Equal(t, name, resp.Name)
	}

	// The default general ledger document must be present.
	result, err := client.Search(ctx, CollectionLedgers, &api.SearchCollectionParams{
		Q:       "general_ledger_id",
		QueryBy: "ledger_id",
	})
	require.NoError(t, err)
	require.NotNil(t, result.Found)
	assert.GreaterOrEqual(t, *result.Found, 1, "default general ledger should be indexed")

	// Second call is idempotent: collections already exist, no error.
	require.NoError(t, client.EnsureCollectionsExist(ctx))
}

func TestCreateCollection_IdempotentOnAlreadyExists(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()
	resetCollections(t, client)

	// Collection already exists from resetCollections: the "already exists"
	// error must be suppressed and return nil, nil.
	resp, err := client.CreateCollection(ctx, getLedgerSchema())
	require.NoError(t, err)
	assert.Nil(t, resp)
}

func TestHandleNotification_UpsertAndSearchRoundTrip(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()
	resetCollections(t, client)

	ledgerID := "ldg_" + gofakeit.UUID()
	name := "Search Test Ledger " + gofakeit.LetterN(8)
	created := time.Date(2024, 3, 1, 12, 0, 0, 0, time.UTC)

	// created_at as an RFC3339 string and a nil meta_data exercise
	// normalizeTimeFields and processMetadata on the real write path.
	data := map[string]interface{}{
		"ledger_id":  ledgerID,
		"name":       name,
		"created_at": created.Format(time.RFC3339),
		"meta_data":  nil,
	}
	require.NoError(t, client.HandleNotification(ctx, CollectionLedgers, data))

	result, err := client.Search(ctx, CollectionLedgers, &api.SearchCollectionParams{
		Q:       ledgerID,
		QueryBy: "ledger_id",
	})
	require.NoError(t, err)
	require.NotNil(t, result.Found)
	require.Equal(t, 1, *result.Found)

	doc := (*result.Hits)[0].Document
	require.NotNil(t, doc)
	assert.Equal(t, name, (*doc)["name"])
	// RFC3339 string must have been normalized to a Unix timestamp.
	assert.EqualValues(t, created.Unix(), (*doc)["created_at"])

	// Upserting the same ID with a new name updates, not duplicates.
	data["name"] = name + " v2"
	data["created_at"] = created.Format(time.RFC3339)
	require.NoError(t, client.HandleNotification(ctx, CollectionLedgers, data))

	result, err = client.Search(ctx, CollectionLedgers, &api.SearchCollectionParams{
		Q:       ledgerID,
		QueryBy: "ledger_id",
	})
	require.NoError(t, err)
	require.Equal(t, 1, *result.Found, "upsert must replace, not duplicate")
	assert.Equal(t, name+" v2", (*(*result.Hits)[0].Document)["name"])
}

func TestHandleNotification_UnknownTable(t *testing.T) {
	client := newTestTypesenseClient(t)
	err := client.HandleNotification(context.Background(), "no_such_table", map[string]interface{}{"id": "x"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "unknown collection")
}

func TestHandleBatchNotification_DependenciesBeforePrimary(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()
	resetCollections(t, client)

	ledgerID := "ldg_" + gofakeit.UUID()
	balSrc := "bln_" + gofakeit.UUID()
	balDst := "bln_" + gofakeit.UUID()
	txnID := "txn_" + gofakeit.UUID()
	now := time.Now().UTC()

	mkBalance := func(id string) map[string]interface{} {
		return map[string]interface{}{
			"balance_id": id,
			"ledger_id":  "general_ledger_id",
			"indicator":  "",
			"currency":   "USD",
			"balance":    "0",
			"created_at": now,
		}
	}

	batch := NewIndexBatch("batch_" + ledgerID)
	batch.AddDependency(CollectionBalances, balSrc, mkBalance(balSrc))
	batch.AddDependency(CollectionBalances, balDst, mkBalance(balDst))
	// Duplicate dependency must be removed by Deduplicate, not double-indexed.
	batch.AddDependency(CollectionBalances, balSrc, mkBalance(balSrc))
	batch.SetPrimary(CollectionTransactions, txnID, map[string]interface{}{
		"transaction_id": txnID,
		"source":         balSrc,
		"destination":    balDst,
		"reference":      "ref_" + gofakeit.UUID(),
		"currency":       "USD",
		"amount":         100.50,
		"precise_amount": "10050",
		"status":         "APPLIED",
		"created_at":     now,
	})

	require.NoError(t, client.HandleBatchNotification(ctx, batch))
	assert.Len(t, batch.Dependencies, 2, "duplicate dependency should have been deduplicated")

	// Both balances and the transaction must be retrievable.
	for id, collection := range map[string]string{balSrc: CollectionBalances, txnID: CollectionTransactions} {
		queryBy := collectionConfigs[collection].IDField
		result, err := client.Search(ctx, collection, &api.SearchCollectionParams{Q: id, QueryBy: queryBy})
		require.NoError(t, err)
		require.NotNil(t, result.Found)
		assert.Equal(t, 1, *result.Found, "document %s should exist in %s", id, collection)
	}
}

func TestMultiSearch_AcrossCollections(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()
	resetCollections(t, client)

	ledgerID := "ldg_" + gofakeit.UUID()
	identityID := "idt_" + gofakeit.UUID()
	require.NoError(t, client.HandleNotification(ctx, CollectionLedgers, map[string]interface{}{
		"ledger_id": ledgerID, "name": "multi search ledger", "created_at": time.Now(),
	}))
	require.NoError(t, client.HandleNotification(ctx, CollectionIdentities, map[string]interface{}{
		"identity_id": identityID, "first_name": "Multi", "last_name": "Search", "created_at": time.Now(),
	}))

	ledgerQ := ledgerID
	identityQ := identityID
	ledgerBy := "ledger_id"
	identityBy := "identity_id"
	result, err := client.MultiSearch(ctx, api.MultiSearchSearchesParameter{
		Searches: []api.MultiSearchCollectionParameters{
			{Collection: CollectionLedgers, Q: &ledgerQ, QueryBy: &ledgerBy},
			{Collection: CollectionIdentities, Q: &identityQ, QueryBy: &identityBy},
		},
	})
	require.NoError(t, err)
	require.Len(t, result.Results, 2)
	require.NotNil(t, result.Results[0].Found)
	require.NotNil(t, result.Results[1].Found)
	assert.Equal(t, 1, *result.Results[0].Found, "ledger should be found via multi-search")
	assert.Equal(t, 1, *result.Results[1].Found, "identity should be found via multi-search")
}

func TestMigrateTypeSenseSchema_AddsMissingFields(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()
	require.NoError(t, client.DropAllCollections(ctx))

	// Create the ledgers collection from a reduced schema missing "name" and
	// "meta_data" — simulating a collection created by an older Blnk version.
	// EnableNestedFields must be set at creation time: Typesense rejects
	// adding object-typed fields (meta_data) to collections without it.
	facet := true
	sortBy := "created_at"
	enableNested := true
	reduced := &api.CollectionSchema{
		Name: CollectionLedgers,
		Fields: []api.Field{
			{Name: "ledger_id", Type: "string", Facet: &facet},
			{Name: "created_at", Type: "int64", Facet: &facet},
		},
		DefaultSortingField: &sortBy,
		EnableNestedFields:  &enableNested,
	}
	_, err := client.CreateCollection(ctx, reduced)
	require.NoError(t, err)

	require.NoError(t, client.MigrateTypeSenseSchema(ctx, CollectionLedgers))

	resp, err := client.Client.Collection(CollectionLedgers).Retrieve(ctx)
	require.NoError(t, err)
	fields := make(map[string]bool)
	for _, f := range resp.Fields {
		fields[f.Name] = true
	}
	assert.True(t, fields["name"], "migration should have added the missing 'name' field")
	assert.True(t, fields["meta_data"], "migration should have added the missing 'meta_data' field")
}

func TestMigrateTypeSenseSchema_PopulatedLegacyCollections(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := t.Context()
	require.NoError(t, client.DropAllCollections(ctx))
	optional := true
	balances, transactions := getBalanceSchema(), getTransactionSchema()
	for _, schema := range []*api.CollectionSchema{balances, transactions} {
		for i, field := range schema.Fields {
			if slices.Contains(collectionConfigs[schema.Name].BigIntFields, field.Name) {
				schema.Fields[i].Type = "int64"
			}
		}
	}
	transactions.Fields = append(transactions.Fields,
		api.Field{Name: "rate", Type: "float"},
		api.Field{Name: "sources", Type: "string[]"},
		api.Field{Name: "destinations", Type: "string[]"},
		api.Field{Name: "merchant_tag", Type: "string", Optional: &optional},
	)
	for _, schema := range []*api.CollectionSchema{balances, transactions} {
		_, err := client.CreateCollection(ctx, schema)
		require.NoError(t, err)
	}
	require.NoError(t, client.EnsureCollectionsExist(ctx))

	balance := map[string]interface{}{"balance_id": "bal_existing", "ledger_id": "general_ledger_id"}
	for _, name := range collectionConfigs[CollectionBalances].BigIntFields {
		balance[name] = int64(1)
	}
	require.NoError(t, client.HandleNotification(ctx, CollectionBalances, balance))
	transaction := map[string]interface{}{
		"transaction_id": "txn_existing", "source": "bal_existing", "destination": "bal_existing",
		"precise_amount": int64(9007199254740993), "merchant_tag": "custom", "meta_data": map[string]interface{}{"invoice": "A"},
		"rate": 1.0, "sources": []string{"bal_existing"}, "destinations": []string{"bal_existing"},
	}
	require.NoError(t, client.HandleNotification(ctx, CollectionTransactions, transaction))
	delete(transaction, "rate")
	require.ErrorContains(t, client.HandleNotification(ctx, CollectionTransactions, transaction), "rate")
	delete(transaction, "sources")
	delete(transaction, "destinations")

	for range 2 {
		require.NoError(t, client.MigrateTypeSenseSchema(ctx, CollectionBalances))
		require.NoError(t, client.MigrateTypeSenseSchema(ctx, CollectionTransactions))
	}

	// The old int64 is still searchable exactly after its index changes type.
	filter := "precise_amount:=9007199254740993"
	retained, err := client.Search(ctx, CollectionTransactions, &api.SearchCollectionParams{
		Q: "*", QueryBy: "transaction_id", FilterBy: &filter,
	})
	require.NoError(t, err)
	require.NotNil(t, retained.Found)
	require.Equal(t, 1, *retained.Found)

	// A value outside int64 must survive both an update and a new document.
	const large = "9223372036854775809"
	for _, name := range collectionConfigs[CollectionBalances].BigIntFields {
		balance[name] = large
	}
	require.NoError(t, client.HandleNotification(ctx, CollectionBalances, balance))
	transaction["precise_amount"] = large
	for _, id := range []string{"txn_existing", "txn_new"} {
		transaction["transaction_id"] = id
		require.NoError(t, client.HandleNotification(ctx, CollectionTransactions, transaction))
		got, err := client.Client.Collection(CollectionTransactions).Document(id).Retrieve(ctx)
		require.NoError(t, err)
		assert.Equal(t, large, got["precise_amount"])
		assert.Equal(t, "custom", got["merchant_tag"])
		assert.Equal(t, transaction["meta_data"], got["meta_data"])
		assert.Equal(t, "bal_existing", got["source"])
		assert.Equal(t, "bal_existing", got["destination"])
	}
	schema, err := client.Client.Collection(CollectionTransactions).Retrieve(ctx)
	require.NoError(t, err)
	assert.True(t, slices.ContainsFunc(schema.Fields, func(f api.Field) bool { return f.Name == "merchant_tag" }))
}

func TestHandleNotification_FillsLinkedLegacyArrays(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := t.Context()
	require.NoError(t, client.DropAllCollections(ctx))
	require.NoError(t, client.EnsureCollectionsExist(ctx))

	bal := "bal_linked"
	require.NoError(t, client.HandleNotification(ctx, CollectionBalances, map[string]interface{}{
		"balance_id": bal, "ledger_id": "general_ledger_id",
	}))

	// A collection created while sources and destinations referenced balances.
	require.NoError(t, client.DropCollection(ctx, CollectionTransactions))
	legacy := getTransactionSchema()
	ref := "balances.balance_id"
	for i, field := range legacy.Fields {
		if field.Name == "source" || field.Name == "destination" {
			legacy.Fields[i].Reference = &ref
		}
	}
	legacy.Fields = append(legacy.Fields,
		api.Field{Name: "sources", Type: "string[]", Reference: &ref},
		api.Field{Name: "destinations", Type: "string[]", Reference: &ref},
	)
	_, err := client.CreateCollection(ctx, legacy)
	require.NoError(t, err)
	require.NoError(t, client.MigrateTypeSenseSchema(ctx, CollectionTransactions))

	txnID := "txn_linked_new"
	require.NoError(t, client.HandleNotification(ctx, CollectionTransactions, map[string]interface{}{
		"transaction_id": txnID, "source": bal, "destination": bal,
	}))
	got, err := client.Client.Collection(CollectionTransactions).Document(txnID).Retrieve(ctx)
	require.NoError(t, err)
	assert.Equal(t, []any{bal}, got["sources"])
	assert.Equal(t, []any{bal}, got["destinations"])

	// The current collection has neither list. A payment must not grow them.
	require.NoError(t, client.DropCollection(ctx, CollectionTransactions))
	require.NoError(t, client.EnsureCollectionsExist(ctx))
	currentID := "txn_current_schema"
	require.NoError(t, client.HandleNotification(ctx, CollectionTransactions, map[string]interface{}{
		"transaction_id": currentID, "source": bal, "destination": bal,
	}))
	current, err := client.Client.Collection(CollectionTransactions).Document(currentID).Retrieve(ctx)
	require.NoError(t, err)
	_, hasSources := current["sources"]
	_, hasDestinations := current["destinations"]
	assert.False(t, hasSources)
	assert.False(t, hasDestinations)
}

func TestHandleNotification_OtherClientRecreateUsesCurrentSchema(t *testing.T) {
	clientA := newTestTypesenseClient(t)
	clientB := newTestTypesenseClient(t)
	ctx := t.Context()
	require.NoError(t, clientA.DropAllCollections(ctx))
	require.NoError(t, clientA.EnsureCollectionsExist(ctx))

	bal := "bal_cross_client"
	require.NoError(t, clientA.HandleNotification(ctx, CollectionBalances, map[string]interface{}{
		"balance_id": bal, "ledger_id": "general_ledger_id",
	}))

	// Client A learns a collection that still requires the retired lists.
	require.NoError(t, clientA.DropCollection(ctx, CollectionTransactions))
	legacy := getTransactionSchema()
	ref := "balances.balance_id"
	for i, field := range legacy.Fields {
		if field.Name == "source" || field.Name == "destination" {
			legacy.Fields[i].Reference = &ref
		}
	}
	legacy.Fields = append(legacy.Fields,
		api.Field{Name: "sources", Type: "string[]", Reference: &ref},
		api.Field{Name: "destinations", Type: "string[]", Reference: &ref},
	)
	_, err := clientA.CreateCollection(ctx, legacy)
	require.NoError(t, err)
	require.NoError(t, clientA.HandleNotification(ctx, CollectionTransactions, map[string]interface{}{
		"transaction_id": "txn_cross_warm", "source": bal, "destination": bal,
	}))

	// Client B is another Core instance. It recreates the current collection.
	require.NoError(t, clientB.DropCollection(ctx, CollectionTransactions))
	require.NoError(t, clientB.EnsureCollectionsExist(ctx))

	currentID := "txn_cross_current"
	require.NoError(t, clientA.HandleNotification(ctx, CollectionTransactions, map[string]interface{}{
		"transaction_id": currentID, "source": bal, "destination": bal,
	}))
	got, err := clientA.Client.Collection(CollectionTransactions).Document(currentID).Retrieve(ctx)
	require.NoError(t, err)
	_, hasSources := got["sources"]
	_, hasDestinations := got["destinations"]
	assert.False(t, hasSources)
	assert.False(t, hasDestinations)
}

func TestMigrateTypeSenseSchema_UnknownCollection(t *testing.T) {
	client := newTestTypesenseClient(t)
	err := client.MigrateTypeSenseSchema(context.Background(), "no_such_collection")
	require.Error(t, err)
}

func TestDropCollection_RemovesAndIgnoresMissing(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()
	resetCollections(t, client)

	require.NoError(t, client.DropCollection(ctx, CollectionReconciliations))
	_, err := client.Client.Collection(CollectionReconciliations).Retrieve(ctx)
	require.Error(t, err, "dropped collection should no longer exist")

	// Dropping a collection that does not exist is not an error.
	require.NoError(t, client.DropCollection(ctx, CollectionReconciliations))
	require.NoError(t, client.DropCollection(ctx, "never_existed_"+gofakeit.LetterN(6)))
}

func TestDropAllCollections_RemovesEverything(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()
	resetCollections(t, client)

	require.NoError(t, client.DropAllCollections(ctx))
	for _, name := range []string{CollectionLedgers, CollectionBalances, CollectionTransactions, CollectionReconciliations, CollectionIdentities} {
		_, err := client.Client.Collection(name).Retrieve(ctx)
		assert.Error(t, err, "collection %s should be gone", name)
	}
}

func TestSearch_DroppedCollectionSurfacesError(t *testing.T) {
	client := newTestTypesenseClient(t)
	ctx := context.Background()
	resetCollections(t, client)
	require.NoError(t, client.DropCollection(ctx, CollectionIdentities))

	_, err := client.Search(ctx, CollectionIdentities, &api.SearchCollectionParams{Q: "x", QueryBy: "identity_id"})
	require.Error(t, err, "searching a missing collection must surface the error, not swallow it")
}

func TestClient_DeadServerReturnsError(t *testing.T) {
	// Port 1 is reserved/unused: connections fail fast. The client must
	// return an error within its timeout budget rather than hang.
	client := NewTypesenseClient("any-key", []string{"http://localhost:1"}, DefaultTimeout)

	done := make(chan error, 1)
	go func() {
		err := client.EnsureCollectionsExist(context.Background())
		done <- err
	}()

	select {
	case err := <-done:
		require.Error(t, err)
	case <-time.After(10 * time.Second):
		t.Fatal("operation against dead server did not return within 10s (client timeout is 5s)")
	}
}
