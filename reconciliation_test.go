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
package blnk

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database/mocks"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
)

// fakeExporter is a test double for reconciliationExporter that records every
// Upload and PresignGet call so assertions can verify the key format, payload,
// and expiry without a real S3 endpoint.
type fakeExporter struct {
	uploads      []fakeUpload
	presignCalls []fakePresign
	uploadErr    error
	presignURL   string
	presignErr   error
}

type fakeUpload struct {
	bucket      string
	key         string
	data        []byte
	contentType string
}

type fakePresign struct {
	bucket string
	key    string
	expiry time.Duration
}

func (f *fakeExporter) Upload(_ context.Context, bucket, key string, data []byte, contentType string) error {
	f.uploads = append(f.uploads, fakeUpload{bucket: bucket, key: key, data: data, contentType: contentType})
	return f.uploadErr
}

func (f *fakeExporter) PresignGet(bucket, key string, expiry time.Duration) (string, error) {
	f.presignCalls = append(f.presignCalls, fakePresign{bucket: bucket, key: key, expiry: expiry})
	if f.presignErr != nil {
		return "", f.presignErr
	}
	return f.presignURL, nil
}

// TestOneToManyReconciliation tests the one-to-many reconciliation strategy
func TestOneToManyReconciliation(t *testing.T) {
	cnf := &config.Configuration{
		Transaction: config.TransactionConfig{
			BatchSize:  100000,
			MaxWorkers: 1,
		},
	}
	config.ConfigStore.Store(cnf)
	mockDS := new(mocks.MockDataSource)
	blnk := &Blnk{datasource: mockDS}

	ctx := context.Background()
	externalTxns := []*model.Transaction{
		{TransactionID: "ext1", Amount: 300, CreatedAt: time.Now(), Description: "External 1 Internal 1 2 3", Reference: "REF1", Currency: "USD"},
	}
	internalTxns := []*model.Transaction{
		{TransactionID: "int1", Amount: 100, CreatedAt: time.Now().Add(-1 * time.Hour), Description: "Internal 1", Reference: "REF1-A", Currency: "USD"},
		{TransactionID: "int2", Amount: 150, CreatedAt: time.Now(), Description: "Internal 2", Reference: "REF1-B", Currency: "USD"},
		{TransactionID: "int3", Amount: 53, CreatedAt: time.Now().Add(1 * time.Hour), Description: "Internal 3", Reference: "REF1-C", Currency: "USD"},
	}

	groupedInternalTxns := map[string][]*model.Transaction{
		"parent_transaction": internalTxns,
	}

	emptyGroup := map[string][]*model.Transaction{}

	mockDS.On("GroupTransactions", mock.Anything, mock.Anything, 100000, int64(0)).Return(groupedInternalTxns, nil)
	mockDS.On("GroupTransactions", mock.Anything, mock.Anything, 100000, int64(100000)).Return(emptyGroup, nil)

	matchingRules := []model.MatchingRule{
		{
			RuleID: "parent_transaction",
			Criteria: []model.MatchingCriteria{
				{Field: "amount", Operator: "equals", AllowableDrift: 0.01}, // 1% drift allowed
				{Field: "description", Operator: "contains"},
				{Field: "reference", Operator: "contains"},
				{Field: "currency", Operator: "equals"},
			},
		},
	}

	matches, unmatched := blnk.oneToManyReconciliation(ctx, externalTxns, "parent_transaction", matchingRules, false)

	assert.Equal(t, 3, len(matches), "Expected 3 matches")
	assert.Equal(t, 0, len(unmatched), "Expected 0 unmatched transactions")

	assert.Equal(t, "ext1", matches[0].ExternalTransactionID)
	assert.Equal(t, "int1", matches[0].InternalTransactionID)
	assert.Equal(t, "ext1", matches[1].ExternalTransactionID)
	assert.Equal(t, "int2", matches[1].InternalTransactionID)
	assert.Equal(t, "ext1", matches[2].ExternalTransactionID)
	assert.Equal(t, "int3", matches[2].InternalTransactionID)
	mockDS.AssertExpectations(t)
}

func TestOneToManyReconciliationNoMatches(t *testing.T) {
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		Transaction: config.TransactionConfig{
			BatchSize:  100000,
			MaxWorkers: 1,
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
	}
	config.ConfigStore.Store(cnf)
	mockDS := new(mocks.MockDataSource)
	blnk := &Blnk{datasource: mockDS}

	ctx := context.Background()
	externalTxns := []*model.Transaction{
		{TransactionID: "ext1", Amount: 300, CreatedAt: time.Now(), Description: "External 1 Internal 1 2 3", Reference: "REF1", Currency: "USD"},
	}
	internalTxns := []*model.Transaction{
		{TransactionID: "int1", Amount: 100, CreatedAt: time.Now().Add(-1 * time.Hour), Description: "Internal 1", Reference: "REF1-A", Currency: "USD"},
		{TransactionID: "int2", Amount: 150, CreatedAt: time.Now(), Description: "Internal 2", Reference: "REF1-B", Currency: "USD"},
		{TransactionID: "int3", Amount: 54, CreatedAt: time.Now().Add(1 * time.Hour), Description: "Internal 3", Reference: "REF1-C", Currency: "USD"},
	}

	groupedInternalTxns := map[string][]*model.Transaction{
		"parent_transaction": internalTxns,
	}

	emptyGroup := map[string][]*model.Transaction{}

	mockDS.On("GroupTransactions", mock.Anything, mock.Anything, 100000, int64(0)).Return(groupedInternalTxns, nil)
	mockDS.On("GroupTransactions", mock.Anything, mock.Anything, 100000, int64(100000)).Return(emptyGroup, nil)

	matchingRules := []model.MatchingRule{
		{
			RuleID: "parent_transaction",
			Criteria: []model.MatchingCriteria{
				{Field: "amount", Operator: "equals", AllowableDrift: 0.01}, // 1% drift allowed
				{Field: "description", Operator: "contains"},
				{Field: "reference", Operator: "contains"},
				{Field: "currency", Operator: "equals"},
			},
		},
	}

	matches, unmatched := blnk.oneToManyReconciliation(ctx, externalTxns, "parent_transaction", matchingRules, false)

	assert.Equal(t, 0, len(matches), "Expected 3 matches")
	assert.Equal(t, 1, len(unmatched), "Expected 0 unmatched transactions")
	mockDS.AssertExpectations(t)
}

func TestManyToOneReconciliationUsesUploadID(t *testing.T) {
	cnf := &config.Configuration{
		Transaction: config.TransactionConfig{
			BatchSize:  100000,
			MaxWorkers: 1,
		},
	}
	config.ConfigStore.Store(cnf)
	mockDS := new(mocks.MockDataSource)
	blnk := &Blnk{datasource: mockDS}

	ctx := context.Background()
	internalTxns := []*model.Transaction{
		{TransactionID: "int1", Amount: 303, CreatedAt: time.Now(), Description: "Internal 1 External 1 2 3", Reference: "REF1", Currency: "USD"},
	}
	externalTxns := []*model.Transaction{
		{TransactionID: "ext1", Amount: 100, CreatedAt: time.Now().Add(-1 * time.Hour), Description: "External 1", Reference: "REF1-A", Currency: "USD"},
		{TransactionID: "ext2", Amount: 150, CreatedAt: time.Now(), Description: "External 2", Reference: "REF1-B", Currency: "USD"},
		{TransactionID: "ext3", Amount: 53, CreatedAt: time.Now().Add(1 * time.Hour), Description: "External 3", Reference: "REF1-C", Currency: "USD"},
	}

	groupedExternalTxns := map[string][]*model.Transaction{
		"ref-group-1": externalTxns,
	}
	emptyGroup := map[string][]*model.Transaction{}

	mockDS.On("FetchAndGroupExternalTransactions", mock.Anything, "upload123", "reference", 100000, int64(0)).
		Return(groupedExternalTxns, nil).Once()
	mockDS.On("FetchAndGroupExternalTransactions", mock.Anything, "upload123", "reference", 100000, int64(100000)).
		Return(emptyGroup, nil).Once()

	matchingRules := []model.MatchingRule{
		{
			RuleID: "reference",
			Criteria: []model.MatchingCriteria{
				{Field: "amount", Operator: "equals", AllowableDrift: 0.01},
				{Field: "description", Operator: "contains"},
				{Field: "reference", Operator: "contains"},
				{Field: "currency", Operator: "equals"},
			},
		},
	}

	matches, unmatched := blnk.manyToOneReconciliation(ctx, internalTxns, "upload123", "reference", matchingRules, true)

	assert.Equal(t, 3, len(matches), "Expected 3 matches")
	assert.Equal(t, 0, len(unmatched), "Expected 0 unmatched transactions")
	assert.Equal(t, "ext1", matches[0].ExternalTransactionID)
	assert.Equal(t, "int1", matches[0].InternalTransactionID)
	assert.Equal(t, "ext2", matches[1].ExternalTransactionID)
	assert.Equal(t, "int1", matches[1].InternalTransactionID)
	assert.Equal(t, "ext3", matches[2].ExternalTransactionID)
	assert.Equal(t, "int1", matches[2].InternalTransactionID)
	mockDS.AssertExpectations(t)
}

// TestMatchingRules tests the matching rules functionality
func TestMatchingRules(t *testing.T) {
	blnk := &Blnk{}

	// A single captured timestamp keeps the date-drift case exactly at the
	// 3600s boundary; two time.Now() calls would add nanoseconds and
	// (correctly) fail the fractional-seconds comparison.
	now := time.Now()

	externalTxn := &model.Transaction{
		TransactionID: "ext1",
		Amount:        100,
		CreatedAt:     now,
		Description:   "Test transaction",
		Reference:     "REF123",
		Currency:      "USD",
	}

	internalTxn := model.Transaction{
		TransactionID: "int1",
		Amount:        101,
		CreatedAt:     now.Add(1 * time.Hour),
		Description:   "Internal test transaction",
		Reference:     "INT-REF123",
		Currency:      "USD",
	}

	tests := []struct {
		name          string
		matchingRules []model.MatchingRule
		expected      bool
	}{
		{
			name: "Amount matching with drift",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule1",
					Criteria: []model.MatchingCriteria{
						{Field: "amount", Operator: "equals", AllowableDrift: 0.01},
					},
				},
			},
			expected: true,
		},
		{
			name: "Amount matching with drift",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule1",
					Criteria: []model.MatchingCriteria{
						{Field: "amount", Operator: "equals", AllowableDrift: 0},
					},
				},
			},
			expected: false,
		},
		{
			name: "Date matching with drift",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule2",
					Criteria: []model.MatchingCriteria{
						{Field: "date", Operator: "equals", AllowableDrift: 3600},
					},
				},
			},
			expected: true,
		},
		{
			name: "Description partial matching",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule3",
					Criteria: []model.MatchingCriteria{
						{Field: "description", Operator: "contains"},
					},
				},
			},
			expected: true,
		},
		{
			name: "Reference partial matching",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule4",
					Criteria: []model.MatchingCriteria{
						{Field: "reference", Operator: "contains"},
					},
				},
			},
			expected: true,
		},
		{
			name: "Currency exact matching",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule5",
					Criteria: []model.MatchingCriteria{
						{Field: "currency", Operator: "equals"},
					},
				},
			},
			expected: true,
		},
		{
			name: "Multiple criteria matching",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule6",
					Criteria: []model.MatchingCriteria{
						{Field: "amount", Operator: "equals", AllowableDrift: 0.01},
						{Field: "currency", Operator: "equals"},
					},
				},
			},
			expected: true,
		},
		{
			name: "Non-matching rule",
			matchingRules: []model.MatchingRule{
				{
					RuleID: "rule7",
					Criteria: []model.MatchingCriteria{
						{Field: "amount", Operator: "equals", AllowableDrift: 0},
						{Field: "description", Operator: "equals"},
					},
				},
			},
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := blnk.matchesRules(externalTxn, internalTxn, tt.matchingRules)
			assert.Equal(t, tt.expected, result)
		})
	}
}

// TestReconciliationEdgeCases tests edge cases in the reconciliation process
func TestReconciliationEdgeCases(t *testing.T) {
	cnf := &config.Configuration{
		Redis: config.RedisConfig{
			Dns: "localhost:6379",
		},
		Transaction: config.TransactionConfig{
			BatchSize:  100000,
			MaxWorkers: 1,
		},
		Queue: config.QueueConfig{
			WebhookQueue:   "webhook_queue",
			NumberOfQueues: 1,
		},
		Reconciliation: config.ReconciliationConfig{
			ProgressInterval: 100,
		},
	}
	config.ConfigStore.Store(cnf)
	mockDS := new(mocks.MockDataSource)

	blnk := &Blnk{datasource: mockDS}

	ctx := context.Background()

	t.Run("Empty external transactions", func(t *testing.T) {
		externalTxns := []*model.Transaction{}
		matchingRules := []model.MatchingRule{
			{
				RuleID: "rule1",
				Criteria: []model.MatchingCriteria{
					{Field: "amount", Operator: "equals", AllowableDrift: 0},
				},
			},
		}

		matches, unmatched := blnk.oneToOneReconciliation(ctx, externalTxns, matchingRules)

		assert.Equal(t, 0, len(matches), "Expected 0 matches")
		assert.Equal(t, 0, len(unmatched), "Expected 0 unmatched transactions")
	})

	t.Run("No matching internal transactions", func(t *testing.T) {
		externalTxns := []*model.Transaction{
			{TransactionID: "ext1", Amount: 100, CreatedAt: time.Now()},
		}

		mockDS.On("GetTransactionsByCriteria", mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, mock.Anything, 100000, int64(0)).Return([]*model.Transaction{}, nil).Once()

		matchingRules := []model.MatchingRule{
			{
				RuleID: "rule1",
				Criteria: []model.MatchingCriteria{
					{Field: "amount", Operator: "equals", AllowableDrift: 0},
				},
			},
		}

		matches, unmatched := blnk.oneToOneReconciliation(ctx, externalTxns, matchingRules)

		assert.Equal(t, 0, len(matches), "Expected 0 matches")
		assert.Equal(t, 1, len(unmatched), "Expected 1 unmatched transaction")

		mockDS.AssertExpectations(t)
	})

	t.Run("One-to-many with no matching group", func(t *testing.T) {
		externalTxns := []*model.Transaction{
			{TransactionID: "ext1", Amount: 300, CreatedAt: time.Now()},
		}
		groupedInternalTxns := map[string][]*model.Transaction{}

		mockDS.On("GroupTransactions", mock.Anything, mock.Anything, 100000, int64(0)).Return(groupedInternalTxns, nil)

		matchingRules := []model.MatchingRule{
			{
				RuleID: "rule1",
				Criteria: []model.MatchingCriteria{
					{Field: "amount", Operator: "equals", AllowableDrift: 1},
				},
			},
		}

		matches, unmatched := blnk.oneToManyReconciliation(ctx, externalTxns, "", matchingRules, false)

		assert.Equal(t, 0, len(matches), "Expected 0 matches")
		// Inputs that never get a group batch must be reported unmatched,
		// not silently dropped from the reconciliation results.
		assert.Equal(t, 1, len(unmatched), "Expected 1 unmatched transaction")

		mockDS.AssertExpectations(t)
	})
}

func TestExportReconciliationToS3_JSON(t *testing.T) {
	config.ConfigStore.Store(&config.Configuration{S3BucketName: "test-bucket"})
	mockDS := new(mocks.MockDataSource)
	exp := &fakeExporter{}
	b := &Blnk{datasource: mockDS, exporter: exp}

	rec := model.Reconciliation{ReconciliationID: "rec_json", ExportType: "json"}
	matchTime := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)

	mockDS.On("GetReconciliation", mock.Anything, "rec_json").
		Return(&model.Reconciliation{ReconciliationID: "rec_json"}, nil).Once()
	mockDS.On("GetMatchesByReconciliationID", mock.Anything, "rec_json").
		Return([]*model.Match{
			{ExternalTransactionID: "ext1", InternalTransactionID: "int1", Amount: 100.5, Date: matchTime},
		}, nil).Once()
	mockDS.On("GetUnmatchedByReconciliationID", mock.Anything, "rec_json").
		Return([]string{"ext2"}, nil).Once()
	mockDS.On("UpdateReconciliationExportKey", mock.Anything, "rec_json",
		mock.AnythingOfType("string"), mock.AnythingOfType("string")).
		Return(nil).Once()

	err := b.exportReconciliationToS3(context.Background(), rec)
	assert.NoError(t, err)
	assert.Len(t, exp.uploads, 2, "expected one upload for matched, one for unmatched")

	// Both keys share the same random id and date folder; they differ only in
	// the suffix and extension. Compute the expected date folder at runtime so
	// the test is not tied to a specific calendar day.
	dateFolder := "reconciliations/" + time.Now().Format("2006-01-02") + "/"
	matchedKey := exp.uploads[0].key
	unmatchedKey := exp.uploads[1].key
	assert.True(t, strings.HasPrefix(matchedKey, dateFolder), "matched key has date folder: %s", matchedKey)
	assert.True(t, strings.HasSuffix(matchedKey, "-matched.json"), "matched key has suffix: %s", matchedKey)
	assert.True(t, strings.HasSuffix(unmatchedKey, "-unmatched.json"), "unmatched key has suffix: %s", unmatchedKey)

	// Strip the suffixes and verify the base id is identical (same upload batch).
	matchedBase := strings.TrimSuffix(strings.TrimPrefix(matchedKey, dateFolder), "-matched.json")
	unmatchedBase := strings.TrimSuffix(strings.TrimPrefix(unmatchedKey, dateFolder), "-unmatched.json")
	assert.Equal(t, matchedBase, unmatchedBase, "matched and unmatched keys share the same base id")

	assert.Equal(t, "application/json", exp.uploads[0].contentType)
	assert.Equal(t, "application/json", exp.uploads[1].contentType)

	var gotMatches []model.Match
	assert.NoError(t, json.Unmarshal(exp.uploads[0].data, &gotMatches))
	assert.Len(t, gotMatches, 1)
	assert.Equal(t, "ext1", gotMatches[0].ExternalTransactionID)
	assert.Equal(t, 100.5, gotMatches[0].Amount)

	var gotUnmatched []string
	assert.NoError(t, json.Unmarshal(exp.uploads[1].data, &gotUnmatched))
	assert.Equal(t, []string{"ext2"}, gotUnmatched)

	mockDS.AssertExpectations(t)
}

func TestExportReconciliationToS3_CSV(t *testing.T) {
	config.ConfigStore.Store(&config.Configuration{S3BucketName: "test-bucket"})
	mockDS := new(mocks.MockDataSource)
	exp := &fakeExporter{}
	b := &Blnk{datasource: mockDS, exporter: exp}

	rec := model.Reconciliation{ReconciliationID: "rec_csv", ExportType: "csv"}
	matchTime := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)

	mockDS.On("GetReconciliation", mock.Anything, "rec_csv").
		Return(&model.Reconciliation{ReconciliationID: "rec_csv"}, nil).Once()
	mockDS.On("GetMatchesByReconciliationID", mock.Anything, "rec_csv").
		Return([]*model.Match{
			{ExternalTransactionID: "ext1", InternalTransactionID: "int1", Amount: 200.75, Date: matchTime},
		}, nil).Once()
	mockDS.On("GetUnmatchedByReconciliationID", mock.Anything, "rec_csv").
		Return([]string{"ext2"}, nil).Once()
	mockDS.On("UpdateReconciliationExportKey", mock.Anything, "rec_csv",
		mock.AnythingOfType("string"), mock.AnythingOfType("string")).
		Return(nil).Once()

	err := b.exportReconciliationToS3(context.Background(), rec)
	assert.NoError(t, err)
	assert.Len(t, exp.uploads, 2)

	assert.True(t, strings.HasSuffix(exp.uploads[0].key, "-matched.csv"))
	assert.True(t, strings.HasSuffix(exp.uploads[1].key, "-unmatched.csv"))
	assert.Equal(t, "text/csv", exp.uploads[0].contentType)
	assert.Equal(t, "text/csv", exp.uploads[1].contentType)

	matchedCSV := string(exp.uploads[0].data)
	assert.Contains(t, matchedCSV, "external_transaction_id,internal_transaction_id,amount,date")
	assert.Contains(t, matchedCSV, "ext1,int1,200.75,")
	assert.Contains(t, matchedCSV, matchTime.UTC().Format(time.RFC3339))

	unmatchedCSV := string(exp.uploads[1].data)
	assert.Contains(t, unmatchedCSV, "external_transaction_id")
	assert.Contains(t, unmatchedCSV, "ext2")

	mockDS.AssertExpectations(t)
}

func TestExportReconciliationToS3_UploadError(t *testing.T) {
	config.ConfigStore.Store(&config.Configuration{S3BucketName: "test-bucket"})
	mockDS := new(mocks.MockDataSource)
	exp := &fakeExporter{uploadErr: fmt.Errorf("s3 unavailable")}
	b := &Blnk{datasource: mockDS, exporter: exp}

	rec := model.Reconciliation{ReconciliationID: "rec_err", ExportType: "json"}

	mockDS.On("GetReconciliation", mock.Anything, "rec_err").
		Return(&model.Reconciliation{ReconciliationID: "rec_err"}, nil).Once()
	mockDS.On("GetMatchesByReconciliationID", mock.Anything, "rec_err").
		Return([]*model.Match{}, nil).Once()
	mockDS.On("GetUnmatchedByReconciliationID", mock.Anything, "rec_err").
		Return([]string{}, nil).Once()

	err := b.exportReconciliationToS3(context.Background(), rec)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to upload matched export")

	// Keys must NOT be persisted when the upload fails.
	mockDS.AssertNotCalled(t, "UpdateReconciliationExportKey", mock.Anything, mock.Anything, mock.Anything, mock.Anything)
}

func TestExportReconciliationToS3_UnsupportedType(t *testing.T) {
	config.ConfigStore.Store(&config.Configuration{S3BucketName: "test-bucket"})
	mockDS := new(mocks.MockDataSource)
	exp := &fakeExporter{}
	b := &Blnk{datasource: mockDS, exporter: exp}

	rec := model.Reconciliation{ReconciliationID: "rec_bad", ExportType: "xml"}
	mockDS.On("GetReconciliation", mock.Anything, "rec_bad").
		Return(&model.Reconciliation{ReconciliationID: "rec_bad"}, nil).Once()
	mockDS.On("GetMatchesByReconciliationID", mock.Anything, "rec_bad").
		Return([]*model.Match{}, nil).Once()
	mockDS.On("GetUnmatchedByReconciliationID", mock.Anything, "rec_bad").
		Return([]string{}, nil).Once()

	err := b.exportReconciliationToS3(context.Background(), rec)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported export type")
	assert.Empty(t, exp.uploads, "no uploads should occur for an unsupported type")
}

func TestGetReconciliationExportURL_Ready(t *testing.T) {
	config.ConfigStore.Store(&config.Configuration{S3BucketName: "test-bucket"})
	mockDS := new(mocks.MockDataSource)
	exp := &fakeExporter{presignURL: "https://fake/presigned?sig=abc"}
	b := &Blnk{datasource: mockDS, exporter: exp}

	mockDS.On("GetReconciliation", mock.Anything, "rec_ready").Return(&model.Reconciliation{
		ReconciliationID:     "rec_ready",
		ExportS3KeyMatched:   "reconciliations/2026-06-27/abc-matched.json",
		ExportS3KeyUnmatched: "reconciliations/2026-06-27/abc-unmatched.json",
	}, nil).Once()

	urls, err := b.GetReconciliationExportURL(context.Background(), "rec_ready")
	assert.NoError(t, err)
	assert.Equal(t, "https://fake/presigned?sig=abc", urls["matched"])
	assert.Equal(t, "https://fake/presigned?sig=abc", urls["unmatched"])
	assert.Len(t, exp.presignCalls, 2)
	assert.Equal(t, 24*time.Hour, exp.presignCalls[0].expiry, "presigned URL TTL must be 24h")
	assert.Equal(t, 24*time.Hour, exp.presignCalls[1].expiry)
	mockDS.AssertExpectations(t)
}

func TestGetReconciliationExportURL_NotReady(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	mockDS.On("GetReconciliation", mock.Anything, "rec_not_exported").Return(&model.Reconciliation{
		ReconciliationID: "rec_not_exported",
	}, nil).Once()

	urls, err := b.GetReconciliationExportURL(context.Background(), "rec_not_exported")
	assert.Nil(t, urls)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok, "expected APIError")
	assert.Equal(t, apierror.ErrReconExportNotReady, apiErr.Code)
}

func TestGetReconciliationExportURL_ReconNotFound(t *testing.T) {
	mockDS := new(mocks.MockDataSource)
	b := &Blnk{datasource: mockDS}

	mockDS.On("GetReconciliation", mock.Anything, "rec_missing").Return(
		(*model.Reconciliation)(nil),
		apierror.NewAPIError(apierror.ErrNotFound, "Reconciliation with ID 'rec_missing' not found", nil),
	).Once()

	urls, err := b.GetReconciliationExportURL(context.Background(), "rec_missing")
	assert.Nil(t, urls)
	assert.Error(t, err)
	// Blnk.GetReconciliation wraps the datasource error with fmt.Errorf("...%w", err),
	// so the error reaches us as a wrapped error. Use errors.As to unwrap it.
	var apiErr apierror.APIError
	assert.True(t, errors.As(err, &apiErr), "expected wrapped APIError, got %T", err)
	assert.Equal(t, apierror.ErrNotFound, apiErr.Code)
}

func TestGetReconciliationExportURL_PresignError(t *testing.T) {
	config.ConfigStore.Store(&config.Configuration{S3BucketName: "test-bucket"})
	mockDS := new(mocks.MockDataSource)
	exp := &fakeExporter{presignErr: fmt.Errorf("presign failed")}
	b := &Blnk{datasource: mockDS, exporter: exp}

	mockDS.On("GetReconciliation", mock.Anything, "rec_presign_err").Return(&model.Reconciliation{
		ReconciliationID:     "rec_presign_err",
		ExportS3KeyMatched:   "k-matched",
		ExportS3KeyUnmatched: "k-unmatched",
	}, nil).Once()

	urls, err := b.GetReconciliationExportURL(context.Background(), "rec_presign_err")
	assert.Nil(t, urls)
	assert.Error(t, err)
	apiErr, ok := err.(apierror.APIError)
	assert.True(t, ok, "expected APIError")
	assert.Equal(t, apierror.ErrReconExportS3Failed, apiErr.Code)
}
