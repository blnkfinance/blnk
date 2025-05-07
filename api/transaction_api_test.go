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
package api

import (
	"context"
	"math/big"
	"net/http"
	"testing"

	"github.com/brianvoe/gofakeit/v6"
	model2 "github.com/jerry-enebeli/blnk/api/model"
	"github.com/jerry-enebeli/blnk/internal/request"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/stretchr/testify/assert"
)

func TestRecordTransaction(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	// Create ledger and balances for testing
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		t.Fatalf("Failed to create ledger: %v", err)
	}

	newSourceBalance, err := b.CreateBalance(context.Background(), model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Fatalf("Failed to create source balance: %v", err)
	}

	newDestinationBalance, err := b.CreateBalance(context.Background(), model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Fatalf("Failed to create destination balance: %v", err)
	}

	tests := []struct {
		name         string
		payload      model2.RecordTransaction
		expectedCode int
		wantErr      bool
	}{
		{
			name: "Valid Transaction",
			payload: model2.RecordTransaction{
				Amount:      922337203.6854,
				Precision:   10000000000,
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Currency:    "NGN",
				Source:      newSourceBalance.BalanceID,
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusCreated,
			wantErr:      false,
		},
		{
			name: "Valid Transaction With precision 100",
			payload: model2.RecordTransaction{
				Amount:      100.68,
				Precision:   100,
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Currency:    "NGN",
				Source:      newSourceBalance.BalanceID,
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusCreated,
			wantErr:      false,
		},
		{
			name: "Missing Amount",
			payload: model2.RecordTransaction{
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Currency:    "NGN",
				Source:      newSourceBalance.BalanceID,
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Missing Reference",
			payload: model2.RecordTransaction{
				Amount:      750,
				Description: "For fees",
				Currency:    "NGN",
				Source:      newSourceBalance.BalanceID,
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Missing Currency",
			payload: model2.RecordTransaction{
				Amount:      750,
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Source:      newSourceBalance.BalanceID,
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Missing Source",
			payload: model2.RecordTransaction{
				Amount:      750,
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Currency:    "NGN",
				Destination: newDestinationBalance.BalanceID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
		{
			name: "Missing Destination",
			payload: model2.RecordTransaction{
				Amount:      750,
				Reference:   "ref_001adcfgf",
				Description: "For fees",
				Currency:    "NGN",
				Source:      newSourceBalance.BalanceID,
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBytes, _ := request.ToJsonReq(&tt.payload)
			var response model.Transaction
			testRequest := TestRequest{
				Payload:  payloadBytes,
				Response: &response,
				Method:   "POST",
				Route:    "/transactions",
				Auth:     "",
				Router:   router,
			}

			resp, err := SetUpTestRequest(testRequest)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetUpTestRequest() error = %v, wantErr %v", err, tt.wantErr)
			}
			assert.Equal(t, tt.expectedCode, resp.Code)

			if !tt.wantErr && tt.expectedCode == http.StatusCreated {
				assert.Equal(t, tt.payload.Amount, response.Amount)
				assert.Equal(t, model.ApplyPrecision(&model.Transaction{
					Amount:    tt.payload.Amount,
					Precision: tt.payload.Precision,
				}).String(), response.PreciseAmount.String())
				assert.Equal(t, tt.payload.Reference, response.Reference)
				assert.Equal(t, tt.payload.Description, response.Description)
				assert.Equal(t, tt.payload.Currency, response.Currency)
				assert.Equal(t, tt.payload.Source, response.Source)
				assert.Equal(t, tt.payload.Destination, response.Destination)
				assert.Equal(t, "QUEUED", response.Status)
			}
		})
	}
}

func TestRecordTransactionWithExitingRef(t *testing.T) {
	router, b, _ := setupRouter()
	newLedger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	if err != nil {
		return
	}

	newSourceBalance, err := b.CreateBalance(context.Background(), model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Error(err)
		return
	}

	newDestinationBalance, err := b.CreateBalance(context.Background(), model.Balance{LedgerID: newLedger.LedgerID, Currency: "NGN"})
	if err != nil {
		t.Error(err)
		return
	}
	validPayload := model2.RecordTransaction{
		Amount:      10000,
		Reference:   gofakeit.UUID(),
		Description: "test",
		Currency:    "NGN",
		Destination: newDestinationBalance.BalanceID,
		Source:      newSourceBalance.BalanceID,
	}
	payloadBytes, _ := request.ToJsonReq(&validPayload)
	var response model.Transaction
	testRequest := TestRequest{
		Payload:  payloadBytes,
		Response: &response,
		Method:   "POST",
		Route:    "/transactions",
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}

	assert.Equal(t, http.StatusCreated, resp.Code)
	assert.Equal(t, response.Currency, validPayload.Currency)
	assert.Equal(t, response.Status, "QUEUED")
}

func TestInflightTransaction_Commit_API(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	ctx := context.Background()
	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	assert.NoError(t, err)

	sourceBalance, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	assert.NoError(t, err)
	destBalance, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "USD"})
	assert.NoError(t, err)

	// 1. Create Inflight Transaction
	inflightPayload := model2.RecordTransaction{
		Amount:         100.50,
		Precision:      100,
		Reference:      "inflight_commit_" + gofakeit.UUID(),
		Description:    "Inflight for API commit test",
		Currency:       "USD",
		Source:         sourceBalance.BalanceID,
		Destination:    destBalance.BalanceID,
		AllowOverDraft: true,
		Inflight:       true,
		SkipQueue:      true, // To process it synchronously as inflight
	}
	payloadBytes, _ := request.ToJsonReq(&inflightPayload)
	var inflightTxResponse model.Transaction

	testReqInflight := TestRequest{
		Payload:  payloadBytes,
		Response: &inflightTxResponse,
		Method:   "POST",
		Route:    "/transactions",
		Auth:     "",
		Router:   router,
	}

	respInflight, errInflight := SetUpTestRequest(testReqInflight)
	assert.NoError(t, errInflight)
	assert.Equal(t, http.StatusCreated, respInflight.Code)
	assert.Equal(t, "INFLIGHT", inflightTxResponse.Status)
	assert.True(t, inflightTxResponse.Inflight)

	// 2. Verify Initial Balances (Inflight)
	sbAfterInflight, _ := b.GetBalanceByID(ctx, sourceBalance.BalanceID, nil, false)
	dbAfterInflight, _ := b.GetBalanceByID(ctx, destBalance.BalanceID, nil, false)

	expectedInflightDebit := model.ApplyPrecision(&model.Transaction{Amount: inflightPayload.Amount, Precision: inflightPayload.Precision})
	expectedInflightCredit := model.ApplyPrecision(&model.Transaction{Amount: inflightPayload.Amount, Precision: inflightPayload.Precision})

	assert.Equal(t, int64(0), sbAfterInflight.Balance.Int64(), "Source balance should be 0 before commit")
	assert.Equal(t, expectedInflightDebit.Neg(expectedInflightDebit).String(), sbAfterInflight.InflightBalance.String(), "Source inflight balance incorrect")
	assert.Equal(t, int64(0), dbAfterInflight.Balance.Int64(), "Destination balance should be 0 before commit")
	assert.Equal(t, expectedInflightCredit.String(), dbAfterInflight.InflightBalance.String(), "Destination inflight balance incorrect")

	// 3. Commit Transaction
	commitPayload := model2.InflightUpdate{
		Status:        "commit",
		PreciseAmount: inflightTxResponse.PreciseAmount,
	}
	commitPayloadBytes, _ := request.ToJsonReq(&commitPayload)
	var commitTxResponse model.Transaction

	testReqCommit := TestRequest{
		Payload:  commitPayloadBytes,
		Response: &commitTxResponse,
		Method:   "PUT",
		Route:    "/transactions/inflight/" + inflightTxResponse.TransactionID,
		Auth:     "",
		Router:   router,
	}

	respCommit, errCommit := SetUpTestRequest(testReqCommit)
	assert.NoError(t, errCommit)
	assert.Equal(t, http.StatusOK, respCommit.Code)
	assert.Equal(t, "APPLIED", commitTxResponse.Status)
	assert.Equal(t, inflightTxResponse.PreciseAmount.String(), commitTxResponse.PreciseAmount.String())

	// 4. Verify Final Balances (Committed)
	sbAfterCommit, _ := b.GetBalanceByID(ctx, sourceBalance.BalanceID, nil, false)
	dbAfterCommit, _ := b.GetBalanceByID(ctx, destBalance.BalanceID, nil, false)
	assert.Equal(t, expectedInflightDebit.String(), sbAfterCommit.Balance.String(), "Source balance incorrect after commit")
	assert.Equal(t, int64(0), sbAfterCommit.InflightBalance.Int64(), "Source inflight balance should be 0 after commit")
	assert.Equal(t, expectedInflightCredit.String(), dbAfterCommit.Balance.String(), "Destination balance incorrect after commit")
	assert.Equal(t, int64(0), dbAfterCommit.InflightBalance.Int64(), "Destination inflight balance should be 0 after commit")
}

func TestInflightTransaction_Void_API(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	ctx := context.Background()
	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	assert.NoError(t, err)

	sourceBalance, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "EUR"})
	assert.NoError(t, err)
	destBalance, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "EUR"})
	assert.NoError(t, err)

	// 1. Create Inflight Transaction
	inflightPayload := model2.RecordTransaction{
		Amount:         250.75,
		Precision:      100,
		Reference:      "inflight_void_" + gofakeit.UUID(),
		Description:    "Inflight for API void test",
		Currency:       "EUR",
		Source:         sourceBalance.BalanceID,
		Destination:    destBalance.BalanceID,
		AllowOverDraft: true,
		Inflight:       true,
		SkipQueue:      true, // To process it synchronously as inflight
	}
	payloadBytes, _ := request.ToJsonReq(&inflightPayload)
	var inflightTxResponse model.Transaction

	testReqInflight := TestRequest{
		Payload:  payloadBytes,
		Response: &inflightTxResponse,
		Method:   "POST",
		Route:    "/transactions",
		Auth:     "",
		Router:   router,
	}

	respInflight, errInflight := SetUpTestRequest(testReqInflight)
	assert.NoError(t, errInflight)
	assert.Equal(t, http.StatusCreated, respInflight.Code)
	assert.Equal(t, "INFLIGHT", inflightTxResponse.Status)
	assert.True(t, inflightTxResponse.Inflight)

	// 2. Verify Initial Balances (Inflight)
	sbAfterInflight, _ := b.GetBalanceByID(ctx, sourceBalance.BalanceID, nil, false)
	dbAfterInflight, _ := b.GetBalanceByID(ctx, destBalance.BalanceID, nil, false)

	expectedInflightDebit := model.ApplyPrecision(&model.Transaction{Amount: inflightPayload.Amount, Precision: inflightPayload.Precision})
	expectedInflightCredit := model.ApplyPrecision(&model.Transaction{Amount: inflightPayload.Amount, Precision: inflightPayload.Precision})

	assert.Equal(t, int64(0), sbAfterInflight.Balance.Int64(), "Source balance should be 0 before void")
	assert.Equal(t, expectedInflightDebit.Neg(expectedInflightDebit).String(), sbAfterInflight.InflightBalance.String(), "Source inflight balance incorrect")
	assert.Equal(t, int64(0), dbAfterInflight.Balance.Int64(), "Destination balance should be 0 before void")
	assert.Equal(t, expectedInflightCredit.String(), dbAfterInflight.InflightBalance.String(), "Destination inflight balance incorrect")

	// 3. Void Transaction
	voidPayload := model2.InflightUpdate{
		Status: "void",
		// Amount is not strictly needed for void, but API might expect it or use precise_amount from original txn
	}
	voidPayloadBytes, _ := request.ToJsonReq(&voidPayload)
	var voidTxResponse model.Transaction

	testReqVoid := TestRequest{
		Payload:  voidPayloadBytes,
		Response: &voidTxResponse,
		Method:   "PUT",
		Route:    "/transactions/inflight/" + inflightTxResponse.TransactionID,
		Auth:     "",
		Router:   router,
	}

	respVoid, errVoid := SetUpTestRequest(testReqVoid)
	assert.NoError(t, errVoid)
	assert.Equal(t, http.StatusOK, respVoid.Code)
	assert.Equal(t, "VOID", voidTxResponse.Status)
	// The voided transaction amount should reflect the remaining inflight amount that was voided
	assert.Equal(t, inflightTxResponse.PreciseAmount.String(), voidTxResponse.PreciseAmount.String())

	// 4. Verify Final Balances (Voided)
	sbAfterVoid, _ := b.GetBalanceByID(ctx, sourceBalance.BalanceID, nil, false)
	dbAfterVoid, _ := b.GetBalanceByID(ctx, destBalance.BalanceID, nil, false)
	assert.Equal(t, int64(0), sbAfterVoid.Balance.Int64(), "Source balance should be 0 after void")
	assert.Equal(t, int64(0), sbAfterVoid.InflightBalance.Int64(), "Source inflight balance should be 0 after void")
	assert.Equal(t, int64(0), dbAfterVoid.Balance.Int64(), "Destination balance should be 0 after void")
	assert.Equal(t, int64(0), dbAfterVoid.InflightBalance.Int64(), "Destination inflight balance should be 0 after void")
}

func TestInflightTransaction_Commit_WithAmount_API(t *testing.T) {
	router, b, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	ctx := context.Background()
	ledger, err := b.CreateLedger(model.Ledger{Name: gofakeit.Name()})
	assert.NoError(t, err)

	sourceBalance, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "CAD"})
	assert.NoError(t, err)
	destBalance, err := b.CreateBalance(ctx, model.Balance{LedgerID: ledger.LedgerID, Currency: "CAD"})
	assert.NoError(t, err)

	// 1. Create Inflight Transaction
	originalAmount := 300.50 // Changed to include decimals
	inflightPayload := model2.RecordTransaction{
		Amount:         originalAmount,
		Precision:      100,
		Reference:      "inflight_partial_commit_" + gofakeit.UUID(),
		Description:    "Inflight for API partial commit test",
		Currency:       "CAD",
		Source:         sourceBalance.BalanceID,
		Destination:    destBalance.BalanceID,
		AllowOverDraft: true,
		Inflight:       true,
		SkipQueue:      true, // To process it synchronously as inflight
	}
	payloadBytes, _ := request.ToJsonReq(&inflightPayload)
	var inflightTxResponse model.Transaction

	testReqInflight := TestRequest{
		Payload:  payloadBytes,
		Response: &inflightTxResponse,
		Method:   "POST",
		Route:    "/transactions",
		Auth:     "",
		Router:   router,
	}

	respInflight, errInflight := SetUpTestRequest(testReqInflight)
	assert.NoError(t, errInflight)
	assert.Equal(t, http.StatusCreated, respInflight.Code)
	assert.Equal(t, "INFLIGHT", inflightTxResponse.Status)
	assert.True(t, inflightTxResponse.Inflight)
	originalPreciseAmount := inflightTxResponse.PreciseAmount

	// 2. Verify Initial Balances (Inflight)
	sbAfterInflight, _ := b.GetBalanceByID(ctx, sourceBalance.BalanceID, nil, false)
	dbAfterInflight, _ := b.GetBalanceByID(ctx, destBalance.BalanceID, nil, false)

	expectedInitialInflightDebit := model.ApplyPrecision(&model.Transaction{Amount: inflightPayload.Amount, Precision: inflightPayload.Precision})
	expectedInitialInflightCredit := model.ApplyPrecision(&model.Transaction{Amount: inflightPayload.Amount, Precision: inflightPayload.Precision})

	assert.Equal(t, int64(0), sbAfterInflight.Balance.Int64(), "Source balance should be 0 before commit")
	assert.Equal(t, expectedInitialInflightDebit.Neg(expectedInitialInflightDebit).String(), sbAfterInflight.InflightBalance.String(), "Source inflight balance incorrect")
	assert.Equal(t, int64(0), dbAfterInflight.Balance.Int64(), "Destination balance should be 0 before commit")
	assert.Equal(t, expectedInitialInflightCredit.String(), dbAfterInflight.InflightBalance.String(), "Destination inflight balance incorrect")

	// 3. Partially Commit Transaction using Amount (float64)
	partialAmountFloat := 100.25 // Changed to include decimals
	commitPartialPayload := model2.InflightUpdate{
		Status: "commit",
		Amount: partialAmountFloat, // Using Amount (float64) instead of PreciseAmount
	}
	commitPartialPayloadBytes, _ := request.ToJsonReq(&commitPartialPayload)
	var commitPartialTxResponse model.Transaction

	testReqPartialCommit := TestRequest{
		Payload:  commitPartialPayloadBytes,
		Response: &commitPartialTxResponse,
		Method:   "PUT",
		Route:    "/transactions/inflight/" + inflightTxResponse.TransactionID,
		Auth:     "",
		Router:   router,
	}

	respPartialCommit, errPartialCommit := SetUpTestRequest(testReqPartialCommit)
	assert.NoError(t, errPartialCommit)
	assert.Equal(t, http.StatusOK, respPartialCommit.Code)
	assert.Equal(t, "APPLIED", commitPartialTxResponse.Status)

	// Assert that the response PreciseAmount is what we expect from the float Amount and precision
	expectedPartialPreciseAmount := model.ApplyPrecision(&model.Transaction{Amount: partialAmountFloat, Precision: inflightPayload.Precision})
	assert.Equal(t, expectedPartialPreciseAmount.String(), commitPartialTxResponse.PreciseAmount.String())

	// 4. Verify Balances after Partial Commit
	sbAfterPartialCommit, _ := b.GetBalanceByID(ctx, sourceBalance.BalanceID, nil, false)
	dbAfterPartialCommit, _ := b.GetBalanceByID(ctx, destBalance.BalanceID, nil, false)

	// Use expectedPartialPreciseAmount for balance assertions
	remainingInflightPreciseAmount := new(big.Int).Sub(originalPreciseAmount, expectedPartialPreciseAmount)

	assert.Equal(t, expectedPartialPreciseAmount.Neg(expectedPartialPreciseAmount).String(), sbAfterPartialCommit.Balance.String(), "Source balance incorrect after partial commit")
	assert.Equal(t, remainingInflightPreciseAmount.Neg(remainingInflightPreciseAmount).String(), sbAfterPartialCommit.InflightBalance.String(), "Source inflight balance incorrect after partial commit")
	assert.Equal(t, expectedPartialPreciseAmount.Neg(expectedPartialPreciseAmount).String(), dbAfterPartialCommit.Balance.String(), "Destination balance incorrect after partial commit")
	assert.Equal(t, remainingInflightPreciseAmount.Neg(remainingInflightPreciseAmount).String(), dbAfterPartialCommit.InflightBalance.String(), "Destination inflight balance incorrect after partial commit")

	// 5. Commit Remaining Transaction (by not specifying amount or precise_amount in payload)
	commitRemainingPayload := model2.InflightUpdate{
		Status: "commit",
		// Amount: 0.0, // Can be explicit 0.0 or absent due to omitempty
		// PreciseAmount is nil by default
	}
	commitRemainingPayloadBytes, _ := request.ToJsonReq(&commitRemainingPayload)
	var commitRemainingTxResponse model.Transaction

	testReqRemainingCommit := TestRequest{
		Payload:  commitRemainingPayloadBytes,
		Response: &commitRemainingTxResponse,
		Method:   "PUT",
		Route:    "/transactions/inflight/" + inflightTxResponse.TransactionID,
		Auth:     "",
		Router:   router,
	}

	respRemainingCommit, errRemainingCommit := SetUpTestRequest(testReqRemainingCommit)
	assert.NoError(t, errRemainingCommit)
	assert.Equal(t, http.StatusOK, respRemainingCommit.Code)
	assert.Equal(t, "APPLIED", commitRemainingTxResponse.Status)
	assert.Equal(t, remainingInflightPreciseAmount.String(), commitRemainingTxResponse.PreciseAmount.String(), "Remaining committed amount mismatch")

	// 6. Verify Final Balances (Fully Committed)
	sbAfterFullCommit, _ := b.GetBalanceByID(ctx, sourceBalance.BalanceID, nil, false)
	dbAfterFullCommit, _ := b.GetBalanceByID(ctx, destBalance.BalanceID, nil, false)

	assert.Equal(t, originalPreciseAmount.Neg(originalPreciseAmount).String(), sbAfterFullCommit.Balance.String(), "Source balance incorrect after full commit")
	assert.Equal(t, int64(0), sbAfterFullCommit.InflightBalance.Int64(), "Source inflight balance should be 0 after full commit")
	assert.Equal(t, originalPreciseAmount.Neg(originalPreciseAmount).String(), dbAfterFullCommit.Balance.String(), "Destination balance incorrect after full commit")
	assert.Equal(t, int64(0), dbAfterFullCommit.InflightBalance.Int64(), "Destination inflight balance should be 0 after full commit")
}
