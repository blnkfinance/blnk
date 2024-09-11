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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jerry-enebeli/blnk/internal/request"

	"github.com/brianvoe/gofakeit/v6"
	model2 "github.com/jerry-enebeli/blnk/api/model"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/model"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/database"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
)

type TestRequest struct {
	Payload  io.Reader
	Router   *gin.Engine
	Response interface{}
	Method   string
	Route    string
	Auth     string
	Header   map[string]string
}

func SetUpTestRequest(s TestRequest) (*httptest.ResponseRecorder, error) {
	req := httptest.NewRequest(s.Method, s.Route, s.Payload)
	for key, value := range s.Header {
		req.Header.Set(key, value)

	}
	req.Header.Set("Content-Type", "application/json")
	resp := httptest.NewRecorder()
	s.Router.ServeHTTP(resp, req)

	err := json.NewDecoder(resp.Body).Decode(&s.Response)
	if err != nil {
		return nil, err
	}
	return resp, nil
}

func setupRouter() (*gin.Engine, *blnk.Blnk, error) {
	config.MockConfig(&config.Configuration{
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	})
	cnf, err := config.Fetch()
	if err != nil {
		return nil, nil, err
	}
	db, err := database.NewDataSource(cnf)
	if err != nil {
		return nil, nil, err
	}
	newBlnk, err := blnk.NewBlnk(db)
	if err != nil {
		return nil, nil, err
	}
	router := NewAPI(newBlnk).Router()

	return router, newBlnk, nil
}

func TestCreateLedger(t *testing.T) {
	router, blnk, err := setupRouter()
	if err != nil {
		t.Fatalf("Failed to setup router: %v", err)
	}

	tests := []struct {
		name         string
		payload      model2.CreateLedger
		expectedCode int
		wantErr      bool
	}{
		{
			name: "Valid Ledger",
			payload: model2.CreateLedger{
				Name: gofakeit.Name(),
			},
			expectedCode: http.StatusCreated,
			wantErr:      false,
		},
		{
			name: "Empty Name",
			payload: model2.CreateLedger{
				Name: "",
			},
			expectedCode: http.StatusBadRequest,
			wantErr:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payloadBytes, _ := request.ToJsonReq(&tt.payload)
			var response model.Ledger
			testRequest := TestRequest{
				Payload:  payloadBytes,
				Response: &response,
				Method:   "POST",
				Route:    "/ledgers",
				Auth:     "",
				Router:   router,
			}

			resp, err := SetUpTestRequest(testRequest)
			if (err != nil) != tt.wantErr {
				t.Errorf("SetUpTestRequest() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			assert.Equal(t, tt.expectedCode, resp.Code)

			if tt.expectedCode == http.StatusCreated {
				// Verify that the ledger is actually created in the database
				ledgerFromDB, err := blnk.GetLedgerByID(response.LedgerID)
				if err != nil {
					t.Errorf("Failed to retrieve ledger by ID: %v", err)
					return
				}
				assert.Equal(t, response.LedgerID, ledgerFromDB.LedgerID)
				assert.Equal(t, tt.payload.Name, ledgerFromDB.Name)
			}
		})
	}
}

func TestGetLedger(t *testing.T) {
	router, b, _ := setupRouter()
	validPayload := model.Ledger{Name: gofakeit.Name()}
	newLedger, err := b.CreateLedger(validPayload)
	if err != nil {
		return
	}
	fmt.Println(newLedger)
	var response model.Ledger
	testRequest := TestRequest{
		Payload:  nil,
		Response: &response,
		Method:   "GET",
		Route:    fmt.Sprintf("/ledgers/%s", newLedger.LedgerID),
		Auth:     "",
		Router:   router,
	}
	resp, err := SetUpTestRequest(testRequest)
	if err != nil {
		t.Error(err)
		return
	}
	fmt.Println(response)
	assert.Equal(t, http.StatusOK, resp.Code)
	assert.Equal(t, response.LedgerID, newLedger.LedgerID)
	assert.Equal(t, response.Name, newLedger.Name)
}
