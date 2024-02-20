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
	config.MockConfig(false, "", "")
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
	router, _, _ := setupRouter()
	validPayload := model2.CreateLedger{
		Name: gofakeit.Name(),
	}
	payloadBytes, _ := request.ToJsonReq(&validPayload)
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
	if err != nil {
		t.Error(err)
		return
	}
	assert.Equal(t, http.StatusCreated, resp.Code)
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
