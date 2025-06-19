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
	"fmt"
	"net/http"

	"github.com/typesense/typesense-go/typesense/api"
	"go.opentelemetry.io/contrib/instrumentation/github.com/gin-gonic/gin/otelgin"

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/api/middleware"
	"github.com/jerry-enebeli/blnk/config"
	"go.elastic.co/apm/module/apmgin/v2"
)

// Api represents the API structure for handling requests.
type Api struct {
	blnk   *blnk.Blnk
	router *gin.Engine
	auth   *middleware.AuthMiddleware
}

// Router sets up the routes for the API and returns the router instance.
//
// Responses:
// - 200 OK: When the router is successfully set up.
func (a Api) Router() *gin.Engine {
	router := a.router

	// Apply auth middleware to all routes
	router.Use(a.auth.Authenticate())
	router.Use(apmgin.Middleware(router))

	// Ledger routes
	router.POST("/ledgers", a.CreateLedger)
	router.GET("/ledgers/:id", a.GetLedger)
	router.GET("/ledgers", a.GetAllLedgers)

	// Balance routes
	router.POST("/balances", a.CreateBalance)
	router.GET("/balances", a.GetBalances)
	router.GET("/balances/:id", a.GetBalance)
	router.GET("/balances/indicator/:indicator/currency/:currency", a.GetBalanceByIndicator)
	router.GET("/balances/:id/at", a.GetBalanceAtTime)
	router.POST("/balances-snapshots", a.TakeBalanceSnapshots)

	// Balance Monitor routes
	router.POST("/balance-monitors", a.CreateBalanceMonitor)
	router.GET("/balance-monitors/:id", a.GetBalanceMonitor)
	router.GET("/balance-monitors", a.GetAllBalanceMonitors)
	router.GET("/balance-monitors/balances/:balance_id", a.GetBalanceMonitorsByBalanceID)
	router.PUT("/balance-monitors/:id", a.UpdateBalanceMonitor)

	// Transaction routes
	router.POST("/transactions", a.QueueTransaction)
	router.POST("/transactions/bulk", a.CreateBulkTransactions)
	router.POST("/refund-transaction/:id", a.RefundTransaction)
	router.GET("/transactions/:id", a.GetTransaction)
	router.PUT("/transactions/inflight/:txID", a.UpdateInflightStatus)

	// Identity routes
	router.POST("/identities", a.CreateIdentity)
	router.GET("/identities/:id", a.GetIdentity)
	router.PUT("/identities/:id", a.UpdateIdentity)
	router.GET("/identities", a.GetAllIdentities)
	router.GET("/identities/:id/tokenized-fields", a.GetTokenizedFields)
	router.POST("/identities/:id/tokenize/:field", a.TokenizeIdentityField)
	router.GET("/identities/:id/detokenize/:field", a.DetokenizeIdentityField)
	router.POST("/identities/:id/tokenize", a.TokenizeIdentity)
	router.POST("/identities/:id/detokenize", a.DetokenizeIdentity)

	// Account routes
	router.POST("/accounts", a.CreateAccount)
	router.GET("/accounts/:id", a.GetAccount)
	router.GET("/accounts", a.GetAllAccounts)

	// Mocked Account route
	router.GET("/mocked-account", a.generateMockAccount)

	// Backup routes
	router.GET("/backup", a.BackupDB)
	router.GET("/backup-s3", a.BackupDBS3)

	// Search routes
	router.POST("/search/:collection", a.Search)
	router.POST("/multi-search", a.MultiSearch)

	// Reconciliation routes
	router.POST("/reconciliation/upload", a.UploadExternalData)
	router.POST("/reconciliation/matching-rules", a.CreateMatchingRule)
	router.POST("/reconciliation/start", a.StartReconciliation)
	router.POST("/reconciliation/start-instant", a.InstantReconciliation)
	router.GET("/reconciliation/:id", a.GetReconciliation)

	// Metadata routes
	router.POST("/:entity-id/metadata", a.UpdateMetadata)

	// Hook management routes
	router.POST("/hooks", a.RegisterHook)
	router.PUT("/hooks/:id", a.UpdateHook)
	router.GET("/hooks/:id", a.GetHook)
	router.GET("/hooks", a.ListHooks)
	router.DELETE("/hooks/:id", a.DeleteHook)

	// API Key routes
	router.POST("/api-keys", a.CreateAPIKey)
	router.GET("/api-keys", a.ListAPIKeys)
	router.DELETE("/api-keys/:id", a.RevokeAPIKey)

	return a.router
}

// NewAPI creates a new Api instance with the provided Blnk service and sets up the router.
//
// Parameters:
// - b: The Blnk service used to interact with business logic.
//
// Returns:
// - *Api: A new instance of the Api with the configured router.
func NewAPI(b *blnk.Blnk) *Api {
	gin.SetMode(gin.ReleaseMode)
	conf, err := config.Fetch()
	if err != nil {
		return nil
	}
	r := gin.Default()
	auth := middleware.NewAuthMiddleware(b)
	r.Use(middleware.RateLimitMiddleware(conf))
	r.Use(otelgin.Middleware("BLNK"))

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, "server running...")
	})

	r.POST("/webhook", func(c *gin.Context) {
		var payload map[string]interface{}
		err := c.Bind(&payload)
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println(payload)
		c.JSON(200, "webhook received")
	})

	return &Api{blnk: b, router: r, auth: auth}
}

// Search performs a search query on a specified collection.
// It binds the incoming JSON request to a SearchCollectionParams object,
// executes the search query, and responds with the search results.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or performing the search.
// - 201 Created: If the search query is successfully executed and results are returned.
func (a Api) Search(c *gin.Context) {
	collection, passed := c.Params.Get("collection")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "collection is required. pass id in the route /:collection"})
		return
	}

	var query api.SearchCollectionParams
	err := c.BindJSON(&query)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := a.blnk.Search(collection, &query)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

// MultiSearch performs a multi-search query.
// It binds the incoming JSON request to a MultiSearchParameter object,
// executes the multi-search query, and responds with the search results.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in binding JSON or performing the search.
// - 200 OK: If the multi-search query is successfully executed and results are returned.
func (a Api) MultiSearch(c *gin.Context) {
	var searchRequests api.MultiSearchSearchesParameter
	if err := c.BindJSON(&searchRequests); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	resp, err := a.blnk.MultiSearch(&searchRequests)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}
