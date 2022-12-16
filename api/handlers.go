package api

import (
	"net/http"
	"sync"

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/saifu"
)

type api struct {
	ledger saifu.Service
	router *gin.Engine
}

func (a api) Router() *gin.Engine {
	router := a.router
	router.POST("/ledger", a.CreateLedger)
	router.POST("/balance", a.CreateBalance)
	router.POST("/transaction", a.RecordTransaction)
	router.GET("/ledger/:id", a.GetLedger)
	router.GET("/balance/:id", a.GetBalance)
	router.GET("/transaction/:id", a.GetTransaction)

	return a.router
}

func NewAPI(ledger saifu.Service) *api {
	r := gin.Default()
	gin.SetMode(gin.ReleaseMode)
	return &api{ledger: ledger, router: r}
}

func (a api) CreateLedger(c *gin.Context) {
	var ledger saifu.Ledger
	if err := c.ShouldBindJSON(&ledger); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := a.ledger.CreateLedger(ledger)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a api) CreateBalance(c *gin.Context) {
	var balance saifu.Balance
	if err := c.ShouldBindJSON(&balance); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := a.ledger.CreateBalance(balance)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a api) RecordTransaction(c *gin.Context) {
	var transaction saifu.Transaction
	if err := c.ShouldBindJSON(&transaction); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	mutex := sync.Mutex{}

	mutex.Lock()
	resp, err := a.ledger.RecordTransaction(transaction)
	mutex.Unlock()

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a api) GetLedger(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.ledger.GetLedger(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a api) GetBalance(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.ledger.GetBalance(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a api) GetTransaction(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.ledger.GetTransaction(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}
