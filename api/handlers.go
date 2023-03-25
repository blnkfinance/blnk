package api

import (
	"net/http"
	"strconv"
	"sync"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/pkg"

	"github.com/gin-gonic/gin"
)

type api struct {
	blnk   *pkg.Blnk
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
	router.GET("/transaction/group/currency", a.GroupTransactionsByCurrency)
	return a.router
}

func NewAPI(blnk *pkg.Blnk) *api {
	r := gin.Default()
	gin.SetMode(gin.DebugMode)
	return &api{blnk: blnk, router: r}
}

func (a api) CreateLedger(c *gin.Context) {
	var ledger blnk.Ledger
	if err := c.ShouldBindJSON(&ledger); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := a.blnk.CreateLedger(ledger)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a api) CreateBalance(c *gin.Context) {
	var balance blnk.Balance
	if err := c.ShouldBindJSON(&balance); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	resp, err := a.blnk.CreateBalance(balance)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a api) RecordTransaction(c *gin.Context) {
	var transaction blnk.Transaction
	if err := c.ShouldBindJSON(&transaction); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	mutex := sync.Mutex{}

	mutex.Lock()
	resp, err := a.blnk.RecordTransaction(transaction)
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

	idInt, _ := strconv.Atoi(id)

	resp, err := a.blnk.GetLedgerByID(int64(idInt))
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

	idInt, _ := strconv.Atoi(id)

	resp, err := a.blnk.GetBalance(int64(idInt))
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

	resp, err := a.blnk.GetTransaction(id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a api) GroupTransactionsByCurrency(c *gin.Context) {

	resp, err := a.blnk.GroupTransactionsByCurrency()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}
