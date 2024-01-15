package api

import (
	"fmt"
	"net/http"

	model2 "github.com/jerry-enebeli/blnk/api/model"

	"github.com/gin-gonic/gin"
)

func (a Api) RecordTransaction(c *gin.Context) {
	var newTransaction model2.RecordTransaction
	if err := c.ShouldBindJSON(&newTransaction); err != nil {
		return
	}

	err := newTransaction.ValidateRecordTransaction()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}
	resp, err := a.blnk.RecordTransaction(newTransaction.ToTransaction())

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) QueueTransaction(c *gin.Context) {
	var newTransaction model2.RecordTransaction
	if err := c.ShouldBindJSON(&newTransaction); err != nil {
		return
	}
	err := newTransaction.ValidateRecordTransaction()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}
	resp, err := a.blnk.QueueTransaction(newTransaction.ToTransaction())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) RefundTransaction(c *gin.Context) {
	id, passed := c.Params.Get("id")
	fmt.Println(id, passed)
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}
	resp, err := a.blnk.RefundTransaction(id)

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) GetTransaction(c *gin.Context) {
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

func (a Api) GroupTransactionsByCurrency(c *gin.Context) {

	resp, err := a.blnk.GroupTransactionsByCurrency()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}
