package api

import (
	"errors"
	"net/http"

	"github.com/sirupsen/logrus"

	model2 "github.com/jerry-enebeli/blnk/api/model"
	"github.com/jerry-enebeli/blnk/model"

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
	resp, err := a.blnk.RecordTransaction(c.Request.Context(), newTransaction.ToTransaction())

	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) QueueTransaction(c *gin.Context) {

	var newTransaction model2.RecordTransaction
	if err := c.ShouldBindJSON(&newTransaction); err != nil {
		logrus.Error(err)
		return
	}
	err := newTransaction.ValidateRecordTransaction()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	resp, err := a.blnk.QueueTransaction(c.Request.Context(), newTransaction.ToTransaction())
	if err != nil {
		logrus.Error(err)
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)

}

func (a Api) RefundTransaction(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}
	transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, 0, 1, false, a.blnk.GetRefundableTransactionsByParentID, a.blnk.RefundWorker)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(transaction) == 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "no transaction to refund"})
		return
	}
	resp := transaction[0]
	c.JSON(http.StatusCreated, resp)
}

func (a Api) GetTransaction(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetTransaction(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) UpdateInflightStatus(c *gin.Context) {
	var resp *model.Transaction
	id, passed := c.Params.Get("txID")
	var req model2.InflightUpdate
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}
	err := c.BindJSON(&req)
	if err != nil {
		return
	}

	status := req.Status
	if status == "commit" {
		transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, req.Amount, 1, false, a.blnk.GetInflightTransactionsByParentID, a.blnk.CommitWorker)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if len(transaction) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "no transaction to commit"})
			return
		}
		resp = transaction[0]
	} else if status == "void" {
		transaction, err := a.blnk.ProcessTransactionInBatches(c.Request.Context(), id, req.Amount, 1, false, a.blnk.GetInflightTransactionsByParentID, a.blnk.VoidWorker)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		if len(transaction) == 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "no transaction to void"})
			return
		}
		resp = transaction[0]
	} else {
		c.JSON(http.StatusBadRequest, gin.H{"error": errors.New("status not supported. use either commit or void")})
		return

	}

	c.JSON(http.StatusOK, resp)
}
