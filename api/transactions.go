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

func (a Api) UpdateInflightStatus(c *gin.Context) {
	var resp *model.Transaction
	id, passed := c.Params.Get("id")
	var req map[string]float64
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	status, passed := c.Params.Get("status")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "status is required. pass id in the route /:status"})
		return
	}

	c.BindJSON(&req)

	amount := req["amount"]

	if status == "commit" {
		transaction, err := a.blnk.CommitInflightTransaction(c.Request.Context(), id, amount)
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		resp = transaction
	} else if status == "void" {
		transaction, err := a.blnk.VoidInflightTransaction(c.Request.Context(), id)

		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		resp = transaction
	} else {

		c.JSON(http.StatusBadRequest, gin.H{"error": errors.New("status not supported. use either commit or void")})
		return

	}

	c.JSON(http.StatusOK, resp)
}
