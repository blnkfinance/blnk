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
	"net/http"

	model2 "github.com/jerry-enebeli/blnk/api/model"

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk/model"
)

func (a Api) CreateBalance(c *gin.Context) {
	var newBalance model2.CreateBalance
	if err := c.ShouldBindJSON(&newBalance); err != nil {
		return
	}

	err := newBalance.ValidateCreateBalance()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	resp, err := a.blnk.CreateBalance(c.Request.Context(), newBalance.ToBalance())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) GetBalance(c *gin.Context) {
	id, passed := c.Params.Get("id")

	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	// Extracting 'include' parameter from the query
	includes := c.QueryArray("include")

	resp, err := a.blnk.GetBalanceByID(c.Request.Context(), id, includes)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) CreateBalanceMonitor(c *gin.Context) {
	var newMonitor model2.CreateBalanceMonitor
	if err := c.ShouldBindJSON(&newMonitor); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	err := newMonitor.ValidateCreateBalanceMonitor()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"errors": err.Error()})
		return
	}

	resp, err := a.blnk.CreateMonitor(c.Request.Context(), newMonitor.ToBalanceMonitor())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, resp)
}

func (a Api) GetBalanceMonitor(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	resp, err := a.blnk.GetMonitorByID(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, resp)
}

func (a Api) GetAllBalanceMonitors(c *gin.Context) {
	monitors, err := a.blnk.GetAllMonitors(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, monitors)
}

func (a Api) GetBalanceMonitorsByBalanceID(c *gin.Context) {
	balanceID, passed := c.Params.Get("balance_id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "balance_id is required. pass balance_id in the route /:balance_id"})
		return
	}

	monitors, err := a.blnk.GetMonitorByID(c.Request.Context(), balanceID)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, monitors)
}

func (a Api) UpdateBalanceMonitor(c *gin.Context) {
	var monitor model.BalanceMonitor
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	if err := c.ShouldBindJSON(&monitor); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	monitor.MonitorID = id
	err := a.blnk.UpdateMonitor(c.Request.Context(), &monitor)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "BalanceMonitor updated successfully"})
}

func (a Api) DeleteBalanceMonitor(c *gin.Context) {
	id, passed := c.Params.Get("id")
	if !passed {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id is required. pass id in the route /:id"})
		return
	}

	err := a.blnk.DeleteMonitor(c.Request.Context(), id)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "BalanceMonitor deleted successfully"})
}
