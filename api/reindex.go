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
	"net/http"
	"sync"

	"github.com/blnkfinance/blnk/internal/search"
	"github.com/gin-gonic/gin"
)

// ReindexRequest represents the request body for starting a reindex operation.
type ReindexRequest struct {
	BatchSize int `json:"batch_size"`
}

type reindexManager struct {
	service *search.ReindexService
	mu      sync.RWMutex
}

var globalReindexManager = &reindexManager{}

// StartReindex triggers a full reindex of all data from the database to Typesense.
// The reindex runs asynchronously to avoid HTTP timeouts.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 202 Accepted: Reindex started successfully, returns initial progress.
// - 409 Conflict: If a reindex is already in progress.
func (a Api) StartReindex(c *gin.Context) {
	var req ReindexRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		req.BatchSize = 0
	}

	if req.BatchSize <= 0 {
		req.BatchSize = 1000
	}

	globalReindexManager.mu.Lock()
	if globalReindexManager.service != nil {
		progress := globalReindexManager.service.GetProgress()
		if progress.Status == "in_progress" {
			globalReindexManager.mu.Unlock()
			c.JSON(http.StatusConflict, gin.H{
				"error":    "A reindex operation is already in progress",
				"progress": progress,
			})
			return
		}
	}

	config := search.ReindexConfig{
		BatchSize: req.BatchSize,
	}

	reindexService := search.NewReindexService(
		a.blnk.GetSearchClient(),
		a.blnk.GetDataSource(),
		config,
	)
	globalReindexManager.service = reindexService
	globalReindexManager.mu.Unlock()

	go func() {
		_, _ = reindexService.StartReindex(context.Background())
	}()

	c.JSON(http.StatusAccepted, gin.H{
		"message":  "Reindex operation started",
		"progress": reindexService.GetProgress(),
	})
}

// GetReindexProgress returns the current progress of the reindex operation.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 200 OK: Returns current progress.
// - 404 Not Found: If no reindex operation has been started.
func (a Api) GetReindexProgress(c *gin.Context) {
	globalReindexManager.mu.RLock()
	defer globalReindexManager.mu.RUnlock()

	if globalReindexManager.service == nil {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "No reindex operation has been started",
		})
		return
	}

	progress := globalReindexManager.service.GetProgress()
	c.JSON(http.StatusOK, progress)
}
