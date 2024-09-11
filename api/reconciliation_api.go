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

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk/model"
	"github.com/sirupsen/logrus"
)

// UploadExternalData handles the upload of external transaction data.
// It receives the file and source details from the request, processes the upload,
// and returns an upload ID along with the record count and source information.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the file upload fails.
// - 500 Internal Server Error: If there is an error processing the upload.
// - 200 OK: If the upload is successful.
func (a Api) UploadExternalData(c *gin.Context) {
	source := c.PostForm("source")
	file, header, err := c.Request.FormFile("file")
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "File upload failed"})
		return
	}
	defer file.Close()

	fileName := header.Filename

	uploadID, total, err := a.blnk.UploadExternalData(c.Request.Context(), source, file, fileName)
	if err != nil {
		logrus.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to process upload"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"upload_id": uploadID, "record_count": total, "source": source})
}

// StartReconciliation initiates a new reconciliation process based on the provided parameters.
// It starts the reconciliation process and returns the reconciliation ID.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the request body is invalid or required fields are missing.
// - 500 Internal Server Error: If there is an error starting the reconciliation process.
// - 200 OK: If the reconciliation process is successfully started.
func (a Api) StartReconciliation(c *gin.Context) {
	var req struct {
		UploadID         string   `json:"upload_id" binding:"required"`
		Strategy         string   `json:"strategy" binding:"required"`
		GroupingCriteria string   `json:"grouping_criteria"`
		DryRun           bool     `json:"dry_run"`
		MatchingRuleIDs  []string `json:"matching_rule_ids" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	reconciliationID, err := a.blnk.StartReconciliation(c.Request.Context(), req.UploadID, req.Strategy, req.GroupingCriteria, req.MatchingRuleIDs, req.DryRun)
	if err != nil {
		logrus.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to start reconciliation"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"reconciliation_id": reconciliationID})
}

// CreateMatchingRule creates a new matching rule based on the provided rule details.
// It returns the created matching rule.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the request body is invalid.
// - 500 Internal Server Error: If there is an error creating the matching rule.
// - 201 Created: If the matching rule is successfully created.
func (a Api) CreateMatchingRule(c *gin.Context) {
	var rule model.MatchingRule
	if err := c.ShouldBindJSON(&rule); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	createdRule, err := a.blnk.CreateMatchingRule(c.Request.Context(), rule)
	if err != nil {
		logrus.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to create matching rule"})
		return
	}

	c.JSON(http.StatusCreated, createdRule)
}

// UpdateMatchingRule updates an existing matching rule identified by its ID.
// It returns the updated matching rule.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the Matching Rule ID is missing or the request body is invalid.
// - 500 Internal Server Error: If there is an error updating the matching rule.
// - 200 OK: If the matching rule is successfully updated.
func (a Api) UpdateMatchingRule(c *gin.Context) {
	ruleID := c.Param("id")
	if ruleID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Matching Rule ID is required"})
		return
	}

	var rule model.MatchingRule
	if err := c.ShouldBindJSON(&rule); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	rule.RuleID = ruleID
	updatedRule, err := a.blnk.UpdateMatchingRule(c.Request.Context(), rule)
	if err != nil {
		logrus.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update matching rule"})
		return
	}

	c.JSON(http.StatusOK, updatedRule)
}

// DeleteMatchingRule deletes a matching rule identified by its ID.
// It confirms the deletion with a success message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the Matching Rule ID is missing.
// - 500 Internal Server Error: If there is an error deleting the matching rule.
// - 200 OK: If the matching rule is successfully deleted.
func (a Api) DeleteMatchingRule(c *gin.Context) {
	ruleID := c.Param("id")
	if ruleID == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Matching Rule ID is required"})
		return
	}

	err := a.blnk.DeleteMatchingRule(c.Request.Context(), ruleID)
	if err != nil {
		logrus.Error(err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to delete matching rule"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Matching rule deleted successfully"})
}
