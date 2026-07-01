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
	"encoding/json"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	model2 "github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
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
	// Bound the request body so an oversized upload can't exhaust disk/memory.
	if cfg, cfgErr := config.Fetch(); cfgErr == nil && cfg.Server.MaxUploadSizeMB > 0 {
		maxBytes := cfg.Server.MaxUploadSizeMB * 1024 * 1024
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
	}

	source := c.PostForm("source")
	file, header, err := c.Request.FormFile("file")
	if err == nil {
		// Multipart file upload path (existing behaviour). `file` wins when
		// both `file` and `url` are present, preserving backward compatibility.
		defer func() { _ = file.Close() }()
		fileName := header.Filename

		uploadID, total, err := a.blnk.UploadExternalData(c.Request.Context(), source, file, fileName)
		if err != nil {
			logrus.Error(err)
			respondCode(c, apierror.ErrReconUploadProcessingFailed, "Failed to process upload", nil)
			return
		}

		c.JSON(http.StatusOK, gin.H{"upload_id": uploadID, "record_count": total, "source": source})
		return
	}

	// http.MaxBytesReader surfaces oversized multipart bodies as a FormFile error.
	if strings.Contains(err.Error(), "request body too large") {
		respondCode(c, apierror.ErrGenPayloadTooLarge, "upload exceeds the maximum allowed size", nil)
		return
	}

	// No `file` part. Fall back to the URL download path if a `url` was supplied
	// via a form field or a JSON body. If neither is present, return the legacy
	// 400 "File upload failed" so existing clients see unchanged behaviour.
	rawURL, jsonSource := resolveUploadURL(c)
	if jsonSource != "" && source == "" {
		source = jsonSource
	}
	if rawURL == "" {
		respondCode(c, apierror.ErrReconUploadFailed, "File upload failed", nil)
		return
	}

	uploadID, total, ok := a.downloadAndUpload(c, source, rawURL)
	if !ok {
		return // downloadAndUpload already wrote the error response.
	}

	c.JSON(http.StatusOK, gin.H{"upload_id": uploadID, "record_count": total, "source": source})
}

// resolveUploadURL extracts the `url` for a URL-based upload. For JSON bodies
// ({"url": "...", "source": "..."}) it returns both fields; for multipart form
// data it reads the `url` form field (the source is read separately by the
// caller via PostForm).
func resolveUploadURL(c *gin.Context) (rawURL, source string) {
	ctype := c.GetHeader("Content-Type")
	if strings.HasPrefix(strings.ToLower(ctype), "application/json") {
		var body struct {
			URL    string `json:"url"`
			Source string `json:"source"`
		}
		if err := json.NewDecoder(c.Request.Body).Decode(&body); err == nil {
			return body.URL, body.Source
		}
		return "", ""
	}
	return c.PostForm("url"), ""
}

// downloadAndUpload fetches the remote body at rawURL and pipes it through the
// unchanged UploadExternalData service. It enforces SSRF protection
// (http(s)-only, deny-by-default host whitelist, redirect re-validation), a
// configurable GET timeout, and the existing MaxUploadSizeMB body cap via
// io.LimitReader. On failure it writes the error response and returns ok=false.
func (a Api) downloadAndUpload(c *gin.Context, source, rawURL string) (uploadID string, total int, ok bool) {
	cfg, _ := config.Fetch()

	u, err := url.Parse(rawURL)
	if err != nil || u == nil {
		respondCode(c, apierror.ErrReconUploadURLInvalid, "invalid upload URL", nil)
		return "", 0, false
	}

	// Scheme guard: http/https only (rejects ftp://, file://, gopher://, ...).
	scheme := strings.ToLower(u.Scheme)
	if scheme != "http" && scheme != "https" {
		respondCode(c, apierror.ErrReconUploadURLInvalid, "upload URL must use http or https", nil)
		return "", 0, false
	}

	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if host == "" {
		respondCode(c, apierror.ErrReconUploadURLInvalid, "invalid upload URL", nil)
		return "", 0, false
	}

	// Whitelist guard: exact-host match, deny-by-default when empty.
	allowed := cfg.UploadDomainWhitelistHosts()
	if !hostAllowed(host, allowed) {
		respondCode(c, apierror.ErrReconUploadHostNotAllowed, "upload host not allowed", nil)
		return "", 0, false
	}

	// Filename from the URL path; the service layer relies on the extension for
	// type detection. Fall back to "download" when the path has no basename.
	name := path.Base(u.Path)
	if name == "." || name == "/" || name == "" {
		name = "download"
	}

	timeout := time.Duration(config.DEFAULT_UPLOAD_URL_TIMEOUT_SEC) * time.Second
	if cfg != nil && cfg.Server.UploadURLTimeoutSec > 0 {
		timeout = time.Duration(cfg.Server.UploadURLTimeoutSec) * time.Second
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), nil)
	if err != nil {
		logrus.WithError(err).Error("failed to build upload download request")
		respondCode(c, apierror.ErrReconUploadProcessingFailed, "Failed to process upload", nil)
		return "", 0, false
	}
	req.Header.Set("User-Agent", "blnk-reconciliation/1.0")

	// Custom client that re-validates every redirect's host against the
	// whitelist so a whitelisted host can't 302 to an internal IP/loopback.
	client := &http.Client{
		CheckRedirect: func(next *http.Request, _ []*http.Request) error {
			if next == nil {
				return http.ErrUseLastResponse
			}
			nextHost := strings.ToLower(strings.TrimSpace(next.URL.Hostname()))
			if nextHost == "" || !hostAllowed(nextHost, allowed) {
				return http.ErrUseLastResponse
			}
			return nil
		},
	}

	resp, err := client.Do(req)
	if err != nil {
		logrus.WithError(err).Error("failed to fetch upload URL")
		respondCode(c, apierror.ErrReconUploadProcessingFailed, "Failed to process upload", nil)
		return "", 0, false
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		logrus.WithField("status", resp.StatusCode).Error("upload URL returned non-2xx")
		respondCode(c, apierror.ErrReconUploadProcessingFailed, "Failed to process upload", nil)
		return "", 0, false
	}

	// Cap the downloaded body at MaxUploadSizeMB. An oversize response is
	// truncated, which surfaces downstream as a parse/processing failure (500).
	limit := int64(config.DEFAULT_MAX_UPLOAD_SIZE_MB) * 1024 * 1024
	if cfg != nil && cfg.Server.MaxUploadSizeMB > 0 {
		limit = cfg.Server.MaxUploadSizeMB * 1024 * 1024
	}
	limited := io.LimitReader(resp.Body, limit+1)

	id, total, err := a.blnk.UploadExternalData(ctx, source, limited, name)
	if err != nil {
		logrus.WithError(err).Error("failed to process URL upload")
		respondCode(c, apierror.ErrReconUploadProcessingFailed, "Failed to process upload", nil)
		return "", 0, false
	}

	return id, total, true
}

// hostAllowed reports whether host is present in the (lowercased) whitelist.
func hostAllowed(host string, allowed []string) bool {
	for _, a := range allowed {
		if a == host {
			return true
		}
	}
	return false
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
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}
	if len(req.MatchingRuleIDs) == 0 {
		respondCode(c, apierror.ErrReconMatchingRulesRequired, "matching_rule_ids is required", nil)
		return
	}

	reconciliationID, err := a.blnk.StartReconciliation(
		c.Request.Context(),
		req.UploadID,
		req.Strategy,
		req.GroupingCriteria,
		req.MatchingRuleIDs,
		req.DryRun,
	)
	if err != nil {
		logrus.Error(err)
		respondError(
			c,
			err,
			withDefault(apierror.ErrReconStartFailed),
			withFallbackMessage("Failed to start reconciliation"),
		)
		return
	}

	c.JSON(http.StatusOK, gin.H{"reconciliation_id": reconciliationID})
}

// InstantReconciliation initiates a reconciliation process with externally provided transactions
// without requiring a prior file upload. It processes the transactions directly and returns
// the reconciliation ID.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the request body is invalid or required fields are missing.
// - 500 Internal Server Error: If there is an error starting the reconciliation process.
// - 200 OK: If the reconciliation process is successfully started.
func (a Api) InstantReconciliation(c *gin.Context) {
	var req struct {
		ExternalTransactions []model.ExternalTransaction `json:"external_transactions" binding:"required"`
		Strategy             string                      `json:"strategy" binding:"required"`
		GroupingCriteria     string                      `json:"grouping_criteria"`
		DryRun               bool                        `json:"dry_run"`
		MatchingRuleIDs      []string                    `json:"matching_rule_ids" binding:"required"`
	}

	if err := c.ShouldBindJSON(&req); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}
	if len(req.ExternalTransactions) == 0 {
		respondCode(c, apierror.ErrReconExternalTxnsRequired, "external_transactions is required", nil)
		return
	}
	if len(req.ExternalTransactions) > model2.MaxInstantReconciliationItems {
		respondCode(c, apierror.ErrGenValidation,
			"too many external_transactions; max is "+strconv.Itoa(model2.MaxInstantReconciliationItems), nil)
		return
	}
	if len(req.MatchingRuleIDs) == 0 {
		respondCode(c, apierror.ErrReconMatchingRulesRequired, "matching_rule_ids is required", nil)
		return
	}

	reconciliationID, err := a.blnk.StartInstantReconciliation(
		c.Request.Context(),
		req.ExternalTransactions,
		req.Strategy,
		req.GroupingCriteria,
		req.MatchingRuleIDs,
		req.DryRun,
	)
	if err != nil {
		logrus.Error(err)
		respondError(
			c,
			err,
			withDefault(apierror.ErrReconStartFailed),
			withFallbackMessage("Failed to start instant reconciliation"),
		)
		return
	}

	c.JSON(http.StatusOK, gin.H{"reconciliation_id": reconciliationID})
}

// GetReconciliation retrieves details about a specific reconciliation by its ID.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the reconciliation ID is missing.
// - 404 Not Found: If the reconciliation cannot be found.
// - 500 Internal Server Error: If there is an error retrieving the reconciliation.
// - 200 OK: If the reconciliation is successfully retrieved.
func (a Api) GetReconciliation(c *gin.Context) {
	reconciliationID := c.Param("id")
	if reconciliationID == "" {
		respondCode(c, apierror.ErrGenMissingParameter, "Reconciliation ID is required", nil)
		return
	}

	reconciliation, err := a.blnk.GetReconciliation(c.Request.Context(), reconciliationID)
	if err != nil {
		logrus.Error(err)
		if code, ok := classifyMessage(err.Error()); ok && apierror.StatusForCode(code) == http.StatusNotFound {
			// Historical fixed message preserved for the legacy field.
			respondCode(c, apierror.ErrReconNotFound, "Reconciliation not found", nil)
			return
		}
		respondCode(c, apierror.ErrGenInternal, "Failed to retrieve reconciliation", nil)
		return
	}

	c.JSON(http.StatusOK, reconciliation)
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
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}

	createdRule, err := a.blnk.CreateMatchingRule(c.Request.Context(), rule)
	if err != nil {
		logrus.Error(err)
		respondError(
			c,
			err,
			withDefault(apierror.ErrGenInternal),
			withFallbackMessage("Failed to create matching rule"),
		)
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
		respondCode(c, apierror.ErrGenMissingParameter, "Matching Rule ID is required", nil)
		return
	}

	var rule model.MatchingRule
	if err := c.ShouldBindJSON(&rule); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}

	rule.RuleID = ruleID
	updatedRule, err := a.blnk.UpdateMatchingRule(c.Request.Context(), rule)
	if err != nil {
		logrus.Error(err)
		respondError(
			c,
			err,
			withUpgrade(apierror.ErrGenNotFound, apierror.ErrReconRuleNotFound),
			withDefault(apierror.ErrGenInternal),
			withFallbackMessage("Failed to update matching rule"),
		)
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
		respondCode(c, apierror.ErrGenMissingParameter, "Matching Rule ID is required", nil)
		return
	}

	err := a.blnk.DeleteMatchingRule(c.Request.Context(), ruleID)
	if err != nil {
		logrus.Error(err)
		respondError(
			c,
			err,
			withUpgrade(apierror.ErrGenNotFound, apierror.ErrReconRuleNotFound),
			withDefault(apierror.ErrGenInternal),
			withFallbackMessage("Failed to delete matching rule"),
		)
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Matching rule deleted successfully"})
}
