package api

import (
	"net/http"

	"github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/database"
	"github.com/gin-gonic/gin"
)

// CreateAPIKey creates a new API key for the authenticated user
//
// Parameters:
// - c: The Gin context containing the request and response
//
// Responses:
// - 400 Bad Request: If there's an error in the request body
// - 201 Created: If the API key is successfully created
func (a Api) CreateAPIKey(c *gin.Context) {
	var req model.CreateAPIKeyRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	apiKey, err := a.blnk.CreateAPIKey(c.Request.Context(), req.Name, req.Owner, req.Scopes, req.ExpiresAt)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusCreated, apiKey)
}

// ListAPIKeys lists all API keys for the authenticated user
//
// Parameters:
// - c: The Gin context containing the request and response
//
// Responses:
// - 200 OK: Returns the list of API keys
// - 500 Internal Server Error: If there's an error retrieving the keys
func (a Api) ListAPIKeys(c *gin.Context) {
	owner := c.GetString("owner")
	if owner == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	keys, err := a.blnk.ListAPIKeys(c.Request.Context(), owner)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, keys)
}

// RevokeAPIKey revokes an API key
//
// Parameters:
// - c: The Gin context containing the request and response
//
// Responses:
// - 204 No Content: If the API key is successfully revoked
// - 404 Not Found: If the API key is not found
// - 403 Forbidden: If the user doesn't own the API key
func (a Api) RevokeAPIKey(c *gin.Context) {
	id := c.Param("id")
	owner := c.GetString("owner")
	if owner == "" {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}

	if err := a.blnk.RevokeAPIKey(c.Request.Context(), id, owner); err != nil {
		switch err {
		case database.ErrAPIKeyNotFound:
			c.JSON(http.StatusNotFound, gin.H{"error": "API key not found"})
		case database.ErrInvalidAPIKey:
			c.JSON(http.StatusForbidden, gin.H{"error": "unauthorized"})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		}
		return
	}

	c.Status(http.StatusNoContent)
}
