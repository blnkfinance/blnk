package api

import (
	"net/http"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/gin-gonic/gin"
)

// MetadataRequest represents the structure for metadata update requests.
// It contains a required metadata field that holds key-value pairs for updating entity metadata.
type MetadataRequest struct {
	Metadata map[string]interface{} `json:"meta_data" binding:"required"`
}

// UpdateMetadata handles HTTP requests to update metadata for various entity types.
// It processes requests to update metadata for ledgers, transactions, balances, and identities.
// The entity type is determined automatically from the entity ID prefix.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If the entity ID is missing or the request body is invalid.
// - 404 Not Found: If the specified entity doesn't exist.
// - 200 OK: If the metadata is successfully updated.
func (a Api) UpdateMetadata(c *gin.Context) {
	entityID := c.Param("entity-id")

	if entityID == "" {
		respondCode(c, apierror.ErrMetaInvalidEntityID, "entity ID is required", nil)
		return
	}

	var req MetadataRequest
	if err := c.ShouldBindJSON(&req); err != nil {
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}

	// Known conditions (entity not found, unsupported entity type, invalid
	// entity ID) classify to META_* codes; anything else is an internal
	// failure (e.g. database error) and falls back to a sanitized 500.
	updatedMetadata, err := a.blnk.UpdateMetadata(c.Request.Context(), entityID, req.Metadata)
	if err != nil {
		respondError(c, err)
		return
	}

	c.JSON(http.StatusOK, gin.H{"meta_data": updatedMetadata})
}
