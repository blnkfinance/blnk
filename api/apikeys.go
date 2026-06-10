package api

import (
	"errors"
	"net/http"

	authz "github.com/blnkfinance/blnk/api/middleware"
	"github.com/blnkfinance/blnk/api/model"
	"github.com/blnkfinance/blnk/database"
	"github.com/blnkfinance/blnk/internal/apierror"
	coremodel "github.com/blnkfinance/blnk/model"
	"github.com/gin-gonic/gin"
)

var (
	errAPIKeyOwnerRequired    = errors.New("owner is required")
	errAPIKeyCrossOwnerAccess = errors.New("cannot manage API keys for another owner")
	errAPIKeyScopeEscalation  = errors.New("cannot grant scopes broader than caller")
	errMissingAPIKeyPrincipal = errors.New("authenticated API key principal missing")
)

func isMasterKeyRequest(c *gin.Context) bool {
	isMasterKey, _ := c.Get("isMasterKey")
	isMaster, _ := isMasterKey.(bool)
	return isMaster
}

func currentAPIKeyPrincipal(c *gin.Context) (*coremodel.APIKey, bool) {
	apiKeyValue, exists := c.Get("apiKey")
	if !exists {
		return nil, false
	}

	apiKey, ok := apiKeyValue.(*coremodel.APIKey)
	return apiKey, ok
}

func resolveAPIKeyOwner(isMaster bool, principal *coremodel.APIKey, requestedOwner string, ownerRequired bool) (string, error) {
	if isMaster {
		if ownerRequired && requestedOwner == "" {
			return "", errAPIKeyOwnerRequired
		}
		return requestedOwner, nil
	}

	if principal == nil {
		return "", errMissingAPIKeyPrincipal
	}

	if requestedOwner != "" && requestedOwner != principal.OwnerID {
		return "", errAPIKeyCrossOwnerAccess
	}

	return principal.OwnerID, nil
}

func validateGrantedScopes(isMaster bool, principal *coremodel.APIKey, requestedScopes []string) error {
	if isMaster {
		return nil
	}

	if principal == nil {
		return errMissingAPIKeyPrincipal
	}

	if !authz.CanGrantScopes(principal.Scopes, requestedScopes) {
		return errAPIKeyScopeEscalation
	}

	return nil
}

func writeAPIKeyAuthorizationError(c *gin.Context, err error) bool {
	if err == nil {
		return false
	}

	switch err {
	case errAPIKeyOwnerRequired:
		respondCode(c, apierror.ErrAPIKeyOwnerRequired, err.Error(), nil)
	case errMissingAPIKeyPrincipal:
		respondCode(c, apierror.ErrAuthMissingPrincipal, err.Error(), nil)
	case errAPIKeyCrossOwnerAccess:
		respondCode(c, apierror.ErrAuthCrossOwnerAccess, err.Error(), nil)
	case errAPIKeyScopeEscalation:
		respondCode(c, apierror.ErrAuthScopeEscalation, err.Error(), nil)
	default:
		respondError(c, err)
	}

	return true
}

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
		respondCode(c, apierror.ErrGenMalformedRequest, err.Error(), nil)
		return
	}

	isMaster := isMasterKeyRequest(c)
	principal, _ := currentAPIKeyPrincipal(c)

	ownerID, err := resolveAPIKeyOwner(isMaster, principal, req.Owner, true)
	if writeAPIKeyAuthorizationError(c, err) {
		return
	}

	if err := validateGrantedScopes(isMaster, principal, req.Scopes); writeAPIKeyAuthorizationError(c, err) {
		return
	}

	apiKey, err := a.blnk.CreateAPIKey(c.Request.Context(), req.Name, ownerID, req.Scopes, req.ExpiresAt)
	if err != nil {
		respondError(c, err)
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
	isMaster := isMasterKeyRequest(c)
	principal, _ := currentAPIKeyPrincipal(c)

	ownerID, err := resolveAPIKeyOwner(isMaster, principal, c.Query("owner"), isMaster)
	if writeAPIKeyAuthorizationError(c, err) {
		return
	}

	keys, err := a.blnk.ListAPIKeys(c.Request.Context(), ownerID)
	if err != nil {
		respondError(c, err)
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
	isMaster := isMasterKeyRequest(c)
	principal, _ := currentAPIKeyPrincipal(c)

	ownerID, err := resolveAPIKeyOwner(isMaster, principal, c.Query("owner"), isMaster)
	if writeAPIKeyAuthorizationError(c, err) {
		return
	}

	if err := a.blnk.RevokeAPIKey(c.Request.Context(), id, ownerID); err != nil {
		switch err {
		case database.ErrAPIKeyNotFound:
			respondCode(c, apierror.ErrAPIKeyNotFound, "API key not found", nil)
		case database.ErrInvalidAPIKey:
			// In the revoke flow this means the key belongs to another owner —
			// an authorization failure, so 403 (not 401) stays correct.
			respondCode(c, apierror.ErrAuthCrossOwnerAccess, "unauthorized", nil)
		default:
			respondError(c, err)
		}
		return
	}

	c.Status(http.StatusNoContent)
}
