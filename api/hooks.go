package api

import (
	"errors"
	"net/http"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/hooks"
	"github.com/gin-gonic/gin"
)

var errHooksRequireMasterKey = errors.New("hook management requires master key")

// hookFailureCode distinguishes a missing hook (the manager returns
// "hook not found: <id>") from genuine infrastructure failures.
func hookFailureCode(err error) apierror.ErrorCode {
	if code, ok := classifyMessage(err.Error()); ok && apierror.StatusForCode(code) == http.StatusNotFound {
		return apierror.ErrHookNotFound
	}
	return apierror.ErrHookOperationFailed
}

func isHookMasterKeyRequest(c *gin.Context) bool {
	isMasterKey, _ := c.Get("isMasterKey")
	isMaster, _ := isMasterKey.(bool)
	return isMaster
}

func ensureHookManagementAuthorized(c *gin.Context) bool {
	if isHookMasterKeyRequest(c) {
		return true
	}

	respondCode(c, apierror.ErrAuthMasterKeyRequired, errHooksRequireMasterKey.Error(), nil)
	return false
}

// RegisterHook handles the registration of a new webhook.
func (a *Api) RegisterHook(c *gin.Context) {
	if !ensureHookManagementAuthorized(c) {
		return
	}

	var hook hooks.Hook
	if err := c.ShouldBindJSON(&hook); err != nil {
		respondBareAPIError(c, apierror.NewAPIError(apierror.ErrInvalidInput, "invalid hook data", err), apierror.ErrHookInvalid)
		return
	}

	if err := a.blnk.Hooks.RegisterHook(c.Request.Context(), &hook); err != nil {
		respondBareAPIError(c, apierror.NewAPIError(apierror.ErrInvalidInput, "failed to register hook", err), apierror.ErrHookOperationFailed)
		return
	}

	c.JSON(http.StatusCreated, hook)
}

// UpdateHook handles updating an existing webhook.
func (a *Api) UpdateHook(c *gin.Context) {
	if !ensureHookManagementAuthorized(c) {
		return
	}

	hookID := c.Param("id")
	var hook hooks.Hook
	if err := c.ShouldBindJSON(&hook); err != nil {
		respondBareAPIError(c, apierror.NewAPIError(apierror.ErrInvalidInput, "invalid hook data", err), apierror.ErrHookInvalid)
		return
	}

	if err := a.blnk.Hooks.UpdateHook(c.Request.Context(), hookID, &hook); err != nil {
		respondBareAPIError(c, apierror.NewAPIError(apierror.ErrInvalidInput, "failed to update hook", err), hookFailureCode(err))
		return
	}

	c.JSON(http.StatusOK, hook)
}

// GetHook retrieves a specific webhook by ID.
func (a *Api) GetHook(c *gin.Context) {
	if !ensureHookManagementAuthorized(c) {
		return
	}

	hookID := c.Param("id")
	hook, err := a.blnk.Hooks.GetHook(c.Request.Context(), hookID)
	if err != nil {
		respondBareAPIError(c, apierror.NewAPIError(apierror.ErrNotFound, "hook not found", err), apierror.ErrHookNotFound)
		return
	}

	c.JSON(http.StatusOK, hook)
}

// ListHooks retrieves all hooks of a specific type.
func (a *Api) ListHooks(c *gin.Context) {
	if !ensureHookManagementAuthorized(c) {
		return
	}

	hookType := hooks.HookType(c.Query("type"))
	hooks, err := a.blnk.Hooks.ListHooks(c.Request.Context(), hookType)
	if err != nil {
		respondBareAPIError(c, apierror.NewAPIError(apierror.ErrInvalidInput, "failed to list hooks", err), apierror.ErrHookOperationFailed)
		return
	}

	c.JSON(http.StatusOK, hooks)
}

// DeleteHook removes a webhook by ID.
func (a *Api) DeleteHook(c *gin.Context) {
	if !ensureHookManagementAuthorized(c) {
		return
	}

	hookID := c.Param("id")
	if err := a.blnk.Hooks.DeleteHook(c.Request.Context(), hookID); err != nil {
		respondBareAPIError(c, apierror.NewAPIError(apierror.ErrInvalidInput, "failed to delete hook", err), hookFailureCode(err))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "hook deleted successfully"})
}
