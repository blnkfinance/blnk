package api

import (
	"net/http"

	"github.com/blnkfinance/blnk/internal/apierror"
	"github.com/blnkfinance/blnk/internal/hooks"
	"github.com/gin-gonic/gin"
)

// RegisterHook handles the registration of a new webhook.
func (a *Api) RegisterHook(c *gin.Context) {
	var hook hooks.Hook
	if err := c.ShouldBindJSON(&hook); err != nil {
		c.JSON(http.StatusBadRequest, apierror.NewAPIError(apierror.ErrInvalidInput, "invalid hook data", err))
		return
	}

	if err := a.blnk.Hooks.RegisterHook(c.Request.Context(), &hook); err != nil {
		c.JSON(http.StatusBadRequest, apierror.NewAPIError(apierror.ErrInvalidInput, "failed to register hook", err))
		return
	}

	c.JSON(http.StatusCreated, hook)
}

// UpdateHook handles updating an existing webhook.
func (a *Api) UpdateHook(c *gin.Context) {
	hookID := c.Param("id")
	var hook hooks.Hook
	if err := c.ShouldBindJSON(&hook); err != nil {
		c.JSON(http.StatusBadRequest, apierror.NewAPIError(apierror.ErrInvalidInput, "invalid hook data", err))
		return
	}

	if err := a.blnk.Hooks.UpdateHook(c.Request.Context(), hookID, &hook); err != nil {
		c.JSON(http.StatusBadRequest, apierror.NewAPIError(apierror.ErrInvalidInput, "failed to update hook", err))
		return
	}

	c.JSON(http.StatusOK, hook)
}

// GetHook retrieves a specific webhook by ID.
func (a *Api) GetHook(c *gin.Context) {
	hookID := c.Param("id")
	hook, err := a.blnk.Hooks.GetHook(c.Request.Context(), hookID)
	if err != nil {
		c.JSON(http.StatusNotFound, apierror.NewAPIError(apierror.ErrNotFound, "hook not found", err))
		return
	}

	c.JSON(http.StatusOK, hook)
}

// ListHooks retrieves all hooks of a specific type.
func (a *Api) ListHooks(c *gin.Context) {
	hookType := hooks.HookType(c.Query("type"))
	hooks, err := a.blnk.Hooks.ListHooks(c.Request.Context(), hookType)
	if err != nil {
		c.JSON(http.StatusBadRequest, apierror.NewAPIError(apierror.ErrInvalidInput, "failed to list hooks", err))
		return
	}

	c.JSON(http.StatusOK, hooks)
}

// DeleteHook removes a webhook by ID.
func (a *Api) DeleteHook(c *gin.Context) {
	hookID := c.Param("id")
	if err := a.blnk.Hooks.DeleteHook(c.Request.Context(), hookID); err != nil {
		c.JSON(http.StatusBadRequest, apierror.NewAPIError(apierror.ErrInvalidInput, "failed to delete hook", err))
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "hook deleted successfully"})
}
