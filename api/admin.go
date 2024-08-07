package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk/internal/apierror"
	backups "github.com/jerry-enebeli/blnk/internal/pg-backups"
)

func (a Api) BackupDB(c *gin.Context) {
	backupManager, err := backups.NewBackupManager()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": apierror.NewAPIError(apierror.ErrInternalServer, "error creating backup", err)})
		return
	}
	_, err = backupManager.BackupToDisk(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": apierror.NewAPIError(apierror.ErrInternalServer, "error creating backup", err)})
		return
	}
	c.JSON(200, "backup successful")
}

func (a Api) BackupDBS3(c *gin.Context) {
	backupManager, err := backups.NewBackupManager()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": apierror.NewAPIError(apierror.ErrInternalServer, "error creating backup", err)})
		return
	}
	err = backupManager.BackupToS3(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": apierror.NewAPIError(apierror.ErrInternalServer, "error creating backup", err)})
		return
	}
	c.JSON(200, "backup successful")
}
