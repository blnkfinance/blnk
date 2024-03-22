package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
	backups "github.com/jerry-enebeli/blnk/internal/pg-backups"
)

func (a Api) BackupDB(c *gin.Context) {
	err := backups.BackupDB()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, "backup successful")
}

func (a Api) BackupDBS3(c *gin.Context) {
	err := backups.ZipUploadToS3()
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(200, "backup successful")
}
