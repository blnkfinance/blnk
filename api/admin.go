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
	"github.com/jerry-enebeli/blnk/internal/apierror"
	backups "github.com/jerry-enebeli/blnk/internal/pg-backups"
)

// BackupDB creates a backup of the database and stores it on disk.
// It initializes a BackupManager and performs the backup operation.
// If any error occurs during backup creation, it responds with an error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in creating the backup or initializing the BackupManager.
// - 200 OK: If the backup is successfully created and stored on disk.
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
	c.JSON(http.StatusOK, "backup successful")
}

// BackupDBS3 creates a backup of the database and stores it in S3.
// It initializes a BackupManager and performs the backup operation to S3.
// If any error occurs during backup creation, it responds with an error message.
//
// Parameters:
// - c: The Gin context containing the request and response.
//
// Responses:
// - 400 Bad Request: If there's an error in creating the backup or initializing the BackupManager.
// - 200 OK: If the backup is successfully created and stored in S3.
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
	c.JSON(http.StatusOK, "backup successful")
}
