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

package backups

import (
	"context"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	_ "github.com/lib/pq" // Import the postgres driver
	"github.com/stretchr/testify/assert"
)

func TestBackupManager_Struct(t *testing.T) {
	bm := &BackupManager{
		Config:   nil,
		S3Client: nil,
	}

	assert.NotNil(t, bm)
	assert.Nil(t, bm.Config)
	assert.Nil(t, bm.S3Client)
}

func TestBackupManager_BackupToDisk_InvalidDSN(t *testing.T) {
	cfg := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "invalid-dsn",
		},
		BackupDir: "/tmp/blnk-backup-test",
	}

	bm := &BackupManager{
		Config: cfg,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filePath, err := bm.BackupToDisk(ctx)
	assert.Error(t, err)
	assert.Empty(t, filePath)
}

func TestBackupManager_BackupToDisk_UnreachableDB(t *testing.T) {
	cfg := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "postgres://user:password@localhost:9999/nonexistent?sslmode=disable",
		},
		BackupDir: "/tmp/blnk-backup-test",
	}

	bm := &BackupManager{
		Config: cfg,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	filePath, err := bm.BackupToDisk(ctx)
	assert.Error(t, err)
	assert.Empty(t, filePath)
	// Error could be "failed to ping database" or "failed to open database connection"
}

func TestBackupManager_BackupToS3_FailsOnBackup(t *testing.T) {
	cfg := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "invalid-dsn",
		},
		BackupDir: "/tmp/blnk-backup-test",
	}

	bm := &BackupManager{
		Config: cfg,
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := bm.BackupToS3(ctx)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to backup to disk")
}

func TestBackupManager_BackupToDisk_ContextCancellation(t *testing.T) {
	cfg := &config.Configuration{
		DataSource: config.DataSourceConfig{
			Dns: "postgres://user:password@localhost:5432/testdb?sslmode=disable",
		},
		BackupDir: "/tmp/blnk-backup-test",
	}

	bm := &BackupManager{
		Config: cfg,
	}

	// Create an already cancelled context
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	filePath, err := bm.BackupToDisk(ctx)
	assert.Error(t, err)
	assert.Empty(t, filePath)
}
