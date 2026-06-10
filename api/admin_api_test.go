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
	"net/http/httptest"
	"os"
	"testing"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
)

// Backups shell out to pg_dump and write under BackupDir. The happy path is
// environment-dependent (pg_dump binary + matching server version), so it is
// gated behind BLNK_TEST_PG_DUMP; the error path runs everywhere using an
// unwritable BackupDir.
func TestBackupDB(t *testing.T) {
	t.Run("Unwritable backup dir", func(t *testing.T) {
		router, _, _ := setupRouterWithConfig(t, func(cfg *config.Configuration) {
			cfg.BackupDir = "/dev/null/blnk-backups"
		})

		req := httptest.NewRequest("GET", "/backup", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusInternalServerError, resp.Code)
		assert.Contains(t, resp.Body.String(), "error creating backup")
	})

	t.Run("Successful backup to disk", func(t *testing.T) {
		if os.Getenv("BLNK_TEST_PG_DUMP") == "" {
			t.Skip("BLNK_TEST_PG_DUMP not set; skipping pg_dump-dependent test")
		}
		router, _, _ := setupRouterWithConfig(t, func(cfg *config.Configuration) {
			cfg.BackupDir = t.TempDir()
		})

		req := httptest.NewRequest("GET", "/backup", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusOK, resp.Code)
	})
}

func TestBackupDBS3(t *testing.T) {
	t.Run("Unwritable backup dir", func(t *testing.T) {
		router, _, _ := setupRouterWithConfig(t, func(cfg *config.Configuration) {
			cfg.BackupDir = "/dev/null/blnk-backups"
		})

		req := httptest.NewRequest("GET", "/backup-s3", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		assert.Equal(t, http.StatusInternalServerError, resp.Code)
		assert.Contains(t, resp.Body.String(), "error creating backup")
	})
}
