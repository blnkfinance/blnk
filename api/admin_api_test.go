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
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strconv"
	"testing"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// pgDumpUsable reports whether the happy path can actually run here, and why
// not when it cannot.
//
// This used to be gated behind BLNK_TEST_PG_DUMP, which nothing sets — not the
// workflows, not the compose files — so the happy path never ran anywhere,
// including CI. The gate now tests the two real preconditions instead, so the
// backup succeeds wherever the environment permits it.
//
// The version check is the subtle one: pg_dump refuses to dump from a server
// newer than itself. CI pins postgres:latest against ubuntu-latest's bundled
// client, so that skew is expected rather than a defect, and it is reported as
// a skip with both versions named rather than as a failure.
func pgDumpUsable(t *testing.T, dsn string) (bool, string) {
	t.Helper()

	if _, err := exec.LookPath("pg_dump"); err != nil {
		return false, "pg_dump is not installed"
	}

	out, err := exec.Command("pg_dump", "--version").Output()
	if err != nil {
		return false, fmt.Sprintf("pg_dump --version failed: %v", err)
	}
	m := regexp.MustCompile(`(\d+)`).FindStringSubmatch(string(out))
	if m == nil {
		return false, fmt.Sprintf("could not parse pg_dump version from %q", string(out))
	}
	clientMajor, _ := strconv.Atoi(m[1])

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return false, fmt.Sprintf("cannot open database: %v", err)
	}
	defer func() { _ = db.Close() }()

	var serverNum int
	if err := db.QueryRow("SHOW server_version_num").Scan(&serverNum); err != nil {
		return false, fmt.Sprintf("cannot read server version: %v", err)
	}
	serverMajor := serverNum / 10000

	if clientMajor < serverMajor {
		return false, fmt.Sprintf("pg_dump %d is older than server %d; pg_dump refuses to dump a newer server",
			clientMajor, serverMajor)
	}
	return true, ""
}

// Backups shell out to pg_dump and write under BackupDir. The happy path needs
// a usable pg_dump (see pgDumpUsable); the error path runs everywhere using an
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
		backupDir := t.TempDir()
		router, _, cfg := setupRouterWithConfig(t, func(cfg *config.Configuration) {
			cfg.BackupDir = backupDir
		})
		if ok, why := pgDumpUsable(t, cfg.DataSource.Dns); !ok {
			t.Skip("skipping backup happy path: " + why)
		}

		req := httptest.NewRequest("GET", "/backup", nil)
		resp := httptest.NewRecorder()
		router.ServeHTTP(resp, req)
		require.Equal(t, http.StatusOK, resp.Code, resp.Body.String())

		// A 200 alone would also be returned if pg_dump wrote nothing, so
		// assert a non-empty dump actually landed under BackupDir.
		dumps, err := filepath.Glob(filepath.Join(backupDir, "*", "*.sql"))
		require.NoError(t, err)
		require.NotEmpty(t, dumps, "backup reported success but wrote no .sql file under %s", backupDir)

		info, err := os.Stat(dumps[0])
		require.NoError(t, err)
		assert.Greater(t, info.Size(), int64(0), "backup file %s is empty", dumps[0])
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
