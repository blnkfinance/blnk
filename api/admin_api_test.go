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
	"path/filepath"
	"runtime"
	"testing"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// installFakePgDump puts an executable pg_dump stub first on PATH, mirroring
// the helper the pg-backups package already uses. The stub honours -f by
// writing the dump file the real binary would produce.
//
// The happy path used to be gated behind BLNK_TEST_PG_DUMP, which nothing sets
// — not the workflows, not the compose files, nothing in the repo — so it never
// ran anywhere, including CI. Depending on a real pg_dump instead would only
// half-fix that: pg_dump refuses to dump from a server newer than its own major
// version, and CI pins postgres:latest against ubuntu-latest's bundled client,
// so the happy path would still skip on exactly the machine that matters.
//
// A stub removes the environment from the question entirely. What this test
// covers is the endpoint wiring — route, config, BackupDir, response — and that
// is worth asserting on every run rather than never.
func installFakePgDump(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake pg_dump stub requires a POSIX shell")
	}

	dir := t.TempDir()
	stub := filepath.Join(dir, "pg_dump")
	script := `#!/bin/sh
prev=""
for a in "$@"; do
  if [ "$prev" = "-f" ]; then out="$a"; fi
  prev="$a"
done
echo "-- fake dump" > "$out"
`
	require.NoError(t, os.WriteFile(stub, []byte(script), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// Backups shell out to pg_dump and write under BackupDir. The happy path runs
// against a stub so it is environment-independent; the error path runs
// everywhere using an unwritable BackupDir.
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
		installFakePgDump(t)

		backupDir := t.TempDir()
		router, _, _ := setupRouterWithConfig(t, func(cfg *config.Configuration) {
			cfg.BackupDir = backupDir
		})

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
