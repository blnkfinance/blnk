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
	"os"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/blnkfinance/blnk/config"
	_ "github.com/lib/pq"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// installFakePgDump puts an executable pg_dump stub on PATH. The stub writes
// a marker to the file passed via -f and records its arguments, so the test
// exercises the full BackupToDisk flow without a real PostgreSQL client.
func installFakePgDump(t *testing.T, script string) string {
	t.Helper()
	if runtime.GOOS == "windows" {
		t.Skip("fake pg_dump stub requires a POSIX shell")
	}

	dir := t.TempDir()
	stub := filepath.Join(dir, "pg_dump")
	require.NoError(t, os.WriteFile(stub, []byte(script), 0o755))
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
	return dir
}

func diskBackupConfig(t *testing.T, backupDir string) {
	t.Helper()
	config.MockConfig(&config.Configuration{
		BackupDir:  backupDir,
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
	})
}

func TestBackupToDisk_Success(t *testing.T) {
	backupDir := t.TempDir()
	diskBackupConfig(t, backupDir)

	argsFile := filepath.Join(t.TempDir(), "args")
	// Stub finds the -f argument, writes dump content there, records args.
	installFakePgDump(t, `#!/bin/sh
echo "$@" > `+argsFile+`
prev=""
for a in "$@"; do
  if [ "$prev" = "-f" ]; then out="$a"; fi
  prev="$a"
done
echo "-- fake dump" > "$out"
`)

	bm, err := NewBackupManager()
	require.NoError(t, err)

	backupFile, err := bm.BackupToDisk(context.Background())
	require.NoError(t, err)

	content, err := os.ReadFile(backupFile)
	require.NoError(t, err)
	assert.Equal(t, "-- fake dump\n", string(content))
	assert.Contains(t, backupFile, backupDir, "backup must be written under the configured BackupDir")
	assert.Equal(t, ".sql", filepath.Ext(backupFile))

	// pg_dump must receive the database identity parsed from the DSN.
	args, err := os.ReadFile(argsFile)
	require.NoError(t, err)
	assert.Contains(t, string(args), "-U postgres")
	assert.Contains(t, string(args), "-d blnk")
}

func TestBackupToDisk_PgDumpFails(t *testing.T) {
	diskBackupConfig(t, t.TempDir())
	installFakePgDump(t, `#!/bin/sh
echo "fatal: connection refused" >&2
exit 1
`)

	bm, err := NewBackupManager()
	require.NoError(t, err)

	_, err = bm.BackupToDisk(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pg_dump failed")
	assert.Contains(t, err.Error(), "connection refused", "stderr from pg_dump must surface in the error")
}

func TestBackupToDisk_NoFileProduced(t *testing.T) {
	diskBackupConfig(t, t.TempDir())
	// Stub exits 0 without writing the dump file: BackupToDisk must notice.
	installFakePgDump(t, `#!/bin/sh
exit 0
`)

	bm, err := NewBackupManager()
	require.NoError(t, err)

	_, err = bm.BackupToDisk(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "backup file does not exist")
}

func TestBackupToDisk_UnreachableDatabase(t *testing.T) {
	config.MockConfig(&config.Configuration{
		BackupDir:  t.TempDir(),
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
		DataSource: config.DataSourceConfig{Dns: "postgres://postgres:@localhost:59999/blnk?sslmode=disable"},
	})

	bm, err := NewBackupManager()
	require.NoError(t, err)

	_, err = bm.BackupToDisk(context.Background())
	require.Error(t, err)
	assert.Contains(t, err.Error(), "failed to ping database")
}
