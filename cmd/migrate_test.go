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

package main

import (
	"database/sql"
	"testing"

	"github.com/blnkfinance/blnk/config"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const migrateScratchDB = "blnk_migrate_cmd_test"

// createScratchDatabase creates a throwaway database on the local test
// Postgres so migrations can be applied and rolled back without touching
// the shared blnk test database.
func createScratchDatabase(t *testing.T) string {
	t.Helper()

	admin, err := sql.Open("postgres", "postgres://postgres:@localhost:5432/blnk?sslmode=disable")
	require.NoError(t, err)
	require.NoError(t, admin.Ping(), "Postgres must be running for the cmd test suite")

	_, _ = admin.Exec("DROP DATABASE IF EXISTS " + migrateScratchDB + " WITH (FORCE)")
	_, err = admin.Exec("CREATE DATABASE " + migrateScratchDB)
	require.NoError(t, err)

	t.Cleanup(func() {
		_, _ = admin.Exec("DROP DATABASE IF EXISTS " + migrateScratchDB + " WITH (FORCE)")
		_ = admin.Close()
	})

	return "postgres://postgres:@localhost:5432/" + migrateScratchDB + "?sslmode=disable"
}

func TestRunMigrations_UpAndDown(t *testing.T) {
	dsn := createScratchDatabase(t)
	config.MockConfig(&config.Configuration{
		DataSource: config.DataSourceConfig{Dns: dsn},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
	})

	// Up: a fresh database must receive every embedded migration.
	applied, err := runMigrations(migrate.Up)
	require.NoError(t, err)
	assert.Greater(t, applied, 0, "a fresh database must apply at least one migration")

	// The core schema must genuinely exist afterwards.
	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var tableCount int
	require.NoError(t, db.QueryRow(
		`SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'blnk'`,
	).Scan(&tableCount))
	assert.Greater(t, tableCount, 5, "migrate up must create the blnk schema tables")

	// Up again: idempotent, nothing new to apply.
	applied, err = runMigrations(migrate.Up)
	require.NoError(t, err)
	assert.Equal(t, 0, applied, "re-running migrate up must apply nothing")

	// Down: rollbacks must execute against the same schema.
	rolledBack, err := runMigrations(migrate.Down)
	require.NoError(t, err)
	assert.Greater(t, rolledBack, 0, "migrate down must roll back at least one migration")
}

func TestRunMigrations_BadDSNFailsFast(t *testing.T) {
	config.MockConfig(&config.Configuration{
		DataSource: config.DataSourceConfig{Dns: "postgres://%zz-invalid-dsn"},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
	})

	_, err := runMigrations(migrate.Up)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "error connecting to database")
}

func TestMigrateCommands_Wiring(t *testing.T) {
	cmd := migrateCommands(&blnkInstance{})
	subcommands := make(map[string]bool)
	for _, sub := range cmd.Commands() {
		subcommands[sub.Use] = true
	}
	assert.True(t, subcommands["up"], "migrate must expose the up subcommand")
	assert.True(t, subcommands["down"], "migrate must expose the down subcommand")
}

func TestMigrateUpCommand_RunsViaCobra(t *testing.T) {
	dsn := createScratchDatabase(t)
	config.MockConfig(&config.Configuration{
		DataSource: config.DataSourceConfig{Dns: dsn},
		Redis:      config.RedisConfig{Dns: "localhost:6379"},
	})

	// Execute the actual cobra command (bypassing root preRun, which needs a
	// blnk.json file) and verify it migrated the scratch database.
	upCmd := migrateUpCommands()
	upCmd.Run(upCmd, nil)

	db, err := sql.Open("postgres", dsn)
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	var exists bool
	require.NoError(t, db.QueryRow(
		`SELECT EXISTS (SELECT 1 FROM information_schema.tables WHERE table_schema = 'blnk' AND table_name = 'gorp_migrations')`,
	).Scan(&exists))
	assert.True(t, exists, "the cobra up command must record applied migrations")
}
