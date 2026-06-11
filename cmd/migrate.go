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

/*
Package main provides the CLI commands for managing database migrations in the Blnk application.
This includes commands for applying and rolling back migrations.
*/

package main

import (
	"fmt"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// migrateCommands creates the root command for migration-related operations.
func migrateCommands(_ *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "start blnk migration",
	}

	// Add subcommands for migrating up and down.
	cmd.AddCommand(migrateUpCommands())
	cmd.AddCommand(migrateDownCommands())

	return cmd
}

// runMigrations fetches the configuration, connects to the database, and
// applies (or rolls back) the embedded SQL migrations in the "blnk" schema.
// It returns the number of migrations applied.
func runMigrations(direction migrate.MigrationDirection) (int, error) {
	// Define the source of the migrations.
	migrations := migrate.EmbedFileSystemMigrationSource{
		FileSystem: blnk.SQLFiles,
		Root:       "sql",
	}

	// Fetch the configuration.
	cnf, err := config.Fetch()
	if err != nil {
		return 0, fmt.Errorf("error fetching config: %w", err)
	}

	// Connect to the database.
	db, err := database.ConnectDB(cnf.DataSource)
	if err != nil {
		return 0, fmt.Errorf("error connecting to database: %w", err)
	}
	defer func() { _ = db.Close() }()

	// Set the schema for the migrations. (Previously only the up path set
	// this; a standalone `migrate down` would have targeted the default
	// search_path instead of the blnk schema.)
	migrate.SetSchema("blnk")

	return migrate.Exec(db, "postgres", migrations, direction)
}

// migrateUpCommands creates the command for applying migrations.
func migrateUpCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "up",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			n, err := runMigrations(migrate.Up)
			if err != nil {
				return fmt.Errorf("error migrating up: %w", err)
			}
			logrus.Infof("Applied %d migrations!\n", n)
			return nil
		},
	}

	return cmd
}

// migrateDownCommands creates the command for rolling back migrations.
func migrateDownCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:           "down",
		SilenceUsage:  true,
		SilenceErrors: true,
		RunE: func(cmd *cobra.Command, args []string) error {
			n, err := runMigrations(migrate.Down)
			if err != nil {
				return fmt.Errorf("error migrating down: %w", err)
			}
			logrus.Infof("Rolled back %d migrations!\n", n)
			return nil
		},
	}

	return cmd
}
