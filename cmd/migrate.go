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
	"log"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/database"
	migrate "github.com/rubenv/sql-migrate"
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

// migrateUpCommands creates the command for applying migrations.
func migrateUpCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use: "up",
		Run: func(cmd *cobra.Command, args []string) {
			// Define the source of the migrations.
			migrations := migrate.EmbedFileSystemMigrationSource{
				FileSystem: blnk.SQLFiles,
				Root:       "sql",
			}

			// Fetch the configuration.
			cnf, err := config.Fetch()
			if err != nil {
				log.Printf("Error fetching config: %v", err)
				return
			}

			// Connect to the database.
			db, err := database.ConnectDB(cnf.DataSource.Dns)
			if err != nil {
				log.Printf("Error connecting to database: %v", err)
				return
			}

			// Set the schema for the migrations.
			migrate.SetSchema("blnk")

			// Apply the migrations.
			n, err := migrate.Exec(db, "postgres", migrations, migrate.Up)
			if err != nil {
				log.Printf("Error migrating up: %v", err)
			} else {
				fmt.Printf("Applied %d migrations!\n", n)
			}
		},
	}

	return cmd
}

// migrateDownCommands creates the command for rolling back migrations.
func migrateDownCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use: "down",
		Run: func(cmd *cobra.Command, args []string) {
			// Define the source of the migrations.
			migrations := migrate.EmbedFileSystemMigrationSource{
				FileSystem: blnk.SQLFiles,
				Root:       "sql",
			}

			// Fetch the configuration.
			cnf, err := config.Fetch()
			if err != nil {
				log.Printf("Error fetching config: %v", err)
				return
			}

			// Connect to the database.
			db, err := database.ConnectDB(cnf.DataSource.Dns)
			if err != nil {
				log.Printf("Error connecting to database: %v", err)
				return
			}

			// Roll back the migrations.
			n, err := migrate.Exec(db, "postgres", migrations, migrate.Down)
			if err != nil {
				log.Printf("Error migrating down: %v", err)
			} else {
				fmt.Printf("Rolled back %d migrations!\n", n)
			}
		},
	}

	return cmd
}
