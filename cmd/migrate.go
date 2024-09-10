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
	"fmt"
	"log"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/database"

	"github.com/jerry-enebeli/blnk"
	migrate "github.com/rubenv/sql-migrate"
	"github.com/spf13/cobra"
)

func migrateCommands(_ *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "migrate",
		Short: "start blnk migration",
	}

	cmd.AddCommand(migrateUpCommands())
	cmd.AddCommand(migrateDownCommands())

	return cmd
}

func migrateUpCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use: "up",
		Run: func(cmd *cobra.Command, args []string) {
			migrations := migrate.EmbedFileSystemMigrationSource{
				FileSystem: blnk.SQLFiles,
				Root:       "sql",
			}
			cnf, err := config.Fetch()
			if err != nil {
				return
			}
			db, err := database.ConnectDB(cnf.DataSource.Dns)
			if err != nil {
				return
			}
			migrate.SetSchema("blnk")
			n, err := migrate.Exec(db, "postgres", migrations, migrate.Up)
			if err != nil {
				log.Printf("Error migrating up: %v", err)
			}
			fmt.Printf("Applied %d migrations!\n", n)
		},
	}

	return cmd
}

func migrateDownCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use: "down",
		Run: func(cmd *cobra.Command, args []string) {
			migrations := migrate.EmbedFileSystemMigrationSource{
				FileSystem: blnk.SQLFiles,
				Root:       "sql",
			}
			cnf, err := config.Fetch()
			if err != nil {
				return
			}
			db, err := database.ConnectDB(cnf.DataSource.Dns)
			if err != nil {
				log.Printf("Error migrating up: %v", err)
				return
			}
			n, err := migrate.Exec(db, "postgres", migrations, migrate.Down)
			if err != nil {
				log.Printf("Error migrating up: %v", err)
			}
			fmt.Printf("Applied %d migrations!\n", n)
		},
	}

	return cmd
}
