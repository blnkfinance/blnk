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
	"os"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/database"
	"github.com/jerry-enebeli/blnk/internal/notification"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

// Blnk represents the CLI application, encapsulating the root Cobra command.
type Blnk struct {
	cmd *cobra.Command // Root command for the CLI application
}

// blnkInstance holds the Blnk instance and its configuration.
// This is used to store the runtime instance and configuration globally within the application.
type blnkInstance struct {
	blnk *blnk.Blnk            // Blnk object initialized from configuration
	cnf  *config.Configuration // Configuration object holding runtime settings
}

// recoverPanic handles any panics during program execution and logs the error using Logrus.
func recoverPanic() {
	if rec := recover(); rec != nil {
		logrus.Error(rec) // Log the recovered panic
		os.Exit(1)        // Exit the program with an error status
	}
}

// preRun sets up the configuration and initializes the Blnk instance before running any command.
// It ensures that the configuration is loaded, and the Blnk instance is initialized properly.
func preRun(app *blnkInstance) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		// Initialize configuration from the specified configuration file.
		err := config.InitConfig("blnk.json")
		if err != nil {
			log.Fatal("error loading config", err)
		}

		// Fetch the configuration settings.
		cnf, err := config.Fetch()
		if err != nil {
			return err
		}

		// Initialize the Blnk instance using the fetched configuration.
		newBlnk, err := setupBlnk(cnf)
		if err != nil {
			notification.NotifyError(err) // Notify via the internal notification system
			log.Fatal(err)                // Log the fatal error
		}

		// Assign the new Blnk instance and configuration to the app struct.
		app.blnk = newBlnk
		app.cnf = cnf

		return nil
	}
}

// setupBlnk creates and initializes a new Blnk instance based on the provided configuration.
// It connects to the data source (such as a database) using the configuration settings.
func setupBlnk(cfg *config.Configuration) (*blnk.Blnk, error) {
	// Initialize a new data source from the configuration.
	db, err := database.NewDataSource(cfg)
	if err != nil {
		return &blnk.Blnk{}, fmt.Errorf("error getting datasource: %v", err)
	}

	// Create a new Blnk instance using the initialized data source.
	newBlnk, err := blnk.NewBlnk(db)
	if err != nil {
		return &blnk.Blnk{}, fmt.Errorf("error creating blnk: %v", err)
	}
	return newBlnk, nil
}

// NewCLI creates the command-line interface (CLI) for the Blnk application.
// It sets up the root command and subcommands like serverCommands, workerCommands, and migrateCommands.
func NewCLI() *Blnk {
	var configFile string // Configuration file path (defaults to ./blnk.json)
	b := &blnkInstance{}  // Instance of Blnk to be passed into commands

	// Define the root command with usage and description.
	var rootCmd = &cobra.Command{
		Use:   "blnk",
		Short: "Open source ledger",                       // Brief description for the CLI tool
		Run:   func(cmd *cobra.Command, args []string) {}, // Main function for the root command
	}

	// Add a persistent flag to the root command for specifying the config file.
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "./blnk.json", "Configuration file for wallet lite")

	// Set the persistent pre-run hook to initialize the app and config before executing any command.
	rootCmd.PersistentPreRunE = preRun(b)

	// Add various subcommands to the root command.
	rootCmd.AddCommand(serverCommands(b))  // Command for starting the server
	rootCmd.AddCommand(workerCommands(b))  // Command for worker processes
	rootCmd.AddCommand(migrateCommands(b)) // Command for database/schema migrations

	return &Blnk{cmd: rootCmd}
}

// executeCLI runs the root command, handling any errors that occur during execution.
// It serves as the main entry point for the CLI application.
func (w Blnk) executeCLI() {
	if err := w.cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err) // Print any errors that occur
		os.Exit(1)                   // Exit the program with an error status
	}
}

// main is the main function and the entry point for the application.
// It recovers from any panic, initializes the CLI, and executes it.
func main() {
	defer recoverPanic() // Ensure that any panic is handled gracefully

	cli := NewCLI()  // Create the CLI application
	cli.executeCLI() // Execute the CLI commands
}
