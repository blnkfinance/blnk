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
	"context"
	"log"
	"net/http"

	"github.com/caddyserver/certmagic"
	"github.com/gin-gonic/gin"
	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/api"
	"github.com/jerry-enebeli/blnk/config"
	trace "github.com/jerry-enebeli/blnk/internal/traces"
	"github.com/spf13/cobra"
)

/*
serveTLS starts an HTTPS server with TLS enabled using CertMagic for automatic certificate management.
It accepts a gin.Engine instance as the router and a ServerConfig struct for server configurations.
If no domain is specified, the server will default to running on localhost.
*/
func serveTLS(r *gin.Engine, conf config.ServerConfig) error {
	// Configure CertMagic's ACME (Automatic Certificate Management Environment) for automatic TLS
	certmagic.DefaultACME.Agreed = true      // Agree to ACME TOS
	certmagic.DefaultACME.Email = conf.Email // Set email for certificate recovery/notifications
	cfg := certmagic.NewDefault()
	cfg.Storage = &certmagic.FileStorage{Path: "path/to/certmagic/storage"} // Define storage for certificates

	// Define domain(s) for the certificate
	domains := []string{conf.Domain}
	if conf.Domain == "" {
		log.Println("No domain specified, defaulting to localhost")
		domains = []string{"localhost"} // Use localhost if no domain is provided
	}

	// Manage TLS certificates for the specified domains
	if err := cfg.ManageSync(context.Background(), domains); err != nil {
		return err
	}

	// Create and configure the HTTPS server
	server := &http.Server{
		Addr:      ":" + conf.Port, // Server address and port
		Handler:   r,               // Handler for HTTP requests (gin router)
		TLSConfig: cfg.TLSConfig(), // TLS configuration from CertMagic
	}

	log.Printf("Starting HTTPS server on %s\n", conf.Port)
	// Start the HTTPS server with automatic certificate management
	if err := server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Failed to start HTTPS server: %v", err)
	}

	return nil
}

/*
migrateTypeSenseSchema ensures that the necessary TypeSense schema is migrated for all required collections.
It takes a TypesenseClient and a context as parameters.
This function loops through the predefined collections and migrates their schema in TypeSense.
*/
func migrateTypeSenseSchema(ctx context.Context, t *blnk.TypesenseClient) error {
	// Define the collections to migrate schema for
	collections := []string{"ledgers", "balances", "transactions", "identities", "reconciliations"}

	// Migrate schema for each collection
	for _, c := range collections {
		err := t.MigrateTypeSenseSchema(ctx, c)
		if err != nil {
			return err // Return if an error occurs during migration
		}
	}
	return nil
}

/*
serverCommands returns the Cobra command responsible for starting the Blnk server.
It sets up the API routes, traces, and TypeSense client before launching the server.
*/
func serverCommands(b *blnkInstance) *cobra.Command {
	// Define the `start` command for starting the server
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start blnk server", // Short description of the command
		Run: func(cmd *cobra.Command, args []string) {
			// Create a new API router using the instance's Blnk object
			router := api.NewAPI(b.blnk).Router()

			// Fetch server configuration from the config package
			cfg, err := config.Fetch()
			if err != nil {
				log.Fatal(err)
			}

			// Set up OpenTelemetry tracing with Blnk service instrumentation
			shutdown, err := trace.SetupOTelSDK(context.Background(), "BLNK")
			if err != nil {
				log.Fatalf("Error setting up OTel SDK: %v", err)
			}
			// Ensure proper shutdown of the tracing system when the function exits
			defer func() {
				if err := shutdown(context.Background()); err != nil {
					log.Printf("Error shutting down OTel SDK: %v", err)
				}
			}()

			// Create a new TypeSense client for managing search capabilities in Blnk
			newSearch := blnk.NewTypesenseClient("blnk-api-key", []string{cfg.TypeSense.Dns})

			// Ensure that all necessary collections exist in TypeSense
			err = newSearch.EnsureCollectionsExist(context.Background())
			if err != nil {
				log.Fatalf("Failed to ensure collections exist: %v", err)
			}

			// Migrate TypeSense schema for the necessary collections
			err = migrateTypeSenseSchema(context.Background(), newSearch)
			if err != nil {
				log.Printf("Failed to migrate typesense schema: %v", err)
			}

			// Check if SSL/TLS is enabled and start server accordingly
			if cfg.Server.SSL {
				// If SSL is enabled, start the server with TLS
				if err := serveTLS(router, cfg.Server); err != nil {
					log.Fatalf("Error setting up TLS: %v", err)
				}
			} else {
				// If SSL is not enabled, start a regular HTTP server
				log.Printf("Starting server on http://localhost:%s", cfg.Server.Port)
				if err := router.Run(":" + cfg.Server.Port); err != nil {
					log.Fatal(err)
				}
			}
		},
	}

	return cmd
}
