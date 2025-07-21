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
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/caddyserver/certmagic"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/api"
	"github.com/jerry-enebeli/blnk/config"
	trace "github.com/jerry-enebeli/blnk/internal/traces"
	_ "github.com/mattn/go-sqlite3"
	"github.com/posthog/posthog-go"
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
func getOrCreateHeartbeatID() string {
	db, err := sql.Open("sqlite3", "./heartbeat.db")
	if err != nil {
		log.Printf("Failed to open SQLite DB: %v", err)
		return uuid.New().String() // fallback to temp UUID
	}
	defer db.Close()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)`)
	if err != nil {
		log.Printf("Failed to create config table: %v", err)
		return uuid.New().String()
	}

	var heartbeatID string
	err = db.QueryRow(`SELECT value FROM config WHERE key = 'heartbeat_id'`).Scan(&heartbeatID)
	if err == sql.ErrNoRows {
		heartbeatID = uuid.New().String()
		_, err = db.Exec(`INSERT INTO config (key, value) VALUES (?, ?)`, "heartbeat_id", heartbeatID)
		if err != nil {
			log.Printf("Failed to insert heartbeat_id: %v", err)
		}
	} else if err != nil {
		log.Printf("Failed to read heartbeat_id: %v", err)
		return uuid.New().String()
	}

	return heartbeatID
}

// sendHeartbeat initializes and maintains a periodic heartbeat to PostHog
func sendHeartbeat(client posthog.Client, heartbeatID string) {
	ticker := time.NewTicker(5 * time.Minute)
	go func() {
		for range ticker.C {
			if err := client.Enqueue(posthog.Capture{
				DistinctId: heartbeatID,
				Event:      "server_heartbeat",
				Timestamp:  time.Now().UTC(),
				Properties: map[string]interface{}{
					"timestamp": time.Now().UTC(),
				},
			}); err != nil {
				log.Printf("Failed to send heartbeat: %v", err)
			}
		}
	}()
}

func healthCheckHandler(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "UP"})
}

func initializeRouter(b *blnkInstance) *gin.Engine {
	router := api.NewAPI(b.blnk).Router()
	router.GET("/health", healthCheckHandler) // Add health check route
	return router
}

func initializeOpenTelemetry(ctx context.Context) (func(context.Context) error, error) {
	shutdown, err := trace.SetupOTelSDK(ctx, "BLNK")
	if err != nil {
		return nil, fmt.Errorf("error setting up OTel SDK: %v", err)
	}
	return shutdown, nil
}

func initializeTypeSense(ctx context.Context, cfg *config.Configuration) (*blnk.TypesenseClient, error) {
	newSearch := blnk.NewTypesenseClient(cfg.TypeSenseKey, []string{cfg.TypeSense.Dns})
	if err := newSearch.EnsureCollectionsExist(ctx); err != nil {
		return nil, fmt.Errorf("failed to ensure collections exist: %v", err)
	}
	if err := migrateTypeSenseSchema(ctx, newSearch); err != nil {
		return nil, fmt.Errorf("failed to migrate typesense schema: %v", err)
	}
	return newSearch, nil
}

func initializePostHog() (posthog.Client, string) {
	client, _ := posthog.NewWithConfig("phc_XbsHF5iBSnPiTA96gl7xygazrwBa0r2Ut4vEHoBHNiG",
		posthog.Config{Endpoint: "https://us.i.posthog.com"})
	heartbeatID := getOrCreateHeartbeatID()
	sendHeartbeat(client, heartbeatID)
	return client, heartbeatID
}

func startServer(router *gin.Engine, cfg config.ServerConfig) error {
	if cfg.SSL {
		return serveTLS(router, cfg)
	}
	log.Printf("Starting server on http://localhost:%s", cfg.Port)
	return router.Run(":" + cfg.Port)
}

// Renamed from initializeObservability to better reflect its purpose
func initializeTelemetryAndObservability(ctx context.Context, cfg *config.Configuration) (posthog.Client, func(context.Context) error, error) {
	var phClient posthog.Client
	var tracingShutdown func(context.Context) error = func(context.Context) error { return nil }
	var err error

	// Initialize tracing if observability is enabled
	if cfg.EnableObservability {
		tracingShutdown, err = initializeOpenTelemetry(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("failed to initialize tracing: %w", err)
		}
	}

	// Initialize PostHog if telemetry is enabled
	if cfg.EnableTelemetry {
		phClient, _ = initializePostHog()
	}

	return phClient, tracingShutdown, nil
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
			ctx := context.Background()

			// Initialize router
			router := initializeRouter(b)

			// Load configuration
			cfg, err := config.Fetch()
			if err != nil {
				log.Println(err)
			}

			// Initialize telemetry and observability
			phClient, shutdown, err := initializeTelemetryAndObservability(ctx, cfg)
			if err != nil {
				log.Fatal(err)
			}
			if shutdown != nil {
				defer func() {
					if err := shutdown(ctx); err != nil {
						log.Printf("Error during shutdown: %v", err)
					}
				}()
			}
			if phClient != nil {
				defer phClient.Close()
			}

			// Initialize TypeSense
			_, err = initializeTypeSense(ctx, cfg)
			if err != nil {
				log.Printf("TypeSense initialization error: %v", err)
			}

			// Start server
			if err := startServer(router, cfg.Server); err != nil {
				log.Fatal(err)
			}
		},
	}

	return cmd
}
