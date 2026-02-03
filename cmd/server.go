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
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/internal/search"
	trace "github.com/blnkfinance/blnk/internal/traces"
	"github.com/caddyserver/certmagic"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	_ "github.com/mattn/go-sqlite3"
	"github.com/posthog/posthog-go"
	"github.com/sirupsen/logrus"
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

	certPath := conf.CertStoragePath
	if certPath == "" {
		certPath = "/var/lib/blnk/certs"
	}
	cfg.Storage = &certmagic.FileStorage{Path: certPath}

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
func migrateTypeSenseSchema(ctx context.Context, t *search.TypesenseClient) error {
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
	defer func() { _ = db.Close() }()

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

func initializeTypeSense(ctx context.Context, cfg *config.Configuration) (*search.TypesenseClient, error) {
	if cfg.TypeSense.Dns == "" {
		log.Println("TypeSense DNS not configured. Search functionality will be disabled.")
		return nil, nil
	}

	newSearch := search.NewTypesenseClient(cfg.TypeSenseKey, []string{cfg.TypeSense.Dns})

	const maxRetries = 5
	var retryDelay = 2 * time.Second
	var err error

	for i := 0; i < maxRetries; i++ {
		if err = newSearch.EnsureCollectionsExist(ctx); err == nil {
			if err = migrateTypeSenseSchema(ctx, newSearch); err == nil {
				return newSearch, nil
			}
		}

		log.Printf("TypeSense initialization failed (attempt %d/%d): %v. Retrying in %v...", i+1, maxRetries, err, retryDelay)

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(retryDelay):
			retryDelay *= 2
		}
	}

	return nil, fmt.Errorf("failed to initialize TypeSense after %d attempts: %v", maxRetries, err)
}

func initializePostHog() (posthog.Client, string) {
	client, _ := posthog.NewWithConfig("phc_XbsHF5iBSnPiTA96gl7xygazrwBa0r2Ut4vEHoBHNiG",
		posthog.Config{Endpoint: "https://us.i.posthog.com"})
	heartbeatID := getOrCreateHeartbeatID()
	sendHeartbeat(client, heartbeatID)
	return client, heartbeatID
}

func startServer(router *gin.Engine, port string) error {
	server := &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}

	// Start server in goroutine
	go func() {
		logrus.Infof("Server started on port %s", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logrus.Fatalf("Server error: %v", err)
		}
	}()

	// Wait for interrupt signal
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit

	logrus.Info("Shutting down server...")

	// Give outstanding requests 30 seconds to complete
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := server.Shutdown(ctx); err != nil {
		logrus.Errorf("Server forced to shutdown: %v", err)
		return err
	}

	logrus.Info("Server exited gracefully")
	return nil
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

			// Start lineage outbox processor
			// This worker processes pending lineage entries that were captured atomically with transactions
			lineageProcessor := blnk.NewLineageOutboxProcessor(b.blnk)
			lineageProcessor.Start(ctx)
			defer lineageProcessor.Stop()

			// Start server
			if err := startServer(router, cfg.Server.Port); err != nil {
				log.Fatal(err)
			}
		},
	}

	return cmd
}
