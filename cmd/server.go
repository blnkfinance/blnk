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
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/blnkfinance/blnk"
	"github.com/blnkfinance/blnk/api"
	"github.com/blnkfinance/blnk/api/middleware"
	"github.com/blnkfinance/blnk/config"
	"github.com/blnkfinance/blnk/database"
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
// resolveCertStoragePath returns the configured certificate storage path,
// falling back to the default location when unset.
func resolveCertStoragePath(conf config.ServerConfig) string {
	if conf.CertStoragePath == "" {
		return "/var/lib/blnk/certs"
	}
	return conf.CertStoragePath
}

// resolveTLSDomains returns the certificate domains, defaulting to localhost
// when no domain is configured.
func resolveTLSDomains(conf config.ServerConfig) []string {
	if conf.Domain == "" {
		return []string{"localhost"}
	}
	return []string{conf.Domain}
}

func serveTLS(r *gin.Engine, conf config.ServerConfig) error {
	// Configure CertMagic's ACME (Automatic Certificate Management Environment) for automatic TLS
	certmagic.DefaultACME.Agreed = true      // Agree to ACME TOS
	certmagic.DefaultACME.Email = conf.Email // Set email for certificate recovery/notifications
	cfg := certmagic.NewDefault()

	cfg.Storage = &certmagic.FileStorage{Path: resolveCertStoragePath(conf)}

	// Define domain(s) for the certificate
	if conf.Domain == "" {
		logrus.Error("No domain specified, defaulting to localhost")
	}
	domains := resolveTLSDomains(conf)

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

	logrus.Errorf("Starting HTTPS server on %s\n", conf.Port)
	// Start the HTTPS server with automatic certificate management
	if err := server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
		logrus.Fatalf("Failed to start HTTPS server: %v", err)
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
	return getOrCreateHeartbeatIDAt("./heartbeat.db")
}

// getOrCreateHeartbeatIDAt persists a stable heartbeat UUID in a SQLite file
// at the given path, creating it on first use. Any storage failure falls
// back to a fresh (non-persistent) UUID.
func getOrCreateHeartbeatIDAt(path string) string {
	db, err := sql.Open("sqlite3", path)
	if err != nil {
		logrus.Errorf("Failed to open SQLite DB: %v", err)
		return uuid.New().String() // fallback to temp UUID
	}
	defer func() { _ = db.Close() }()

	_, err = db.Exec(`CREATE TABLE IF NOT EXISTS config (key TEXT PRIMARY KEY, value TEXT)`)
	if err != nil {
		logrus.Errorf("Failed to create config table: %v", err)
		return uuid.New().String()
	}

	var heartbeatID string
	err = db.QueryRow(`SELECT value FROM config WHERE key = 'heartbeat_id'`).Scan(&heartbeatID)
	if err == sql.ErrNoRows {
		heartbeatID = uuid.New().String()
		_, err = db.Exec(`INSERT INTO config (key, value) VALUES (?, ?)`, "heartbeat_id", heartbeatID)
		if err != nil {
			logrus.Errorf("Failed to insert heartbeat_id: %v", err)
		}
	} else if err != nil {
		logrus.Errorf("Failed to read heartbeat_id: %v", err)
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
				logrus.Errorf("Failed to send heartbeat: %v", err)
			}
		}
	}()
}

func healthCheckHandler(c *gin.Context) {
	cfg, err := config.Fetch()
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "DOWN", "reason": "config unavailable"})
		return
	}

	ds, err := database.GetDBConnection(cfg)
	if err != nil || ds == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "DOWN", "reason": "database unreachable"})
		return
	}

	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()

	if err := ds.Conn.PingContext(ctx); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"status": "DOWN", "reason": "database ping failed"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "UP"})
}

func initializeRouter(b *blnkInstance) *gin.Engine {
	router := api.NewAPI(b.blnk).Router()
	router.GET("/health", healthCheckHandler)
	if h := trace.MetricsHandler(); h != nil {
		cfg, _ := config.Fetch()
		var secure bool
		var token string
		if cfg != nil {
			secure = cfg.Server.Secure
			token = cfg.Server.MetricsBearerToken
		}
		router.GET("/metrics", middleware.MetricsAuth(secure, token), gin.WrapH(h))
	}
	return router
}

func initializeOpenTelemetry(ctx context.Context, monitoringDSN string) (func(context.Context) error, error) {
	shutdown, err := trace.SetupOTelSDK(ctx, "BLNK", monitoringDSN)
	if err != nil {
		return nil, fmt.Errorf("error setting up OTel SDK: %w", err)
	}
	return shutdown, nil
}

func initializeTypeSense(ctx context.Context, cfg *config.Configuration) (*search.TypesenseClient, error) {
	if cfg.TypeSense.Dns == "" {
		logrus.Warn("TypeSense DNS not configured. Search functionality will be disabled.")
		return nil, nil
	}

	newSearch := search.NewTypesenseClient(cfg.TypeSenseKey, []string{cfg.TypeSense.Dns})

	err := retryWithBackoff(ctx, 5, 2*time.Second, func() error {
		if err := newSearch.EnsureCollectionsExist(ctx); err != nil {
			return err
		}
		return migrateTypeSenseSchema(ctx, newSearch)
	})
	if err != nil {
		return nil, err
	}
	return newSearch, nil
}

// retryWithBackoff runs fn up to attempts times with exponential backoff
// starting at baseDelay. It stops early when the context is canceled and
// returns the last error when all attempts fail.
func retryWithBackoff(ctx context.Context, attempts int, baseDelay time.Duration, fn func() error) error {
	retryDelay := baseDelay
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}

		logrus.Errorf("TypeSense initialization failed (attempt %d/%d): %v. Retrying in %v...", i+1, attempts, err, retryDelay)

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(retryDelay):
			retryDelay *= 2
		}
	}
	return fmt.Errorf("failed to initialize TypeSense after %d attempts: %v", attempts, err)
}

func initializePostHog() (posthog.Client, string) {
	client, _ := posthog.NewWithConfig("phc_XbsHF5iBSnPiTA96gl7xygazrwBa0r2Ut4vEHoBHNiG",
		posthog.Config{Endpoint: "https://us.i.posthog.com"})
	heartbeatID := getOrCreateHeartbeatID()
	sendHeartbeat(client, heartbeatID)
	return client, heartbeatID
}

func startServer(router *gin.Engine, port string) error {
	server := newHTTPServer(router, port)

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
	return gracefulShutdown(server, quit, 30*time.Second)
}

// newHTTPServer builds the API HTTP server for the given router and port.
func newHTTPServer(router *gin.Engine, port string) *http.Server {
	return &http.Server{
		Addr:    ":" + port,
		Handler: router,
	}
}

// gracefulShutdown blocks until a signal arrives on quit, then shuts the
// server down, giving outstanding requests up to timeout to complete.
func gracefulShutdown(server *http.Server, quit <-chan os.Signal, timeout time.Duration) error {
	<-quit

	logrus.Info("Shutting down server...")

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
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
		tracingShutdown, err = initializeOpenTelemetry(ctx, cfg.RemoteMonitoringDSN())
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

			// Load configuration
			cfg, err := config.Fetch()
			if err != nil {
				logrus.Error(err)
			}

			// Initialize telemetry and observability before the router,
			// so MetricsHandler() is available when routes are registered.
			phClient, shutdown, err := initializeTelemetryAndObservability(ctx, cfg)
			if err != nil {
				logrus.Fatal(err)
			}
			if shutdown != nil {
				defer func() {
					if err := shutdown(ctx); err != nil {
						logrus.Errorf("Error during shutdown: %v", err)
					}
				}()
			}
			if phClient != nil {
				defer phClient.Close()
			}

			// Initialize router (after OTel so /metrics handler is available)
			router := initializeRouter(b)

			// Initialize TypeSense
			tsClient, err := initializeTypeSense(ctx, cfg)
			if err != nil {
				logrus.Errorf("TypeSense initialization error: %v", err)
			} else if tsClient != nil {
				search.TryReindexIfNeeded(ctx, tsClient, b.blnk.GetDataSource())
			}

			// Close database connection pool on shutdown
			defer func() {
				ds, err := database.GetDBConnection(cfg)
				if err == nil && ds != nil {
					logrus.Info("Closing database connection pool...")
					if err := ds.Close(); err != nil {
						logrus.Errorf("Error closing database connection: %v", err)
					}
				}
			}()

			// Start lineage outbox processor
			// This worker processes pending lineage entries that were captured atomically with transactions
			lineageProcessor := blnk.NewLineageOutboxProcessor(b.blnk)
			lineageProcessor.Start(ctx)
			defer lineageProcessor.Stop()

			// Start the hash-chain processor when enabled. It seals transactions
			// into a tamper-evident chain off the hot path.
			if cfg.Transaction.HashChain.Enabled {
				chainProcessor := blnk.NewChainProcessor(b.blnk)
				chainProcessor.Start(ctx)
				defer chainProcessor.Stop()
			}

			// Start server
			if err := startServer(router, cfg.Server.Port); err != nil {
				logrus.Fatal(err)
			}
		},
	}

	return cmd
}
