package main

import (
	"context"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"

	"github.com/caddyserver/certmagic"

	"github.com/jerry-enebeli/blnk/api"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/spf13/cobra"
)

func serveTLS(r *gin.Engine, conf config.ServerConfig) error {
	// Agree to CA's legal documents and set email for ACME account
	certmagic.DefaultACME.Agreed = true
	certmagic.DefaultACME.Email = conf.Email

	// Configure certmagic for automatic HTTPS
	cfg := certmagic.NewDefault()
	cfg.Storage = &certmagic.FileStorage{Path: "path/to/certmagic/storage"}

	// Handle domains
	domains := []string{conf.Domain}
	if conf.Domain == "" {
		log.Println("No domain specified, defaulting to localhost")
		domains = []string{"localhost"} // Default or handle as needed
	}

	// Prepare certmagic to manage the domains
	if err := cfg.ManageSync(context.Background(), domains); err != nil {
		return err
	}

	// Create a standard net/http server and configure it to use certmagic's TLSConfig
	server := &http.Server{
		Addr:      ":" + conf.Port,
		Handler:   r,               // Your Gin router
		TLSConfig: cfg.TLSConfig(), // Let certmagic handle the TLS configuration
	}

	// Start serving HTTPS
	log.Printf("Starting HTTPS server on %s\n", conf.Port)
	if err := server.ListenAndServeTLS("", ""); err != nil && err != http.ErrServerClosed {
		log.Fatalf("Failed to start HTTPS server: %v", err)
	}

	return nil
}

func serverCommands(b *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start blnk server",
		Run: func(cmd *cobra.Command, args []string) {
			r := api.NewAPI(b.blnk).Router()
			cfg, err := config.Fetch()
			if err != nil {
				log.Fatal(err)
			}

			if cfg.Server.SSL {
				if err := serveTLS(r, cfg.Server); err != nil {
					log.Fatalf("Error setting up TLS: %v", err)
				}
			} else {
				log.Printf("Starting server on http://localhost:%s", cfg.Server.Port)
				if err := r.Run(":" + cfg.Server.Port); err != nil {
					log.Fatal(err)
				}
			}
		},
	}

	return cmd
}
