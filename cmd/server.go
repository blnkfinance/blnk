package main

import (
	"context"
	"log"
	"net/http"
	"time"

	pg_listener "github.com/jerry-enebeli/blnk/internal/pg-listener"

	"github.com/jerry-enebeli/blnk"

	"github.com/gin-gonic/gin"

	"github.com/caddyserver/certmagic"

	"github.com/jerry-enebeli/blnk/api"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/spf13/cobra"
)

func serveTLS(r *gin.Engine, conf config.ServerConfig) error {
	certmagic.DefaultACME.Agreed = true
	certmagic.DefaultACME.Email = conf.Email
	cfg := certmagic.NewDefault()
	cfg.Storage = &certmagic.FileStorage{Path: "path/to/certmagic/storage"}

	domains := []string{conf.Domain}
	if conf.Domain == "" {
		log.Println("No domain specified, defaulting to localhost")
		domains = []string{"localhost"} // Default or handle as needed
	}

	if err := cfg.ManageSync(context.Background(), domains); err != nil {
		return err
	}

	server := &http.Server{
		Addr:      ":" + conf.Port,
		Handler:   r,
		TLSConfig: cfg.TLSConfig(),
	}

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
			router := api.NewAPI(b.blnk).Router()
			cfg, err := config.Fetch()
			if err != nil {
				log.Fatal(err)
			}

			//todo fix exposed api key
			newSearch := blnk.NewTypesenseClient("blnk-api-key", []string{"http://typesense:8108"})
			listener := pg_listener.NewDBListener(pg_listener.ListenerConfig{
				PgConnStr: cfg.DataSource.Dns,
				Interval:  10 * time.Second,
				Timeout:   time.Minute,
			}, newSearch)
			go listener.Start()

			if cfg.Server.SSL {
				if err := serveTLS(router, cfg.Server); err != nil {
					log.Fatalf("Error setting up TLS: %v", err)
				}
			} else {
				log.Printf("Starting server on http://localhost:%s", cfg.Server.Port)
				if err := router.Run(":" + cfg.Server.Port); err != nil {
					log.Fatal(err)
				}
			}
		},
	}

	return cmd
}
