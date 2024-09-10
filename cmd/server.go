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

	"github.com/jerry-enebeli/blnk"

	"github.com/gin-gonic/gin"

	"github.com/caddyserver/certmagic"

	"github.com/jerry-enebeli/blnk/api"
	"github.com/jerry-enebeli/blnk/config"
	trace "github.com/jerry-enebeli/blnk/internal/traces"

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

func migrateTypeSenseSchema(ctx context.Context, t *blnk.TypesenseClient) error {
	collections := []string{"ledgers", "balances", "transactions", "identities", "reconciliations"}
	for _, c := range collections {
		err := t.MigrateTypeSenseSchema(ctx, c)
		if err != nil {
			return err
		}
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

			shutdown, err := trace.SetupOTelSDK(context.Background(), "BLNK")
			if err != nil {
				log.Fatalf("Error setting up OTel SDK: %v", err)
			}
			defer func() {
				if err := shutdown(context.Background()); err != nil {
					log.Printf("Error shutting down OTel SDK: %v", err)
				}
			}()

			newSearch := blnk.NewTypesenseClient("blnk-api-key", []string{cfg.TypeSense.Dns})
			err = newSearch.EnsureCollectionsExist(context.Background())
			if err != nil {
				log.Fatalf("Failed to ensure collections exist: %v", err)
			}
			err = migrateTypeSenseSchema(context.Background(), newSearch)
			if err != nil {
				log.Printf("Failed to migrate typesense schema: %v", err)
			}

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
