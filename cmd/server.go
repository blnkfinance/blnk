package main

import (
	"log"

	"github.com/gin-gonic/gin"

	"github.com/caddyserver/certmagic"

	"github.com/jerry-enebeli/blnk"

	"github.com/jerry-enebeli/blnk/database"

	"github.com/jerry-enebeli/blnk/api"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/spf13/cobra"
)

func serveTLS(router *gin.Engine, conf config.ServerConfig) error {
	// read and agree to your CA's legal documents
	certmagic.DefaultACME.Agreed = true
	// provide an email address
	certmagic.DefaultACME.Email = conf.Email

	_, err := certmagic.Listen([]string{conf.Domain})
	if err != nil {
		return err
	}

	return nil

}

func serverCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start blnk server",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.Fetch()
			if err != nil {
				log.Fatalf("Error getting config: %v\n", err)
			}
			db, err := database.NewDataSource(cfg)
			if err != nil {
				log.Fatalf("Error getting datasource: %v\n", err)
			}
			newBlnk, err := blnk.NewBlnk(db)
			if err != nil {
				log.Fatalf("Error creating blnk: %v\n", err)
			}
			router := api.NewAPI(newBlnk).Router()

			if cfg.Server.SSL {
				err := serveTLS(router, cfg.Server)
				if err != nil {
					log.Fatalf("Error creating tls: %v\n", err)
				}
			}
			err = router.Run(":" + cfg.Server.Port)
			if err != nil {
				log.Fatal(err)
			}
		},
	}

	return cmd
}
