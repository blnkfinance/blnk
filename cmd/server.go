package main

import (
	"log"

	"github.com/sirupsen/logrus"

	"github.com/jerry-enebeli/blnk"

	"github.com/caddyserver/certmagic"

	"github.com/jerry-enebeli/blnk/api"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/spf13/cobra"
)

func serveTLS(conf config.ServerConfig) error {
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

func serverCommands(b *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start blnk server",
		Run: func(cmd *cobra.Command, args []string) {
			_, err := blnk.SetupOTelSDK(cmd.Context())
			if err != nil {
				return
			}
			router := api.NewAPI(b.blnk).Router()
			cfg, err := config.Fetch()
			if err != nil {
				log.Fatal(err)
			}
			if cfg.Server.SSL {
				err := serveTLS(cfg.Server)
				if err != nil {
					log.Fatalf("Error creating tls: %v\n", err)
				}
			}
			logrus.Infof("Blnk sever running on localhost:%s ✅", cfg.Server.Port)
			err = router.Run(":" + cfg.Server.Port)
			if err != nil {
				log.Fatal(err)
			}
		},
	}

	return cmd
}
