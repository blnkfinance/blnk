package main

import (
	"log"

	"github.com/jerry-enebeli/saifu/api"
	"github.com/jerry-enebeli/saifu/config"
	"github.com/jerry-enebeli/saifu/ledger"
	"github.com/spf13/cobra"
)

func serverCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start wallet server",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.Fetch()
			if err != nil {
				log.Fatalf("Error getting config: %v\n", err)
			}

			api := api.NewAPI(ledger.NewLedger()).Router()

			api.Run(":" + cfg.Port)
		},
	}

	return cmd
}
