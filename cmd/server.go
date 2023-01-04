package main

import (
	"log"

	"github.com/jerry-enebeli/blnk/api"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/jerry-enebeli/blnk/ledger"
	"github.com/spf13/cobra"
)

func serverCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "start",
		Short: "start blnk server",
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
