package main

import (
	"log"

	"github.com/jerry-enebeli/blnk"

	"github.com/jerry-enebeli/blnk/database"

	"github.com/jerry-enebeli/blnk/api"
	"github.com/jerry-enebeli/blnk/config"
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
			db, err := database.NewDataSource(cfg)
			if err != nil {
				log.Fatalf("Error getting datasource: %v\n", err)
			}
			newBlnk, err := blnk.NewBlnk(db)
			if err != nil {
				log.Fatalf("Error creating blnk: %v\n", err)
			}
			router := api.NewAPI(newBlnk).Router()
			err = router.Run(":" + cfg.Port)
			if err != nil {
				log.Fatal(err)
			}
		},
	}

	return cmd
}
