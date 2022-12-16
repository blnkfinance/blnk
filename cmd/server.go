package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/jerry-enebeli/saifu/config"
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
			log.Printf("server running on port %s", cfg.Port)
			err = http.ListenAndServe(fmt.Sprintf(":%s", cfg.Port), nil)
			if err != nil {
				panic(err)
			}

		},
	}

	return cmd
}
