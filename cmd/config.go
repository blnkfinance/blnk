package main

import (
	"encoding/json"
	"fmt"
	"log"

	"github.com/jerry-enebeli/saifu/config"
	"github.com/spf13/cobra"
)

func configCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use:   "config",
		Short: "config outputs your instances computed configuration",
		Run: func(cmd *cobra.Command, args []string) {
			cfg, err := config.Fetch()
			if err != nil {
				log.Fatalf("Error getting config: %v\n", err)
			}

			data, err := json.MarshalIndent(cfg, "", "    ")
			if err != nil {
				log.Fatalf("Error printing config: %v\n", err)
			}

			fmt.Println(string(data))
		},
	}
	return cmd
}
