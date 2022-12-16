package main

import (
	"log"

	"github.com/spf13/cobra"
)

func walletCommands() *cobra.Command {
	var id string

	walletCmd := &cobra.Command{
		Use:   "wallet create",
		Short: "manage wallets",
		Run: func(cmd *cobra.Command, args []string) {
			log.Println(args, id)
		},
	}
	walletCmd.Flags().StringVar(&id, "id", "id", "external identify for a new wallet")

	return walletCmd
}
