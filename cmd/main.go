package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jerry-enebeli/saifu/config"
	"github.com/spf13/cobra"
)

type wLite struct {
	cmd *cobra.Command
}

func preRun(rootcmd *cobra.Command, args []string) {
	filePath, err := rootcmd.Flags().GetString("config")
	if err != nil {
		log.Println(err)
	}
	err = config.InitConfig(filePath)
	if err == nil {
		log.Println("config loaded ✅")
	}
	//wallets.NewWalletService()
}

func NewCLI() *wLite {
	var configFile string

	var rootCmd = &cobra.Command{
		Use:   "saifu",
		Short: "Open source wallet as a service",
		Run:   func(cmd *cobra.Command, args []string) {},
	}
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "./saifu.json", "Configuration file for wallet lite")
	rootCmd.PersistentPreRun = preRun
	rootCmd.AddCommand(configCommands())
	rootCmd.AddCommand(serverCommands())
	rootCmd.AddCommand(walletCommands())
	return &wLite{cmd: rootCmd}
}

func (w wLite) executeCLI() {
	if err := w.cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {
	cli := NewCLI()
	cli.executeCLI()
	_, err := config.Fetch()
	if err != nil {
		log.Println(err)
	}
}
