package main

import (
	"fmt"
	"log"
	"os"

	"github.com/sirupsen/logrus"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/spf13/cobra"
)

type Blnk struct {
	cmd *cobra.Command
}

func recoverPanic() {

	if rec := recover(); rec != nil {
		logrus.Error(rec)
		os.Exit(1)
	}
}

func preRun(rootcmd *cobra.Command, args []string) {
	filePath, err := rootcmd.Flags().GetString("config")
	if err != nil {
		log.Println(err)
	}
	err = config.InitConfig(filePath)
	if err != nil {
		log.Fatal("error loading config", err)
	}
	log.Println("config loaded ✅")
}

func NewCLI() *Blnk {
	var configFile string

	var rootCmd = &cobra.Command{
		Use:   "blnk",
		Short: "Open source wallet as a service",
		Run:   func(cmd *cobra.Command, args []string) {},
	}
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "./blnk.json", "Configuration file for wallet lite")
	rootCmd.PersistentPreRun = preRun
	rootCmd.AddCommand(serverCommands())
	rootCmd.AddCommand(workerCommands())
	return &Blnk{cmd: rootCmd}
}

func (w Blnk) executeCLI() {
	if err := w.cmd.Execute(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func main() {
	defer recoverPanic()
	cli := NewCLI()
	cli.executeCLI()
	_, err := config.Fetch()
	if err != nil {
		log.Println(err)
	}

}
