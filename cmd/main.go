package main

import (
	"fmt"
	"log"
	"os"

	"github.com/jerry-enebeli/blnk/internal/notification"

	"github.com/jerry-enebeli/blnk"
	"github.com/jerry-enebeli/blnk/database"
	"github.com/sirupsen/logrus"

	"github.com/jerry-enebeli/blnk/config"
	"github.com/spf13/cobra"
)

type Blnk struct {
	cmd *cobra.Command
}

type blnkInstance struct {
	blnk *blnk.Blnk
	cnf  *config.Configuration
}

func recoverPanic() {
	if rec := recover(); rec != nil {
		logrus.Error(rec)
		os.Exit(1)
	}
}

func preRun(app *blnkInstance) func(cmd *cobra.Command, args []string) error {
	return func(cmd *cobra.Command, args []string) error {
		err := config.InitConfig("blnk.json")
		if err != nil {
			log.Fatal("error loading config", err)
		}

		cnf, err := config.Fetch()
		if err != nil {
			return err
		}
		err = config.SetGrafanaExporterEnvs()
		if err != nil {
			return err
		}
		newBlnk, err := setupBlnk(cnf)
		if err != nil {
			notification.NotifyError(err)
			log.Fatal(err)
		}

		app.blnk = newBlnk
		app.cnf = cnf

		return nil
	}
}

func setupBlnk(cfg *config.Configuration) (*blnk.Blnk, error) {
	db, err := database.NewDataSource(cfg)
	if err != nil {
		return &blnk.Blnk{}, fmt.Errorf("error getting datasource: %v", err)
	}
	newBlnk, err := blnk.NewBlnk(db)
	if err != nil {
		return &blnk.Blnk{}, fmt.Errorf("error creating blnk: %v", err)
	}
	return newBlnk, nil
}

func NewCLI() *Blnk {
	var configFile string
	b := &blnkInstance{}

	var rootCmd = &cobra.Command{
		Use:   "blnk",
		Short: "Open source ledger",
		Run:   func(cmd *cobra.Command, args []string) {},
	}
	rootCmd.PersistentFlags().StringVar(&configFile, "config", "./blnk.json", "Configuration file for wallet lite")
	rootCmd.PersistentPreRunE = preRun(b)

	rootCmd.AddCommand(serverCommands(b))
	rootCmd.AddCommand(workerCommands())
	rootCmd.AddCommand(migrateCommands(b))
	rootCmd.AddCommand(backupCommands(b))
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
}
