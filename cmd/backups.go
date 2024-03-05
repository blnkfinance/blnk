package main

import (
	backups "github.com/jerry-enebeli/blnk/internal/pg-backups"

	"github.com/sirupsen/logrus"

	"github.com/spf13/cobra"
)

func backupCommands(b *blnkInstance) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "backup",
		Short: "start blnk database backup",
	}

	cmd.AddCommand(backupToCommands())
	cmd.AddCommand(backupToS3Commands())

	return cmd
}

func backupToCommands() *cobra.Command {
	cmd := &cobra.Command{
		Use: "drive",
		Run: func(cmd *cobra.Command, args []string) {
			err := backups.BackupDB()
			if err != nil {
				logrus.Error(err)
				return
			}
		},
	}

	return cmd
}

func backupToS3Commands() *cobra.Command {
	cmd := &cobra.Command{
		Use: "s3",
		Run: func(cmd *cobra.Command, args []string) {
			err := backups.ZipUploadToS3()
			if err != nil {
				logrus.Error(err)
				return
			}
		},
	}

	return cmd
}
