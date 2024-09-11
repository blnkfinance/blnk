/*
Copyright 2024 Blnk Finance Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

	http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package backups

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"io"
	"net"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/jerry-enebeli/blnk/config"
	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"
)

var (
	log = logrus.New()
)

// BackupManager manages database backups, supporting both local disk and S3 storage.
type BackupManager struct {
	Config   *config.Configuration // Configuration settings for backup operations
	S3Client *s3.S3                // AWS S3 client for uploading backups to S3
}

// NewBackupManager initializes a BackupManager with configurations for local and S3 storage.
// It returns a BackupManager or an error if the configuration or AWS session creation fails.
//
// Returns:
// - *BackupManager: A new instance of BackupManager with initialized configuration and S3 client.
// - error: An error if configuration fetching or S3 session initialization fails.
func NewBackupManager() (*BackupManager, error) {
	// Fetch configuration from the environment or configuration files.
	cfg, err := config.Fetch()
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch config")
	}

	// Prepare AWS S3 configuration with access credentials and endpoint.
	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(cfg.AwsAccessKeyId, cfg.AwsSecretAccessKey, ""),
		Endpoint:         aws.String(cfg.S3Endpoint),
		Region:           aws.String(cfg.S3Region),
		DisableSSL:       aws.Bool(true), // Disable SSL to use HTTP
		S3ForcePathStyle: aws.Bool(true), // Force path style for certain S3-compatible services
	}

	// Create a new AWS session.
	newSession, err := session.NewSession(s3Config)
	if err != nil {
		log.Fatalf("Error creating session: %v", err)
	}

	return &BackupManager{
		Config:   cfg,
		S3Client: s3.New(newSession),
	}, nil
}

// BackupToDisk creates a backup of the PostgreSQL database and stores it locally on disk.
// It creates a directory based on the current date and stores the backup as a SQL file.
//
// Parameters:
// - ctx context.Context: The context for managing cancellation and deadlines.
//
// Returns:
// - string: The path to the backup file.
// - error: An error if the backup process fails.
func (bm *BackupManager) BackupToDisk(ctx context.Context) (string, error) {
	log.Info("Starting database backup to disk")

	// Open a connection to the database using the provided DSN (Data Source Name).
	db, err := sql.Open("postgres", bm.Config.DataSource.Dns)
	if err != nil {
		return "", errors.Wrap(err, "failed to open database connection")
	}
	defer db.Close()

	// Ensure the database connection is working.
	if err := db.PingContext(ctx); err != nil {
		return "", errors.Wrap(err, "failed to ping database")
	}

	// Parse the DSN to retrieve individual database components like host, port, user, etc.
	parsedURL, err := url.Parse(bm.Config.DataSource.Dns)
	if err != nil {
		return "", errors.Wrap(err, "failed to parse database URL")
	}

	dbUser := parsedURL.User.Username()                      // Extract the database user
	dbPassword, _ := parsedURL.User.Password()               // Extract the database password (if provided)
	dbHost, dbPort, err := net.SplitHostPort(parsedURL.Host) // Extract host and port
	if err != nil {
		return "", errors.Wrap(err, "failed to split host and port")
	}
	dbName := parsedURL.Path[1:] // Remove leading '/' from the database name

	// Define the backup directory using the current date to create a unique directory.
	backupDir := filepath.Join(bm.Config.BackupDir, time.Now().Format("2006-01-02"))
	log.Infof("Backup directory: %s", backupDir)

	// Create the backup directory if it doesn't exist.
	if err := os.MkdirAll(backupDir, os.ModePerm); err != nil {
		return "", errors.Wrap(err, "failed to create backup directory")
	}

	// Define the backup file name using the database name and current time.
	backupFile := filepath.Join(backupDir, fmt.Sprintf("%s-%s.sql", dbName, time.Now().Format("150405")))
	log.Infof("Backup file path: %s", backupFile)

	// Execute the pg_dump command to create a database backup.
	cmd := exec.CommandContext(ctx, "pg_dump", "-U", dbUser, "-d", dbName, "-f", backupFile)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("PGHOST=%s", dbHost),
		fmt.Sprintf("PGPORT=%s", dbPort),
		fmt.Sprintf("PGUSER=%s", dbUser),
		fmt.Sprintf("PGPASSWORD=%s", dbPassword),
	)

	// Capture any error output from the pg_dump command.
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", errors.Wrapf(err, "pg_dump failed: %s", stderr.String())
	}

	log.Infof("Database backup completed: %s", backupFile)

	// Ensure that the backup file was created successfully.
	if _, err := os.Stat(backupFile); os.IsNotExist(err) {
		return "", errors.New("backup file does not exist after pg_dump")
	}
	return backupFile, nil
}

// BackupToS3 performs a database backup and uploads the resulting file to an S3 bucket.
// It first calls BackupToDisk to create a local backup, then uploads the file to S3.
//
// Parameters:
// - ctx context.Context: The context for managing cancellation and deadlines.
//
// Returns:
// - error: An error if the backup process or upload to S3 fails.
func (bm *BackupManager) BackupToS3(ctx context.Context) error {
	log.Info("Starting backup to S3")

	// Perform the backup to disk and get the file path.
	filePath, err := bm.BackupToDisk(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to backup to disk")
	}

	// Open the backup file for reading.
	file, err := os.Open(filePath)
	if err != nil {
		return errors.Wrap(err, "failed to open file for S3 upload")
	}
	defer file.Close()

	_, filename := filepath.Split(filePath) // Get the filename from the full path

	// Read the file contents into memory.
	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return errors.Wrap(err, "failed to read file for S3 upload")
	}

	// Upload the file to S3.
	_, err = bm.S3Client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket: aws.String(bm.Config.S3BucketName),
		Key:    aws.String(filename),
		Body:   bytes.NewReader(fileBytes),
	})

	if err != nil {
		return errors.Wrap(err, "failed to upload to S3")
	}

	log.Infof("Backup uploaded to S3: %s", filename)
	return nil
}
