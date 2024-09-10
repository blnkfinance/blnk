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

type BackupManager struct {
	Config   *config.Configuration
	S3Client *s3.S3
}

func NewBackupManager() (*BackupManager, error) {
	cfg, err := config.Fetch()
	if err != nil {
		return nil, errors.Wrap(err, "failed to fetch config")
	}

	endpoint := cfg.S3Endpoint
	accessKeyID := cfg.AwsAccessKeyId
	secretAccessKey := cfg.AwsSecretAccessKey

	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(accessKeyID, secretAccessKey, ""),
		Endpoint:         aws.String(endpoint),
		Region:           aws.String(cfg.S3Region),
		DisableSSL:       aws.Bool(true),
		S3ForcePathStyle: aws.Bool(true),
	}

	newSession, err := session.NewSession(s3Config)
	if err != nil {
		log.Fatalf("Error creating session: %v", err)
	}

	return &BackupManager{
		Config:   cfg,
		S3Client: s3.New(newSession),
	}, nil
}

// BackupToDisk performs a database backup to the local disk
func (bm *BackupManager) BackupToDisk(ctx context.Context) (string, error) {
	log.Info("Starting database backup to disk")

	db, err := sql.Open("postgres", bm.Config.DataSource.Dns)
	if err != nil {
		return "", errors.Wrap(err, "failed to open database connection")
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return "", errors.Wrap(err, "failed to ping database")
	}

	parsedURL, err := url.Parse(bm.Config.DataSource.Dns)
	if err != nil {
		return "", errors.Wrap(err, "failed to parse database URL")
	}

	dbUser := parsedURL.User.Username()
	dbPassword, _ := parsedURL.User.Password()
	dbHost, dbPort, err := net.SplitHostPort(parsedURL.Host)
	if err != nil {
		return "", errors.Wrap(err, "failed to split host and port")
	}
	dbName := parsedURL.Path[1:] // Remove leading '/'

	backupDir := filepath.Join(bm.Config.BackupDir, time.Now().Format("2006-01-02"))
	log.Infof("Backup directory: %s", backupDir)

	if err := os.MkdirAll(backupDir, os.ModePerm); err != nil {
		return "", errors.Wrap(err, "failed to create backup directory")
	}

	log.Infof("Backup directory created: %s", backupDir)

	backupFile := filepath.Join(backupDir, fmt.Sprintf("%s-%s.sql", dbName, time.Now().Format("150405")))
	log.Infof("Backup file path: %s", backupFile)

	cmd := exec.CommandContext(ctx, "pg_dump", "-U", dbUser, "-d", dbName, "-f", backupFile)
	cmd.Env = append(os.Environ(),
		fmt.Sprintf("PGHOST=%s", dbHost),
		fmt.Sprintf("PGPORT=%s", dbPort),
		fmt.Sprintf("PGUSER=%s", dbUser),
		fmt.Sprintf("PGPASSWORD=%s", dbPassword),
	)

	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return "", errors.Wrapf(err, "pg_dump failed: %s", stderr.String())
	}

	log.Infof("Database backup completed: %s", backupFile)

	// Verify file existence
	if _, err := os.Stat(backupFile); os.IsNotExist(err) {
		return "", errors.New("backup file does not exist after pg_dump")
	}
	return backupFile, nil
}

// BackupToS3 uploads a backup file to S3
func (bm *BackupManager) BackupToS3(ctx context.Context) error {
	log.Info("Starting backup to S3")

	filePath, err := bm.BackupToDisk(ctx)
	if err != nil {
		return errors.Wrap(err, "failed to backup to disk")
	}

	file, err := os.Open(filePath)
	if err != nil {
		return errors.Wrap(err, "failed to open file for S3 upload")
	}
	defer file.Close()

	_, filename := filepath.Split(filePath)

	fileBytes, err := io.ReadAll(file)
	if err != nil {
		return errors.Wrap(err, "failed to open file for S3 upload")
	}

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
