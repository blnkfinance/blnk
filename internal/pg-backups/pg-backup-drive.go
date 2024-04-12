package backups

import (
	"archive/zip"
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

	"github.com/aws/aws-sdk-go-v2/credentials"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func BackupDB() error {
	conf, err := config.Fetch()
	if err != nil {
		return err
	}

	db, err := sql.Open("postgres", conf.DataSource.Dns)
	if err != nil {
		return err
	}
	defer db.Close()

	if err := db.Ping(); err != nil {
		return err
	}

	var dbSize string
	err = db.QueryRow("SELECT pg_size_pretty(pg_database_size(current_database()))").Scan(&dbSize)
	if err != nil {
		return err
	}

	var largestTable string
	var largestTableSize string
	err = db.QueryRow(`
		SELECT relname, pg_size_pretty(pg_relation_size(relid))
		FROM pg_catalog.pg_statio_user_tables 
		ORDER BY pg_relation_size(relid) DESC 
		LIMIT 1`).Scan(&largestTable, &largestTableSize)
	if err != nil {
		return err
	}

	fmt.Printf("Database size: %s\n", dbSize)
	fmt.Printf("Largest table: %s, Size: %s\n", largestTable, largestTableSize)

	// Format today's date as YYYY-MM-DD
	today := time.Now().Format("2006-01-02")
	currentTime := time.Now().Format("150405") // HHMMSS format
	backupDir := fmt.Sprintf("./%s/%s", conf.BackupDir, today)

	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		err := os.Mkdir(backupDir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	// Parse the DNS URL to extract details
	parsedURL, err := url.Parse(conf.DataSource.Dns)
	if err != nil {
		return err
	}

	dbUser := parsedURL.User.Username()
	dbPassword, _ := parsedURL.User.Password()
	dbHost, dbPort, err := net.SplitHostPort(parsedURL.Host)
	if err != nil {
		return err
	}
	dbName := "blnk"
	backupFilePath := fmt.Sprintf("%s/blnk-%s-backup.sql", backupDir, currentTime)
	cmd := exec.Command("pg_dump", "-U", dbUser, "-d", dbName, "-f", backupFilePath)
	cmd.Env = append(os.Environ(), "PGHOST="+dbHost, "PGPORT="+dbPort, "PGUSER="+dbUser, "PGPASSWORD="+dbPassword)

	// Execute the pg_dump command
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "pg_dump failed: %v\n", err)
		fmt.Fprintf(os.Stderr, "pg_dump stderr: %v\n", stderr.String())
		return err
	}

	fmt.Printf("Backup successful: %s\n", backupFilePath)
	return nil
}

func ZipUploadToS3() error {
	cnf, err := config.Fetch()
	if err != nil {
		return err
	}
	yesterday := time.Now().Format("2006-01-02")
	dirToZip := "./backups/" + yesterday
	zipFile := yesterday + ".zip"

	if err := zipDir(dirToZip, zipFile); err != nil {
		return err
	}

	if err := uploadToS3(zipFile, cnf.S3BucketName, zipFile, cnf.AwsAccessKeyId, cnf.AwsSecretAccessKey, cnf.S3Region); err != nil {
		return err
	}

	if err := os.Remove(zipFile); err != nil {
		return err
	}

	fmt.Println("Backup for", yesterday, "zipped and uploaded to S3.")

	return nil
}

func zipDir(srcDir, destZip string) error {
	zipFile, err := os.Create(destZip)
	if err != nil {
		return err
	}
	defer zipFile.Close()

	writer := zip.NewWriter(zipFile)
	defer writer.Close()

	return filepath.Walk(srcDir, func(filePath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		if info.IsDir() {
			return nil
		}

		relPath := filePath[len(srcDir)+1:]
		zipFileWriter, err := writer.Create(relPath)
		if err != nil {
			return err
		}

		srcFile, err := os.Open(filePath)
		if err != nil {
			return err
		}
		defer srcFile.Close()

		_, err = io.Copy(zipFileWriter, srcFile)
		return err
	})
}

func uploadToS3(filePath, bucketName, itemKey, accessKeyID, secretAccessKey, region string) error {
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO(),
		awsconfig.WithRegion(region),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(accessKeyID, secretAccessKey, "")),
	)
	if err != nil {
		return err
	}

	client := s3.NewFromConfig(cfg)

	file, err := os.Open(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = client.PutObject(context.TODO(), &s3.PutObjectInput{
		Bucket: aws.String(bucketName),
		Key:    aws.String(itemKey),
		Body:   file,
	})

	return err
}
