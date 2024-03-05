package backups

import (
	"archive/zip"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

func BackupDB() error {
	dbName := "blnk"
	dbUser := "postgres"
	dbHost := "localhost"
	dbPort := "5432"

	// Format today's date as YYYY-MM-DD
	today := time.Now().Format("2006-01-02")
	currentTime := time.Now().Format("150405") // HHMMSS format
	backupDir := fmt.Sprintf("./backups/%s", today)

	// Create a directory for today's date if it doesn't exist
	if _, err := os.Stat(backupDir); os.IsNotExist(err) {
		err := os.Mkdir(backupDir, os.ModePerm)
		if err != nil {
			return err
		}
	}

	// Define the backup file path
	backupFilePath := fmt.Sprintf("%s/blnk-%s-backup.sql", backupDir, currentTime)

	// Construct the pg_dump command
	cmd := exec.Command("pg_dump", "-U", dbUser, "-d", dbName, "-f", backupFilePath)
	cmd.Env = append(os.Environ(), "PGHOST="+dbHost, "PGPORT="+dbPort)

	// Execute the pg_dump command
	if err := cmd.Run(); err != nil {
		fmt.Fprintf(os.Stderr, "pg_dump failed: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Backup successful: %s\n", backupFilePath)
	return nil
}

func ZipUploadToS3() error {
	yesterday := time.Now().Format("2006-01-02")
	dirToZip := "./backups/" + yesterday
	zipFile := yesterday + ".zip"

	// Zip the directory
	if err := zipDir(dirToZip, zipFile); err != nil {
		return err
	}

	// Upload to S3
	if err := uploadToS3(zipFile, "your_s3_bucket_name", zipFile); err != nil {
		return err
	}

	// Optional: Remove the zip file after upload
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

func uploadToS3(filePath, bucketName, itemKey string) error {
	cfg, err := awsconfig.LoadDefaultConfig(context.TODO())
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
