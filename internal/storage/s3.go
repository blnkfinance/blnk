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

// Package storage provides thin, reusable cloud-storage helpers. The S3 helper
// here mirrors the credential/endpoint handling used by the pg-backup manager
// (internal/pg-backups/pg-backup-drive.go) so that LocalStack, MinIO, and real
// AWS S3 all work through the same code path.
package storage

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/blnkfinance/blnk/config"
)

// S3 wraps an AWS S3 client built from a Blnk configuration. The same struct
// is shared by the reconciliation exporter and any future consumer that needs
// to upload to or presign downloads from the configured bucket.
type S3 struct {
	client *s3.S3
	cfg    *config.Configuration
}

// NewS3Client builds an S3 client from configuration. It uses the same
// disableSSL / S3ForcePathStyle logic as the pg-backup manager so that
// LocalStack (http://localhost:4566) and MinIO work without further changes.
//
// Parameters:
// - cfg: the Blnk configuration holding AWS credentials, region, endpoint, and bucket.
//
// Returns:
// - *S3: a wrapper around the constructed *s3.S3 client.
// - error: a non-nil error if the AWS session cannot be created.
func NewS3Client(cfg *config.Configuration) (*S3, error) {
	if cfg == nil {
		return nil, fmt.Errorf("storage: nil configuration")
	}

	// Plaintext HTTP only when the endpoint explicitly asks for it (e.g. a
	// local MinIO or LocalStack in development); default to TLS for real S3.
	disableSSL := strings.HasPrefix(strings.ToLower(cfg.S3Endpoint), "http://")

	s3Config := &aws.Config{
		Credentials:      credentials.NewStaticCredentials(cfg.AwsAccessKeyId, cfg.AwsSecretAccessKey, ""),
		Endpoint:         aws.String(cfg.S3Endpoint),
		Region:           aws.String(cfg.S3Region),
		DisableSSL:       aws.Bool(disableSSL),
		S3ForcePathStyle: aws.Bool(true),
	}

	sess, err := session.NewSession(s3Config)
	if err != nil {
		return nil, fmt.Errorf("storage: failed to create AWS session: %w", err)
	}

	return &S3{client: s3.New(sess), cfg: cfg}, nil
}

// IsConfigured reports whether the configuration carries the minimum S3
// credentials and target to attempt an upload or presign a URL. Bucket, access
// key, secret, and region must all be non-empty. The endpoint is intentionally
// not required so that real AWS S3 users who leave S3Endpoint blank are still
// treated as configured.
func IsConfigured(cfg *config.Configuration) bool {
	if cfg == nil {
		return false
	}
	return cfg.S3BucketName != "" &&
		cfg.AwsAccessKeyId != "" &&
		cfg.AwsSecretAccessKey != "" &&
		cfg.S3Region != ""
}

// Upload writes data to the named bucket/key with the given content type. The
// body is buffered in memory; callers should keep exports small or stream
// themselves before invoking this helper.
//
// Parameters:
// - ctx: context for cancellation and tracing.
// - bucket: the target S3 bucket.
// - key: the target S3 object key.
// - data: the bytes to upload.
// - contentType: the value to set on the object's Content-Type header.
//
// Returns:
// - error: a non-nil error if PutObject fails.
func (s *S3) Upload(ctx context.Context, bucket, key string, data []byte, contentType string) error {
	_, err := s.client.PutObjectWithContext(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String(contentType),
	})
	if err != nil {
		return fmt.Errorf("storage: failed to upload %s/%s: %w", bucket, key, err)
	}
	return nil
}

// CreateBucket creates an S3 bucket in the configured region. It exists for
// test setup and for callers that want to provision the bucket before the
// first upload; production reconciliations rely on a pre-provisioned bucket
// rather than creating one per run.
//
// Parameters:
// - ctx: context for cancellation and tracing.
// - bucket: the bucket name to create.
//
// Returns:
// - error: a non-nil error if CreateBucket fails.
func (s *S3) CreateBucket(ctx context.Context, bucket string) error {
	_, err := s.client.CreateBucketWithContext(ctx, &s3.CreateBucketInput{
		Bucket: aws.String(bucket),
	})
	if err != nil {
		return fmt.Errorf("storage: failed to create bucket %s: %w", bucket, err)
	}
	return nil
}

// PresignGet returns a presigned GET URL for the named bucket/key valid for
// the given expiry. The URL embeds the configured endpoint, so callers using
// LocalStack will receive a LocalStack URL.
//
// Parameters:
// - bucket: the target S3 bucket.
// - key: the target S3 object key.
// - expiry: how long the presigned URL should remain valid.
//
// Returns:
// - string: the presigned URL.
// - error: a non-nil error if presigning fails.
func (s *S3) PresignGet(bucket, key string, expiry time.Duration) (string, error) {
	req, _ := s.client.GetObjectRequest(&s3.GetObjectInput{
		Bucket: aws.String(bucket),
		Key:    aws.String(key),
	})
	url, err := req.Presign(expiry)
	if err != nil {
		return "", fmt.Errorf("storage: failed to presign %s/%s: %w", bucket, key, err)
	}
	return url, nil
}

// BuildExportKey constructs the S3 object key for a reconciliation export
// part. The format is reconciliations/<YYYY-MM-DD>/<id>-<part>.<ext>, matching
// the convention chosen for the reconciliation export feature. Extracting this
// into a pure function keeps the key format unit-testable without an S3 mock.
//
// Parameters:
// - date: the upload date (typically time.Now()).
// - id: the random identifier shared by the matched and unmatched objects.
// - part: either "matched" or "unmatched".
// - ext: the export file extension (e.g. "json" or "csv").
//
// Returns:
// - string: the full S3 object key.
func BuildExportKey(date time.Time, id string, part string, ext string) string {
	return fmt.Sprintf("reconciliations/%s/%s-%s.%s", date.Format("2006-01-02"), id, part, ext)
}
