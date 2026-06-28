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

package storage

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/blnkfinance/blnk/config"
	"github.com/stretchr/testify/assert"
)

func TestIsConfigured(t *testing.T) {
	cases := []struct {
		name string
		cfg  *config.Configuration
		want bool
	}{
		{
			name: "nil configuration",
			cfg:  nil,
			want: false,
		},
		{
			name: "all fields empty",
			cfg:  &config.Configuration{},
			want: false,
		},
		{
			name: "only bucket set",
			cfg:  &config.Configuration{S3BucketName: "b"},
			want: false,
		},
		{
			name: "bucket and access key",
			cfg:  &config.Configuration{S3BucketName: "b", AwsAccessKeyId: "a"},
			want: false,
		},
		{
			name: "missing region",
			cfg: &config.Configuration{
				S3BucketName:       "b",
				AwsAccessKeyId:     "a",
				AwsSecretAccessKey: "s",
			},
			want: false,
		},
		{
			name: "all required fields set, endpoint blank",
			cfg: &config.Configuration{
				S3BucketName:       "b",
				AwsAccessKeyId:     "a",
				AwsSecretAccessKey: "s",
				S3Region:           "us-east-1",
			},
			want: true,
		},
		{
			name: "all required fields set including endpoint",
			cfg: &config.Configuration{
				S3BucketName:       "b",
				AwsAccessKeyId:     "a",
				AwsSecretAccessKey: "s",
				S3Region:           "us-east-1",
				S3Endpoint:         "http://localhost:4566",
			},
			want: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, IsConfigured(tc.cfg))
		})
	}
}

func TestBuildExportKey(t *testing.T) {
	date := time.Date(2026, 6, 27, 12, 0, 0, 0, time.UTC)
	id := "abc123"

	cases := []struct {
		name string
		part string
		ext  string
		want string
	}{
		{
			name: "json matched",
			part: "matched",
			ext:  "json",
			want: "reconciliations/2026-06-27/abc123-matched.json",
		},
		{
			name: "json unmatched",
			part: "unmatched",
			ext:  "json",
			want: "reconciliations/2026-06-27/abc123-unmatched.json",
		},
		{
			name: "csv matched",
			part: "matched",
			ext:  "csv",
			want: "reconciliations/2026-06-27/abc123-matched.csv",
		},
		{
			name: "csv unmatched",
			part: "unmatched",
			ext:  "csv",
			want: "reconciliations/2026-06-27/abc123-unmatched.csv",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, BuildExportKey(date, id, tc.part, tc.ext))
		})
	}
}

// TestS3_LocalStackRoundTrip exercises Upload + PresignGet against a real S3
// endpoint (LocalStack in dev/CI). It is skipped when BLNK_S3_ENDPOINT is
// unset, so `go test ./...` in an environment without LocalStack stays fast
// and side-effect free. When LocalStack IS available (CI or `docker compose
// up localstack`), this test verifies:
//
//  1. NewS3Client builds without error against the LocalStack endpoint.
//  2. CreateBucket provisions a fresh bucket.
//  3. Upload stores the bytes with the correct Content-Type.
//  4. PresignGet returns a URL that, when fetched with a plain HTTP client,
//     yields the original bytes — i.e. the full Put → Presign → HTTP GET
//     round-trip works end-to-end.
//
// The bucket name includes a nanosecond timestamp so concurrent runs do not
// collide; cleanup is best-effort because LocalStack in CI is ephemeral.
func TestS3_LocalStackRoundTrip(t *testing.T) {
	endpoint := os.Getenv("BLNK_S3_ENDPOINT")
	if endpoint == "" {
		t.Skip("BLNK_S3_ENDPOINT not set; skipping LocalStack integration test")
	}

	cfg := &config.Configuration{
		S3Endpoint:         endpoint,
		S3BucketName:       fmt.Sprintf("blnk-export-test-%d", time.Now().UnixNano()),
		S3Region:           envOr("BLNK_S3_REGION", "us-east-1"),
		AwsAccessKeyId:     envOr("BLNK_AWS_ACCESS_KEY_ID", "test"),
		AwsSecretAccessKey: envOr("BLNK_AWS_SECRET_ACCESS_KEY", "test"),
	}

	client, err := NewS3Client(cfg)
	if err != nil {
		t.Fatalf("NewS3Client: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := client.CreateBucket(ctx, cfg.S3BucketName); err != nil {
		t.Skipf("CreateBucket failed (is LocalStack reachable at %s?): %v", endpoint, err)
	}

	// Build a representative export payload: the same JSON shape the
	// reconciliation exporter would produce for the matched file.
	payload := []byte(`{"reconciliation_id":"rec_localstack_test","status":"completed","matched":1,"unmatched":0}`)
	key := BuildExportKey(time.Now(), "localstack-roundtrip", "matched", "json")
	contentType := "application/json"

	if err := client.Upload(ctx, cfg.S3BucketName, key, payload, contentType); err != nil {
		t.Fatalf("Upload: %v", err)
	}

	url, err := client.PresignGet(cfg.S3BucketName, key, 24*time.Hour)
	if err != nil {
		t.Fatalf("PresignGet: %v", err)
	}
	assert.NotEmpty(t, url)
	assert.Contains(t, url, key, "presigned URL must reference the uploaded key")

	resp, err := http.Get(url)
	if err != nil {
		t.Fatalf("HTTP GET presigned URL: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, contentType, resp.Header.Get("Content-Type"))

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	assert.Equal(t, payload, body, "downloaded bytes must match what was uploaded")
}

// envOr returns the value of the named environment variable, or fallback if
// it is unset/empty.
func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
