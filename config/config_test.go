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
package config

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func TestValidateAndAddDefaults_InsecureWarning(t *testing.T) {
	baseConfig := func() Configuration {
		return Configuration{
			ProjectName: "Test Project",
			DataSource:  DataSourceConfig{Dns: "some-dns"},
			Redis:       RedisConfig{Dns: "localhost:6379"},
		}
	}

	t.Run("warns when secure is disabled", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := baseConfig()
		cnf.Server.Secure = false
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		var warned bool
		for _, entry := range hook.AllEntries() {
			if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "authentication is DISABLED") {
				warned = true
			}
		}
		if !warned {
			t.Errorf("Expected a SECURITY warning when server.secure is false")
		}
	})

	t.Run("no warning when secure is enabled", func(t *testing.T) {
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := baseConfig()
		cnf.Server.Secure = true
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("Expected no error, got %v", err)
		}

		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, "authentication is DISABLED") {
				t.Errorf("Did not expect a SECURITY warning when server.secure is true")
			}
		}
	})
}

func TestValidateAndAddDefaults(t *testing.T) {
	// Test case with empty ProjectName and DataSource DNS
	cnf := Configuration{
		ProjectName: "",
		DataSource: DataSourceConfig{
			Dns: "",
		},
		Redis: RedisConfig{
			Dns: "localhost:6379",
		},
	}

	err := cnf.validateAndAddDefaults()
	if err == nil || err.Error() != "data source DNS is required" {
		t.Errorf("Expected data source DNS required error, got %v", err)
	}
	cnf = Configuration{
		ProjectName: "",
		DataSource: DataSourceConfig{
			Dns: "postgres://localhost:5432",
		},
		Redis: RedisConfig{
			Dns: "",
		},
	}

	err = cnf.validateAndAddDefaults()
	if err == nil || err.Error() != "redis DNS is required" {
		t.Errorf("Expected redis DNS required error, got %v", err)
	}
	// Test case with all required fields filled, expect no error
	cnf = Configuration{
		ProjectName: "Test Project",
		DataSource: DataSourceConfig{
			Dns: "some-dns",
		},
		Redis: RedisConfig{
			Dns: "localhost:6379",
		},
	}

	err = cnf.validateAndAddDefaults()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	// Test default port setting
	cnf.Server.Port = ""
	err = cnf.validateAndAddDefaults()
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if cnf.Server.Port != DEFAULT_PORT {
		t.Errorf("Expected default port %s, got %s", DEFAULT_PORT, cnf.Server.Port)
	}
	if cnf.Transaction.LockWaitTimeout != 3*time.Second {
		t.Errorf("Expected default lock wait timeout %s, got %s", 3*time.Second, cnf.Transaction.LockWaitTimeout)
	}
	if !cnf.Transaction.EnableCoalescing {
		t.Errorf("Expected coalescing to default to enabled")
	}
}

func TestValidateAndAddDefaults_TransactionLockWaitTimeout(t *testing.T) {
	cnf := Configuration{
		ProjectName: "Test Project",
		DataSource: DataSourceConfig{
			Dns: "some-dns",
		},
		Redis: RedisConfig{
			Dns: "localhost:6379",
		},
		Transaction: TransactionConfig{
			LockWaitTimeout: 12,
		},
	}

	err := cnf.validateAndAddDefaults()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if cnf.Transaction.LockWaitTimeout != 12*time.Second {
		t.Fatalf("Expected LockWaitTimeout to be 12s, got %s", cnf.Transaction.LockWaitTimeout)
	}
}

func TestValidateAndAddDefaults_TransactionLockWaitTimeoutAlreadyDuration(t *testing.T) {
	cnf := Configuration{
		ProjectName: "Test Project",
		DataSource: DataSourceConfig{
			Dns: "some-dns",
		},
		Redis: RedisConfig{
			Dns: "localhost:6379",
		},
		Transaction: TransactionConfig{
			LockWaitTimeout: 5 * time.Second,
		},
	}

	err := cnf.validateAndAddDefaults()
	if err != nil {
		t.Fatalf("Expected no error, got %v", err)
	}

	if cnf.Transaction.LockWaitTimeout != 5*time.Second {
		t.Fatalf("Expected LockWaitTimeout to remain 5s, got %s", cnf.Transaction.LockWaitTimeout)
	}
}

func TestLoadConfigFromFile(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up after the test

	// Sample configuration to write to the temp file
	sampleConfig := Configuration{
		ProjectName: "Temp Project",
		DataSource: DataSourceConfig{
			Dns: "temp-dns",
		},
		Redis: RedisConfig{
			Dns: "temp-redis",
		},
		Transaction: TransactionConfig{
			LockWaitTimeout: 7,
		},
	}
	if err := json.NewEncoder(tmpFile).Encode(sampleConfig); err != nil {
		t.Fatalf("Unable to write to temporary file: %v", err)
	}
	tmpFile.Close() // Close the file so loadConfigFromFile can open it

	// Set an environment variable to override the project name
	os.Setenv("BLNK_PROJECT_NAME", "Env Project")
	defer os.Unsetenv("BLNK_PROJECT_NAME") // Clean up after the test

	// Load the configuration from the file
	if err := loadConfigFromFile(tmpFile.Name()); err != nil {
		t.Fatalf("loadConfigFromFile failed: %v", err)
	}

	// Fetch the loaded configuration
	loadedConfig, err := Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	// Check if the environment variable override worked
	if loadedConfig.ProjectName != "Env Project" {
		t.Errorf("Expected ProjectName to be 'Env Project', got '%s'", loadedConfig.ProjectName)
	}

	// Check if the DNS was loaded correctly from the file
	if loadedConfig.DataSource.Dns != "temp-dns" {
		t.Errorf("Expected DataSource.Dns to be 'temp-dns', got '%s'", loadedConfig.DataSource.Dns)
	}
	if loadedConfig.Transaction.LockWaitTimeout != 7*time.Second {
		t.Errorf("Expected LockWaitTimeout to be '7s', got '%s'", loadedConfig.Transaction.LockWaitTimeout)
	}
}

func TestLoadConfigFromFileMonitoringDSN(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	defer func() { _ = os.Remove(tmpFile.Name()) }()

	sampleConfig := Configuration{
		ProjectName:         "Monitoring DSN Test",
		MonitoringDSN:       "https://blnk_obs_test@observe.blnk.cloud/monproj_test",
		EnableObservability: true,
		DataSource: DataSourceConfig{
			Dns: "temp-dns",
		},
		Redis: RedisConfig{
			Dns: "temp-redis",
		},
	}
	if err := json.NewEncoder(tmpFile).Encode(sampleConfig); err != nil {
		t.Fatalf("Unable to write to temporary file: %v", err)
	}
	if err := tmpFile.Close(); err != nil {
		t.Fatalf("Unable to close temporary file: %v", err)
	}

	if err := loadConfigFromFile(tmpFile.Name()); err != nil {
		t.Fatalf("loadConfigFromFile failed: %v", err)
	}

	loadedConfig, err := Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}
	if loadedConfig.MonitoringDSN != "https://blnk_obs_test@observe.blnk.cloud/monproj_test" {
		t.Fatalf("Expected MonitoringDSN from config file, got %q", loadedConfig.MonitoringDSN)
	}
	if loadedConfig.RemoteMonitoringDSN() != "https://blnk_obs_test@observe.blnk.cloud/monproj_test" {
		t.Fatalf("Expected remote monitoring DSN from config file, got %q", loadedConfig.RemoteMonitoringDSN())
	}
}

func TestInitConfig(t *testing.T) {
	// Create a temporary file
	tmpFile, err := os.CreateTemp("", "blnk.json")
	if err != nil {
		t.Fatalf("Unable to create temporary file: %v", err)
	}
	defer os.Remove(tmpFile.Name()) // Clean up after the test

	// Sample configuration to write to the temp file
	sampleConfig := Configuration{
		ProjectName: "InitConfig Test",
		DataSource: DataSourceConfig{
			Dns: "init-config-dns",
		}, Redis: RedisConfig{
			Dns: "localhost:6379",
		},
	}
	if err := json.NewEncoder(tmpFile).Encode(sampleConfig); err != nil {
		t.Fatalf("Unable to write to temporary file: %v", err)
	}
	tmpFile.Close() // Close the file so InitConfig can open it

	// Attempt to initialize the configuration using the temporary file
	if err := InitConfig(tmpFile.Name()); err != nil {
		t.Fatalf("InitConfig failed: %v", err)
	}

	// Fetch the loaded configuration to verify it was loaded correctly
	loadedConfig, err := Fetch()
	if err != nil {
		t.Fatalf("Fetch failed: %v", err)
	}

	// Verify the configuration was loaded correctly
	if loadedConfig.ProjectName != "InitConfig Test" {
		t.Errorf("Expected ProjectName to be 'InitConfig Test', got '%s'", loadedConfig.ProjectName)
	}
	if loadedConfig.DataSource.Dns != "init-config-dns" {
		t.Errorf("Expected DataSource.Dns to be 'init-config-dns', got '%s'", loadedConfig.DataSource.Dns)
	}
}

// TestUploadURLTimeoutDefault verifies that an unset/zero upload URL timeout is
// defaulted to DEFAULT_UPLOAD_URL_TIMEOUT_SEC (30s).
func TestUploadURLTimeoutDefault(t *testing.T) {
	cnf := Configuration{
		ProjectName: "Test Project",
		DataSource:  DataSourceConfig{Dns: "some-dns"},
		Redis:       RedisConfig{Dns: "localhost:6379"},
	}
	cnf.Server.UploadURLTimeoutSec = 0
	if err := cnf.validateAndAddDefaults(); err != nil {
		t.Fatalf("validateAndAddDefaults failed: %v", err)
	}
	if cnf.Server.UploadURLTimeoutSec != DEFAULT_UPLOAD_URL_TIMEOUT_SEC {
		t.Errorf("expected default timeout %d, got %d", DEFAULT_UPLOAD_URL_TIMEOUT_SEC, cnf.Server.UploadURLTimeoutSec)
	}

	// An explicitly-configured value must be preserved.
	cnf2 := Configuration{
		ProjectName: "Test Project",
		DataSource:  DataSourceConfig{Dns: "some-dns"},
		Redis:       RedisConfig{Dns: "localhost:6379"},
	}
	cnf2.Server.UploadURLTimeoutSec = 7
	if err := cnf2.validateAndAddDefaults(); err != nil {
		t.Fatalf("validateAndAddDefaults failed: %v", err)
	}
	if cnf2.Server.UploadURLTimeoutSec != 7 {
		t.Errorf("expected explicit timeout 7 to be preserved, got %d", cnf2.Server.UploadURLTimeoutSec)
	}
}

// TestUploadWhitelistHostsParsing verifies parsing of the comma-separated
// whitelist: trimming, scheme-tolerance, case-folding, de-duplication, and
// deny-by-default (empty -> nil).
func TestUploadWhitelistHostsParsing(t *testing.T) {
	cases := []struct {
		name   string
		raw    string
		expect []string
	}{
		{
			name:   "mixed entries with schemes, empties, and dup",
			raw:    "example.com, https://x.com/path, ,Foo.COM, example.com",
			expect: []string{"example.com", "x.com", "foo.com"},
		},
		{
			name:   "bare hostnames only",
			raw:    "a.example.com, b.example.com",
			expect: []string{"a.example.com", "b.example.com"},
		},
		{
			name:   "empty whitelist is deny-by-default",
			raw:    "",
			expect: nil,
		},
		{
			name:   "only whitespace and commas",
			raw:    " , , ",
			expect: nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cnf := Configuration{Server: ServerConfig{UploadDomainWhitelist: tc.raw}}
			got := cnf.UploadDomainWhitelistHosts()
			if len(got) != len(tc.expect) {
				t.Fatalf("expected %v, got %v", tc.expect, got)
			}
			for i := range got {
				if got[i] != tc.expect[i] {
					t.Errorf("entry %d: expected %q, got %q", i, tc.expect[i], got[i])
				}
			}
		})
	}
}
