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
	"os"
	"strings"
	"testing"

	"github.com/sirupsen/logrus"
	logtest "github.com/sirupsen/logrus/hooks/test"
)

func isolateSecurityEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{"BLNK_ALLOW_INSECURE", "BLNK_SERVER_SECURE", "BLNK_SERVER_SECRET_KEY", "BLNK_DATA_SOURCE_DNS"} {
		t.Setenv(key, "unset-me")
		// Unset after Setenv so an empty value is not left for envconfig to parse.
		// The key is static, so Unsetenv cannot fail.
		_ = os.Unsetenv(key)
	}
}

func openLedgerConfig() Configuration {
	return Configuration{
		ProjectName: "Blnk",
		DataSource:  DataSourceConfig{Dns: "postgres://postgres:password@postgres:5432/blnk?sslmode=disable"},
		Redis:       RedisConfig{Dns: "redis:6379"},
		Server:      ServerConfig{Port: "5001"},
	}
}

func TestServerSecurityGate(t *testing.T) {
	t.Run("config load still allows migrate without authentication", func(t *testing.T) {
		isolateSecurityEnv(t)
		cnf := openLedgerConfig()
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("loading an open config must not block migrate, got %v", err)
		}
	})

	t.Run("refuses to serve the unauthenticated default", func(t *testing.T) {
		isolateSecurityEnv(t)
		cnf := openLedgerConfig()
		err := cnf.RequireAPIAuthentication()
		if err == nil || !strings.Contains(err.Error(), "authentication is DISABLED") {
			t.Fatalf("expected the API server to be refused, got %v", err)
		}
	})

	t.Run("allows insecure mode only with an explicit opt-in", func(t *testing.T) {
		isolateSecurityEnv(t)
		t.Setenv("BLNK_ALLOW_INSECURE", "true")
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := openLedgerConfig()
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("opt-in config should load, got %v", err)
		}
		if err := cnf.RequireAPIAuthentication(); err != nil {
			t.Fatalf("opt-in should allow the API server, got %v", err)
		}
		var warned bool
		for _, entry := range hook.AllEntries() {
			if entry.Level == logrus.WarnLevel && strings.Contains(entry.Message, "authentication is DISABLED") {
				warned = true
			}
		}
		if !warned {
			t.Fatal("expected a SECURITY warning when insecure mode is explicitly allowed")
		}
	})

	t.Run("refuses secure mode without a master key", func(t *testing.T) {
		isolateSecurityEnv(t)
		cnf := openLedgerConfig()
		cnf.Server.Secure = true
		cnf.Server.SecretKey = "   "
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("secure config without a key must still load for migrate, got %v", err)
		}
		err := cnf.RequireAPIAuthentication()
		if err == nil || !strings.Contains(err.Error(), "server.secret_key is required") {
			t.Fatalf("expected a missing secret_key error, got %v", err)
		}
	})

	t.Run("accepts secure mode with a master key", func(t *testing.T) {
		isolateSecurityEnv(t)
		hook := logtest.NewGlobal()
		defer hook.Reset()

		cnf := openLedgerConfig()
		cnf.Server.Secure = true
		cnf.Server.SecretKey = "master-secret"
		if err := cnf.validateAndAddDefaults(); err != nil {
			t.Fatalf("secure config should load, got %v", err)
		}
		if err := cnf.RequireAPIAuthentication(); err != nil {
			t.Fatalf("secure config should serve, got %v", err)
		}
		for _, entry := range hook.AllEntries() {
			if strings.Contains(entry.Message, "authentication is DISABLED") {
				t.Fatal("did not expect an authentication-disabled warning")
			}
		}
	})
}

func TestShippedConfigMapRequiresASecret(t *testing.T) {
	isolateSecurityEnv(t)
	raw, err := os.ReadFile("../infrastructure/k8s-manifests/blnk-config.yaml")
	if err != nil {
		t.Fatal(err)
	}
	const marker = "blnk.json: |"
	index := strings.Index(string(raw), marker)
	if index < 0 {
		t.Fatal("blnk-config.yaml has no blnk.json document")
	}
	var jsonBody strings.Builder
	for _, line := range strings.Split(string(raw)[index+len(marker):], "\n") {
		if strings.TrimSpace(line) == "" {
			continue
		}
		if !strings.HasPrefix(line, "    ") {
			break
		}
		jsonBody.WriteString(strings.TrimPrefix(line, "    "))
		jsonBody.WriteByte('\n')
	}

	file, err := os.CreateTemp("", "blnk-configmap-*.json")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = os.Remove(file.Name()) }()
	if _, err := file.WriteString(jsonBody.String()); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	t.Setenv("BLNK_DATA_SOURCE_DNS", "postgres://postgres:secret@postgres:5432/blnk?sslmode=disable")
	if err := InitConfig(file.Name()); err != nil {
		t.Fatalf("shipped config must load without a master key so migrate can run, got %v", err)
	}
	loaded, err := Fetch()
	if err != nil {
		t.Fatal(err)
	}
	if err := loaded.RequireAPIAuthentication(); err == nil || !strings.Contains(err.Error(), "server.secret_key is required") {
		t.Fatalf("API server should refuse the shipped config without a master key, got %v", err)
	}

	t.Setenv("BLNK_SERVER_SECRET_KEY", "env-master-key")
	if err := InitConfig(file.Name()); err != nil {
		t.Fatalf("shipped config with a master key from the environment should load, got %v", err)
	}
	loaded, err = Fetch()
	if err != nil {
		t.Fatal(err)
	}
	if err := loaded.RequireAPIAuthentication(); err != nil {
		t.Fatal(err)
	}
	if !loaded.Server.Secure || loaded.Server.SecretKey != "env-master-key" {
		t.Fatalf("secure=%v", loaded.Server.Secure)
	}
}
