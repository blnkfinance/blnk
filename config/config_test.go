package config

import (
	"encoding/json"
	"os"
	"testing"
)

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

func TestSetGrafanaExporterEnvs(t *testing.T) {
	// Load a mock configuration into ConfigStore
	mockConfig := Configuration{
		OtelGrafanaCloud: OtelGrafanaCloud{
			OtelExporterOtlpProtocol: "http/protobuf",
			OtelExporterOtlpEndpoint: "localhost:4317",
			OtelExporterOtlpHeaders:  "api-key=12345",
		},
	}
	ConfigStore.Store(&mockConfig)

	// Attempt to set Grafana exporter environment variables
	err := SetGrafanaExporterEnvs()
	if err != nil {
		t.Fatalf("SetGrafanaExporterEnvs failed: %v", err)
	}

	// Verify the environment variables were set correctly
	if os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL") != "http/protobuf" {
		t.Errorf("Expected OTEL_EXPORTER_OTLP_PROTOCOL to be 'http/protobuf', got '%s'", os.Getenv("OTEL_EXPORTER_OTLP_PROTOCOL"))
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "localhost:4317" {
		t.Errorf("Expected OTEL_EXPORTER_OTLP_ENDPOINT to be 'localhost:4317', got '%s'", os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"))
	}
	if os.Getenv("OTEL_EXPORTER_OTLP_HEADERS") != "api-key=12345" {
		t.Errorf("Expected OTEL_EXPORTER_OTLP_HEADERS to be 'api-key=12345', got '%s'", os.Getenv("OTEL_EXPORTER_OTLP_HEADERS"))
	}
}
