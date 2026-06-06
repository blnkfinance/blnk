package monitoringexporter

import (
	"testing"
	"time"
)

func TestParseDSN(t *testing.T) {
	cfg, err := ParseDSN("https://blnk_obs_abc123@observe.blnk.cloud/monproj_123")
	if err != nil {
		t.Fatalf("ParseDSN returned error: %v", err)
	}

	if cfg.PublicKey != "blnk_obs_abc123" {
		t.Fatalf("expected write key blnk_obs_abc123, got %s", cfg.PublicKey)
	}
	if cfg.ProjectID != "monproj_123" {
		t.Fatalf("expected project id monproj_123, got %s", cfg.ProjectID)
	}
	if cfg.Endpoint != "https://observe.blnk.cloud" {
		t.Fatalf("expected endpoint https://observe.blnk.cloud, got %s", cfg.Endpoint)
	}
	if cfg.Timeout != 5*time.Second {
		t.Fatalf("expected default timeout 5s, got %s", cfg.Timeout)
	}
}

func TestParseDSNAllowsCustomHostWithSameShape(t *testing.T) {
	cfg, err := ParseDSN("https://write_key_123@telemetry.example.com/project_123")
	if err != nil {
		t.Fatalf("ParseDSN returned error: %v", err)
	}

	if cfg.PublicKey != "write_key_123" {
		t.Fatalf("expected write key write_key_123, got %s", cfg.PublicKey)
	}
	if cfg.ProjectID != "project_123" {
		t.Fatalf("expected project id project_123, got %s", cfg.ProjectID)
	}
	if cfg.Endpoint != "https://telemetry.example.com" {
		t.Fatalf("expected endpoint https://telemetry.example.com, got %s", cfg.Endpoint)
	}
	if cfg.SignalURL("logs") != "https://telemetry.example.com/monitoring/ingest/v1/logs" {
		t.Fatalf("unexpected logs URL: %s", cfg.SignalURL("logs"))
	}
	if cfg.OTLPSignalURL("metrics") != "https://telemetry.example.com/monitoring/ingest/v1/otlp/metrics" {
		t.Fatalf("unexpected OTLP metrics URL: %s", cfg.OTLPSignalURL("metrics"))
	}
}

func TestParseDSNRejectsInvalidInputs(t *testing.T) {
	tests := []string{
		"",
		"http://pk_test@observe.blnk.cloud/project_123",
		"https://observe.blnk.cloud/project_123",
		"https://example.com/in/e_test",
		"https://pk_test@observe.blnk.cloud",
		"https://pk_test@observe.blnk.cloud/project/extra",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			if _, err := ParseDSN(input); err == nil {
				t.Fatalf("expected error for %q", input)
			}
		})
	}
}

func TestFromEnv(t *testing.T) {
	t.Setenv(envDSN, "")
	t.Setenv(envLegacyDSN, "")
	cfg, enabled, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv returned error without DSN: %v", err)
	}
	if enabled {
		t.Fatalf("expected monitoring exporter to be disabled without DSN")
	}
	if cfg.ProjectID != "" {
		t.Fatalf("expected empty config without DSN")
	}

	t.Setenv(envDSN, "https://blnk_obs_live@observe.blnk.cloud/monproj_live")

	cfg, enabled, err = FromEnv()
	if err != nil {
		t.Fatalf("FromEnv returned error: %v", err)
	}
	if !enabled {
		t.Fatalf("expected monitoring exporter to be enabled")
	}
	if cfg.ProjectID != "monproj_live" {
		t.Fatalf("expected project id to be copied, got %q", cfg.ProjectID)
	}
}

func TestFromEnvFallsBackToLegacyCloudDSN(t *testing.T) {
	t.Setenv(envDSN, "")
	t.Setenv(envLegacyDSN, "https://legacy_key@observe.blnk.cloud/legacy_project")

	cfg, enabled, err := FromEnv()
	if err != nil {
		t.Fatalf("FromEnv returned error: %v", err)
	}
	if !enabled {
		t.Fatalf("expected monitoring exporter to be enabled")
	}
	if cfg.ProjectID != "legacy_project" {
		t.Fatalf("expected legacy project id, got %q", cfg.ProjectID)
	}
}

func TestHeadersAndSignalURL(t *testing.T) {
	cfg := Config{
		PublicKey: "blnk_obs_abc123",
		ProjectID: "monproj_123",
		Endpoint:  "https://observe.blnk.cloud/",
	}

	headers := cfg.Headers()
	if headers["Authorization"] != "Bearer blnk_obs_abc123" {
		t.Fatalf("unexpected auth header: %s", headers["Authorization"])
	}
	if _, ok := headers["X-Blnk-Project-ID"]; ok {
		t.Fatalf("did not expect project header")
	}
	if _, ok := headers["X-Blnk-Environment"]; ok {
		t.Fatalf("did not expect environment header")
	}
	if _, ok := headers["X-Blnk-Instance-ID"]; ok {
		t.Fatalf("did not expect instance id header")
	}
	if cfg.SignalURL("logs") != "https://observe.blnk.cloud/monitoring/ingest/v1/logs" {
		t.Fatalf("unexpected logs URL: %s", cfg.SignalURL("logs"))
	}
	if cfg.OTLPSignalURL("traces") != "https://observe.blnk.cloud/monitoring/ingest/v1/otlp/traces" {
		t.Fatalf("unexpected OTLP traces URL: %s", cfg.OTLPSignalURL("traces"))
	}
}
