package trace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"
)

// TestNewMeterProvider_PrometheusOnlyWhenNoOTLPEndpoint verifies that when
// neither OTEL_EXPORTER_OTLP_ENDPOINT nor OTEL_EXPORTER_OTLP_METRICS_ENDPOINT
// is set, the MeterProvider is created with only the Prometheus exporter.
func TestNewMeterProvider_PrometheusOnlyWhenNoOTLPEndpoint(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	ctx := context.Background()
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceNameKey.String("test"),
	))
	require.NoError(t, err)

	mp, err := newMeterProvider(ctx, res)
	require.NoError(t, err)
	require.NotNil(t, mp)
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })

	// The Prometheus handler should be available after setup.
	handler := MetricsHandler()
	require.NotNil(t, handler, "MetricsHandler should be non-nil after MeterProvider setup")
}

// TestMetricsHandler_ServesPrometheusFormat verifies the full pipeline:
// MeterProvider → Prometheus exporter → HTTP handler → valid Prometheus text response.
func TestMetricsHandler_ServesPrometheusFormat(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	ctx := context.Background()
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceNameKey.String("test"),
	))
	require.NoError(t, err)

	mp, err := newMeterProvider(ctx, res)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mp.Shutdown(ctx) })

	handler := MetricsHandler()
	require.NotNil(t, handler)

	rec := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	handler.ServeHTTP(rec, req)

	assert.Equal(t, http.StatusOK, rec.Code)
	contentType := rec.Header().Get("Content-Type")
	assert.True(t, strings.Contains(contentType, "text/plain") || strings.Contains(contentType, "application/openmetrics-text"),
		"expected Prometheus content type, got: %s", contentType)
	assert.Contains(t, rec.Body.String(), "target_info", "response should contain OTel default metrics")
}

// TestParseOTLPEndpoint verifies backward-compatible endpoint normalization.
// Operators may provide the endpoint in various formats — all should resolve
// to the correct host:port for WithEndpoint().
func TestParseOTLPEndpoint(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantHost     string
		wantInsecure bool
	}{
		{
			name:         "full http URL (existing deployments)",
			input:        "http://jaeger:4318",
			wantHost:     "jaeger:4318",
			wantInsecure: true,
		},
		{
			name:         "full http URL with path",
			input:        "http://jaeger:4318/v1/traces",
			wantHost:     "jaeger:4318",
			wantInsecure: true,
		},
		{
			name:         "https URL",
			input:        "https://otel-collector.prod:4318",
			wantHost:     "otel-collector.prod:4318",
			wantInsecure: false,
		},
		{
			name:         "bare host:port",
			input:        "jaeger:4318",
			wantHost:     "jaeger:4318",
			wantInsecure: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			host, insecure := parseOTLPEndpoint(tt.input)
			assert.Equal(t, tt.wantHost, host)
			assert.Equal(t, tt.wantInsecure, insecure)
		})
	}
}
