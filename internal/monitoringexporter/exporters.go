package monitoringexporter

import (
	"context"
	"time"

	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// NewTraceExporter creates an OTLP/HTTP trace exporter for remote monitoring.
func NewTraceExporter(ctx context.Context, cfg Config) (sdktrace.SpanExporter, error) {
	return otlptracehttp.New(ctx,
		otlptracehttp.WithEndpointURL(cfg.OTLPSignalURL("traces")),
		otlptracehttp.WithHeaders(cfg.Headers()),
		otlptracehttp.WithTimeout(cfg.Timeout),
	)
}

// NewMetricReader creates an OTLP/HTTP metric reader for remote monitoring.
func NewMetricReader(ctx context.Context, cfg Config) (sdkmetric.Reader, error) {
	exporter, err := otlpmetrichttp.New(ctx,
		otlpmetrichttp.WithEndpointURL(cfg.OTLPSignalURL("metrics")),
		otlpmetrichttp.WithHeaders(cfg.Headers()),
		otlpmetrichttp.WithTimeout(cfg.Timeout),
	)
	if err != nil {
		return nil, err
	}

	return sdkmetric.NewPeriodicReader(exporter,
		sdkmetric.WithInterval(60*time.Second),
		sdkmetric.WithTimeout(cfg.Timeout),
	), nil
}
