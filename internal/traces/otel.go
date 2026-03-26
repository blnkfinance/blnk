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

package trace

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"os"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	promexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/log/global"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.17.0"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// metricsHandler holds the Prometheus HTTP handler for the /metrics endpoint.
// Set during SetupOTelSDK and accessed via MetricsHandler().
var metricsHandler http.Handler

// MetricsHandler returns the Prometheus HTTP handler for serving the /metrics endpoint.
// Returns nil if SetupOTelSDK has not been called or metrics are not configured.
func MetricsHandler() http.Handler {
	return metricsHandler
}

// SetupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func SetupOTelSDK(ctx context.Context, serviceName string) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Create shared resource for all providers.
	res, err := newResource(ctx, serviceName)
	if err != nil {
		return shutdown, err
	}

	// Set up propagator.
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	// Set up trace provider.
	tracerProvider, err := newTraceProvider(ctx, res)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	otel.SetTracerProvider(tracerProvider)

	// Set up meter provider (dual-mode: Prometheus pull + OTLP push).
	meterProvider, err := newMeterProvider(ctx, res)
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	// Set up logger provider.
	loggerProvider, err := newLoggerProvider()
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
	global.SetLoggerProvider(loggerProvider)

	return
}

// newResource creates a shared OTel resource with the service name.
func newResource(ctx context.Context, serviceName string) (*resource.Resource, error) {
	return resource.New(ctx,
		resource.WithAttributes(
			semconv.ServiceNameKey.String(serviceName),
		),
	)
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newTraceProvider(ctx context.Context, res *resource.Resource) (*sdktrace.TracerProvider, error) {
	exporter, err := newTraceExporter(ctx)
	if err != nil {
		return nil, err
	}

	traceProvider := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter,
			sdktrace.WithBatchTimeout(5*time.Second)),
		sdktrace.WithResource(res),
	)
	return traceProvider, nil
}

// newMeterProvider creates a MeterProvider with dual-mode export:
//   - Pull (Prometheus): always active — exposes metrics via /metrics endpoint for scraping.
//   - Push (OTLP HTTP): opt-in — enabled when OTEL_EXPORTER_OTLP_ENDPOINT is set.
//     Uses the standard OTel env var, so it works automatically with any OTel Collector.
func newMeterProvider(ctx context.Context, res *resource.Resource) (*sdkmetric.MeterProvider, error) {
	// Pull exporter: Prometheus scrape endpoint (always active).
	promExp, err := promexporter.New()
	if err != nil {
		return nil, err
	}

	metricsHandler = promhttp.Handler()

	opts := []sdkmetric.Option{
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(promExp),
	}

	// Push exporter: enabled when an OTLP metrics endpoint is configured.
	// Checks OTEL_EXPORTER_OTLP_METRICS_ENDPOINT (signal-specific) or
	// OTEL_EXPORTER_OTLP_ENDPOINT (generic fallback) per the OTel spec.
	if os.Getenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") != "" || os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT") != "" {
		otlpExp, err := otlpmetrichttp.New(ctx,
			otlpmetrichttp.WithInsecure(),
		)
		if err != nil {
			return nil, err
		}
		opts = append(opts, sdkmetric.WithReader(sdkmetric.NewPeriodicReader(otlpExp,
			sdkmetric.WithInterval(60*time.Second),
		)))
	}

	meterProvider := sdkmetric.NewMeterProvider(opts...)
	return meterProvider, nil
}

func newLoggerProvider() (*log.LoggerProvider, error) {
	logExporter, err := stdoutlog.New()
	if err != nil {
		return nil, err
	}

	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
	)
	return loggerProvider, nil
}

func newTraceExporter(ctx context.Context) (sdktrace.SpanExporter, error) {
	// Resolve endpoint from signal-specific var first, then generic fallback.
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT")
	if endpoint == "" {
		endpoint = os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	}
	if endpoint == "" {
		// No endpoint configured — let the SDK use its defaults.
		return otlptracehttp.New(ctx, otlptracehttp.WithInsecure())
	}

	// Normalize the endpoint to host:port. The SDK's WithEndpoint() takes host:port
	// and always appends /v1/traces, so this works regardless of whether the env var
	// contains "jaeger:4318", "http://jaeger:4318", or "http://jaeger:4318/v1/traces".
	host, insecure := parseOTLPEndpoint(endpoint)
	opts := []otlptracehttp.Option{otlptracehttp.WithEndpoint(host)}
	if insecure {
		opts = append(opts, otlptracehttp.WithInsecure())
	}
	return otlptracehttp.New(ctx, opts...)
}

// parseOTLPEndpoint extracts the host:port from an OTLP endpoint string and
// determines whether to use an insecure (non-TLS) connection.
// Handles all common formats: "host:port", "http://host:port", "http://host:port/v1/traces".
func parseOTLPEndpoint(endpoint string) (hostPort string, insecure bool) {
	parsed, err := url.Parse(endpoint)
	if err != nil || parsed.Host == "" {
		// Not a valid URL — treat as bare host:port, assume insecure.
		return endpoint, true
	}
	return parsed.Host, parsed.Scheme != "https"
}
