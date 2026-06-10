package trace

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
)

// fakeOTLPCollector records OTLP/HTTP export requests so tests can assert
// that telemetry genuinely leaves the process.
type fakeOTLPCollector struct {
	mu       sync.Mutex
	requests []*http.Request
	bodies   [][]byte
	srv      *httptest.Server
}

func newFakeOTLPCollector(t *testing.T) *fakeOTLPCollector {
	t.Helper()
	c := &fakeOTLPCollector{}
	c.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body := make([]byte, r.ContentLength)
		_, _ = r.Body.Read(body)
		c.mu.Lock()
		c.requests = append(c.requests, r.Clone(context.Background()))
		c.bodies = append(c.bodies, body)
		c.mu.Unlock()
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("{}"))
	}))
	t.Cleanup(c.srv.Close)
	return c
}

// exportPosts returns recorded OTLP export requests. The URL path depends on
// which env var configured the endpoint (the OTel spec appends /v1/traces to
// the generic endpoint but uses signal-specific endpoints as-is), so exports
// are identified by their protobuf POST signature rather than a fixed path.
func (c *fakeOTLPCollector) exportPosts() []*http.Request {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []*http.Request
	for _, r := range c.requests {
		if r.Method == http.MethodPost && strings.Contains(r.Header.Get("Content-Type"), "x-protobuf") {
			out = append(out, r)
		}
	}
	return out
}

func TestSetupOTelSDK_BasicLifecycle(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	ctx := context.Background()
	shutdown, err := SetupOTelSDK(ctx, "blnk-test", "")
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// The W3C composite propagator must be installed globally:
	// traceparent/tracestate from TraceContext, baggage from Baggage.
	fields := otel.GetTextMapPropagator().Fields()
	assert.ElementsMatch(t, []string{"traceparent", "tracestate", "baggage"}, fields)

	// The Prometheus metrics handler must be wired.
	require.NotNil(t, MetricsHandler())

	// First shutdown flushes cleanly; second is a registered-once no-op.
	require.NoError(t, shutdown(ctx))
	require.NoError(t, shutdown(ctx), "second shutdown call must be a no-op, not a double-shutdown error")
}

func TestSetupOTelSDK_ExportsSpansToCollector(t *testing.T) {
	collector := newFakeOTLPCollector(t)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", collector.srv.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	ctx := context.Background()
	shutdown, err := SetupOTelSDK(ctx, "blnk-span-test", "")
	require.NoError(t, err)

	_, span := otel.Tracer("blnk-test-tracer").Start(ctx, "test-operation")
	span.End()

	// Shutdown forces the batch processor to flush pending spans.
	require.NoError(t, shutdown(ctx))

	posts := collector.exportPosts()
	require.NotEmpty(t, posts, "the span must have been POSTed to the collector")
}

func TestSetupOTelSDK_GenericEndpointFallback(t *testing.T) {
	// Only the generic OTEL_EXPORTER_OTLP_ENDPOINT is set: the trace
	// exporter must fall back to it (newTraceExporter's second branch).
	collector := newFakeOTLPCollector(t)
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", collector.srv.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	ctx := context.Background()
	shutdown, err := SetupOTelSDK(ctx, "blnk-fallback-test", "")
	require.NoError(t, err)

	_, span := otel.Tracer("blnk-test-tracer").Start(ctx, "fallback-operation")
	span.End()
	require.NoError(t, shutdown(ctx))

	posts := collector.exportPosts()
	require.NotEmpty(t, posts, "spans must export via the generic endpoint fallback")
	// The generic endpoint gets /v1/traces appended per the OTel spec.
	assert.Equal(t, "/v1/traces", posts[0].URL.Path)
}

func TestSetupOTelSDK_MetricsOTLPBranch(t *testing.T) {
	// With a metrics endpoint configured, the dual-mode meter provider adds
	// the OTLP periodic reader on top of the always-on Prometheus exporter.
	collector := newFakeOTLPCollector(t)
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", collector.srv.URL)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")

	ctx := context.Background()
	shutdown, err := SetupOTelSDK(ctx, "blnk-metrics-test", "")
	require.NoError(t, err)

	// Record a metric so the periodic reader has something to push on flush.
	meter := otel.GetMeterProvider().Meter("blnk-test-meter")
	counter, err := meter.Int64Counter("blnk_test_counter")
	require.NoError(t, err)
	counter.Add(ctx, 1)

	require.NoError(t, shutdown(ctx))

	// Prometheus pull side must still work alongside the push exporter.
	require.NotNil(t, MetricsHandler())
}

func TestSetupOTelSDK_InvalidMonitoringDSNIsNonFatal(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	ctx := context.Background()
	// A garbage DSN must disable the monitoring exporter with a warning,
	// never fail SDK setup.
	shutdown, err := SetupOTelSDK(ctx, "blnk-bad-dsn-test", "not-a-valid-dsn")
	require.NoError(t, err)
	require.NotNil(t, shutdown)
	require.NoError(t, shutdown(ctx))
}

func TestSetupOTelSDK_ShutdownJoinsErrors(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	shutdown, err := SetupOTelSDK(context.Background(), "blnk-shutdown-err-test", "")
	require.NoError(t, err)

	// Create a span so the batch processor has pending work, then shut down
	// with an already-canceled context: at least one provider must surface
	// the cancellation, and the joined error must propagate to the caller.
	_, span := otel.Tracer("blnk-test-tracer").Start(context.Background(), "doomed-span")
	span.End()

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	err = shutdown(canceled)
	require.Error(t, err, "shutdown with a canceled context must report the underlying provider errors")
}

func TestSetupOTelSDK_ValidMonitoringDSNWiresExporter(t *testing.T) {
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "")
	t.Setenv("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT", "")

	// Point the monitoring DSN at a local fake so exporter construction
	// succeeds without external services.
	collector := newFakeOTLPCollector(t)
	dsn := "https://pk_test@" + collector.srv.Listener.Addr().String() + "/project_test"

	ctx := context.Background()
	shutdown, err := SetupOTelSDK(ctx, "blnk-dsn-test", dsn)
	require.NoError(t, err)
	require.NotNil(t, shutdown)

	// Allow a short grace period for lazy exporter goroutines, then flush.
	time.Sleep(50 * time.Millisecond)
	_ = shutdown(ctx)
}
