package monitoringexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/sirupsen/logrus"
	"go.opentelemetry.io/otel/trace"
)

const logQueueSize = 1024

// LogHook asynchronously forwards sanitized logrus entries to remote monitoring.
type LogHook struct {
	cfg    Config
	client *http.Client
	queue  chan logPayload
	closed atomic.Bool
	wg     sync.WaitGroup
	levels []logrus.Level
}

type logPayload struct {
	Time    time.Time              `json:"time"`
	Level   string                 `json:"level"`
	Message string                 `json:"message"`
	TraceID string                 `json:"trace_id,omitempty"`
	SpanID  string                 `json:"span_id,omitempty"`
	Fields  map[string]interface{} `json:"fields,omitempty"`
}

func NewLogHook(cfg Config) *LogHook {
	h := &LogHook{
		cfg: cfg,
		client: &http.Client{
			Timeout: cfg.Timeout,
		},
		queue:  make(chan logPayload, logQueueSize),
		levels: logrus.AllLevels,
	}
	h.wg.Add(1)
	go h.run()
	return h
}

func InstallLogHook(cfg Config) *LogHook {
	h := NewLogHook(cfg)
	logrus.AddHook(h)
	return h
}

func (h *LogHook) Levels() []logrus.Level {
	return h.levels
}

func (h *LogHook) Fire(entry *logrus.Entry) error {
	if h.closed.Load() {
		return nil
	}

	payload := logPayload{
		Time:    entry.Time.UTC(),
		Level:   entry.Level.String(),
		Message: RedactString(entry.Message),
		Fields:  RedactFields(entry.Data),
	}

	if entry.Context != nil {
		spanContext := trace.SpanContextFromContext(entry.Context)
		if spanContext.IsValid() {
			payload.TraceID = spanContext.TraceID().String()
			payload.SpanID = spanContext.SpanID().String()
		}
	}

	select {
	case h.queue <- payload:
	default:
	}
	return nil
}

func (h *LogHook) Shutdown(ctx context.Context) error {
	if h.closed.Swap(true) {
		return nil
	}
	close(h.queue)

	done := make(chan struct{})
	go func() {
		h.wg.Wait()
		close(done)
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-done:
		return nil
	}
}

func (h *LogHook) run() {
	defer h.wg.Done()
	for payload := range h.queue {
		h.send(payload)
	}
}

func (h *LogHook) send(payload logPayload) {
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), h.cfg.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, h.cfg.SignalURL("logs"), bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	for key, value := range h.cfg.Headers() {
		req.Header.Set(key, value)
	}

	resp, err := h.client.Do(req)
	if err != nil {
		return
	}
	_ = resp.Body.Close()
}
