package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/sirupsen/logrus"
)

func TestLogrusAccessLoggerDropsQueryString(t *testing.T) {
	var buf bytes.Buffer
	logger := logrus.StandardLogger()
	previousOutput := logger.Out
	previousFormatter := logger.Formatter
	defer func() {
		logger.SetOutput(previousOutput)
		logger.SetFormatter(previousFormatter)
	}()

	logger.SetOutput(&buf)
	logger.SetFormatter(&logrus.JSONFormatter{})

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.Use(logrusAccessLogger())
	router.GET("/transactions", func(c *gin.Context) {
		c.Status(http.StatusNoContent)
	})

	req := httptest.NewRequest(http.MethodGet, "/transactions?token=secret&limit=20", nil)
	router.ServeHTTP(httptest.NewRecorder(), req)

	var entry map[string]interface{}
	if err := json.NewDecoder(&buf).Decode(&entry); err != nil {
		t.Fatalf("failed to decode log entry: %v", err)
	}
	if entry["path"] != "/transactions" {
		t.Fatalf("expected query-free path, got %+v", entry["path"])
	}
	if bytes.Contains(buf.Bytes(), []byte("token=secret")) {
		t.Fatalf("access log leaked query string: %s", buf.String())
	}
}
