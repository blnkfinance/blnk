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

package middleware

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRequestSizeLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)

	const limit = 1024 // 1 KiB

	newRouter := func() *gin.Engine {
		r := gin.New()
		r.Use(RequestSizeLimit(limit))
		r.POST("/echo", func(c *gin.Context) {
			body, err := io.ReadAll(c.Request.Body)
			if err != nil {
				c.String(http.StatusRequestEntityTooLarge, "too large")
				return
			}
			c.String(http.StatusOK, "read %d bytes", len(body))
		})
		return r
	}

	t.Run("body within the limit is read normally", func(t *testing.T) {
		r := newRouter()
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/echo", strings.NewReader(strings.Repeat("a", 512)))
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})

	t.Run("body over the limit cannot be fully read", func(t *testing.T) {
		r := newRouter()
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/echo", strings.NewReader(strings.Repeat("a", limit*4)))
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusRequestEntityTooLarge, w.Code,
			"an oversized body must be rejected, not read into memory")
	})

	t.Run("multipart uploads are not capped by this middleware", func(t *testing.T) {
		r := newRouter()
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/echo", strings.NewReader(strings.Repeat("a", limit*4)))
		req.Header.Set("Content-Type", "multipart/form-data; boundary=x")
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code, "uploads enforce their own larger limit")
	})

	t.Run("non-positive limit disables the cap", func(t *testing.T) {
		r := gin.New()
		r.Use(RequestSizeLimit(0))
		r.POST("/echo", func(c *gin.Context) {
			body, err := io.ReadAll(c.Request.Body)
			require.NoError(t, err)
			c.String(http.StatusOK, "read %d bytes", len(body))
		})
		w := httptest.NewRecorder()
		req, _ := http.NewRequest("POST", "/echo", strings.NewReader(strings.Repeat("a", 100000)))
		r.ServeHTTP(w, req)
		assert.Equal(t, http.StatusOK, w.Code)
	})
}
