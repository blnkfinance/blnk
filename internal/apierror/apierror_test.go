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

package apierror_test

import (
	"errors"
	"net/http"
	"testing"

	"github.com/jerry-enebeli/blnk/internal/apierror"
	"github.com/stretchr/testify/assert"
)

func TestNewAPIError(t *testing.T) {
	details := "Some internal error details"
	apiErr := apierror.NewAPIError(apierror.ErrInternalServer, "Something went wrong", details)

	assert.Equal(t, apierror.ErrInternalServer, apiErr.Code)
	assert.Equal(t, "Something went wrong", apiErr.Message)
	assert.Equal(t, details, apiErr.Details)
	assert.Equal(t, "INTERNAL_SERVER_ERROR: Something went wrong", apiErr.Error())
}

func TestMapErrorToHTTPStatus(t *testing.T) {
	tests := []struct {
		name     string
		err      error
		expected int
	}{
		{
			name:     "NotFound Error",
			err:      apierror.NewAPIError(apierror.ErrNotFound, "Resource not found", nil),
			expected: http.StatusNotFound,
		},
		{
			name:     "Conflict Error",
			err:      apierror.NewAPIError(apierror.ErrConflict, "Conflict occurred", nil),
			expected: http.StatusConflict,
		},
		{
			name:     "InvalidInput Error",
			err:      apierror.NewAPIError(apierror.ErrInvalidInput, "Invalid input", nil),
			expected: http.StatusBadRequest,
		},
		{
			name:     "InternalServerError",
			err:      apierror.NewAPIError(apierror.ErrInternalServer, "Internal server error", nil),
			expected: http.StatusInternalServerError,
		},
		{
			name:     "Unknown Error",
			err:      errors.New("Unknown error"),
			expected: http.StatusInternalServerError,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			statusCode := apierror.MapErrorToHTTPStatus(tt.err)
			assert.Equal(t, tt.expected, statusCode)
		})
	}
}
