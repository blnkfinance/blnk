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

package apierror

import (
	"fmt"
	"net/http"

	"github.com/sirupsen/logrus"
)

// ErrorCode defines a string type to represent specific error codes used in the API.
type ErrorCode string

// Predefined error codes to represent different error conditions.
const (
	ErrNotFound       ErrorCode = "NOT_FOUND"             // Used when a requested resource is not found.
	ErrConflict       ErrorCode = "CONFLICT"              // Used when a request conflicts with the current state of the resource.
	ErrBadRequest     ErrorCode = "BAD_REQUEST"           // Used when a request contains invalid data or parameters.
	ErrInvalidInput   ErrorCode = "INVALID_INPUT"         // Used when the provided input does not meet the expected format or constraints.
	ErrInternalServer ErrorCode = "INTERNAL_SERVER_ERROR" // Used for general server errors that are not client-related.
)

// APIError represents a custom error structure for the API.
// It includes an error code, message, and optional details to provide additional context for the error.
type APIError struct {
	Code    ErrorCode   `json:"code"`              // The specific error code that identifies the type of error.
	Message string      `json:"message"`           // A human-readable message that describes the error.
	Details interface{} `json:"details,omitempty"` // Optional field for additional details or context about the error.
}

// Error implements the error interface for APIError.
// It returns a formatted string combining the error code and message.
func (e APIError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

// NewAPIError creates a new APIError instance.
// It logs the error details and returns the error object with the provided code, message, and additional details.
func NewAPIError(code ErrorCode, message string, details interface{}) APIError {
	logrus.Error(details) // Log the error details for monitoring and debugging.
	return APIError{
		Code:    code,
		Message: message,
		Details: details,
	}
}

// MapErrorToHTTPStatus maps APIError codes to appropriate HTTP status codes.
// It returns the corresponding HTTP status code for the given APIError.
func MapErrorToHTTPStatus(err error) int {
	if apiErr, ok := err.(APIError); ok {
		switch apiErr.Code {
		case ErrNotFound:
			return http.StatusNotFound // HTTP 404 Not Found for missing resources.
		case ErrConflict:
			return http.StatusConflict // HTTP 409 Conflict for conflicting requests.
		case ErrInvalidInput:
			return http.StatusBadRequest // HTTP 400 Bad Request for invalid inputs.
		case ErrInternalServer:
			return http.StatusInternalServerError // HTTP 500 Internal Server Error for server issues.
		default:
			return http.StatusInternalServerError
		}
	}
	return http.StatusInternalServerError // Default to 500 Internal Server Error if no specific mapping is found.
}
