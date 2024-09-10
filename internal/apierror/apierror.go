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

type ErrorCode string

const (
	ErrNotFound       ErrorCode = "NOT_FOUND"
	ErrConflict       ErrorCode = "CONFLICT"
	ErrBadRequest     ErrorCode = "BAD_REQUEST"
	ErrInvalidInput   ErrorCode = "INVALID_INPUT"
	ErrInternalServer ErrorCode = "INTERNAL_SERVER_ERROR"
)

type APIError struct {
	Code    ErrorCode   `json:"code"`
	Message string      `json:"message"`
	Details interface{} `json:"details,omitempty"`
}

func (e APIError) Error() string {
	return fmt.Sprintf("%s: %s", e.Code, e.Message)
}

func NewAPIError(code ErrorCode, message string, details interface{}) APIError {
	logrus.Error(details)
	return APIError{
		Code:    code,
		Message: message,
		Details: details,
	}
}

func MapErrorToHTTPStatus(err error) int {
	if apiErr, ok := err.(APIError); ok {
		switch apiErr.Code {
		case ErrNotFound:
			return http.StatusNotFound
		case ErrConflict:
			return http.StatusConflict
		case ErrInvalidInput:
			return http.StatusBadRequest
		case ErrInternalServer:
			return http.StatusInternalServerError
		default:
			return http.StatusInternalServerError
		}
	}
	return http.StatusInternalServerError
}
