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

package request

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
)

// ToJsonReq converts a Go object to a JSON-encoded HTTP request payload.
// It serializes the provided payload to JSON format and wraps it in a buffer for sending in HTTP requests.
//
// Parameters:
// - payload interface{}: The data structure to be serialized into JSON.
//
// Returns:
// - *bytes.Buffer: The JSON-encoded payload wrapped in a bytes buffer, ready to be sent in a request.
// - error: An error if the JSON marshalling process fails.
func ToJsonReq(payload interface{}) (*bytes.Buffer, error) {
	// Marshal the payload into a JSON byte slice
	c, e := json.Marshal(payload)
	if e != nil {
		return nil, e
	}

	// Wrap the JSON byte slice into a bytes buffer and return
	bytePayload := bytes.NewBuffer(c)
	return bytePayload, nil
}

// Call makes an HTTP request using the provided request object and decodes the response into the specified structure.
// It automatically sets the request Content-Type to application/json and decodes the JSON response body into the provided response interface.
//
// Parameters:
// - req *http.Request: The prepared HTTP request to send.
// - response interface{}: The target structure to hold the decoded JSON response.
//
// Returns:
// - *http.Response: The raw HTTP response object.
// - error: An error if the HTTP request or JSON decoding fails.
func Call(req *http.Request, response interface{}) (*http.Response, error) {
	// Set request content type to JSON
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}

	// Send the HTTP request and capture the response
	resp, err := client.Do(req)
	if err != nil {
		return resp, err
	}

	// Decode the JSON response into the provided response structure
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return resp, err
	}
	return resp, err
}

// BasicAuth generates a basic HTTP authentication string by encoding the provided username and password.
// The string is base64-encoded in the format "username:password".
//
// Parameters:
// - username string: The username for basic authentication.
// - password string: The password for basic authentication.
//
// Returns:
// - string: A base64-encoded string in the format "Basic <base64-encoded-credentials>".
func BasicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}
