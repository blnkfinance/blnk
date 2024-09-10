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

func ToJsonReq(payload interface{}) (*bytes.Buffer, error) {

	c, e := json.Marshal(payload)

	if e != nil {

		return nil, e

	}

	bytePayload := bytes.NewBuffer(c)

	return bytePayload, nil

}

func Call(req *http.Request, response interface{}) (*http.Response, error) {
	req.Header.Set("Content-Type", "application/json")
	client := &http.Client{}

	resp, err := client.Do(req)
	if err != nil {
		return resp, err
	}

	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		return resp, err
	}
	return resp, err
}

func BasicAuth(username, password string) string {
	auth := username + ":" + password
	return base64.StdEncoding.EncodeToString([]byte(auth))
}
