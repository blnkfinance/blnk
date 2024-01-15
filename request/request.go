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
