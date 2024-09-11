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

package notification

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jerry-enebeli/blnk/internal/request"
	"github.com/sirupsen/logrus"

	"github.com/jerry-enebeli/blnk/config"
)

// SlackNotification sends an error message to a Slack webhook.
// It formats the error details and the current time into a Slack message payload.
//
// Parameters:
// - err: The error to be reported via Slack.
//
// The function retrieves configuration for the Slack webhook URL, formats the error,
// and sends it as a JSON payload to the Slack webhook.
func SlackNotification(err error) {
	// Format the Slack message payload using the error message and the current time
	data := json.RawMessage(fmt.Sprintf(`{
		"blocks": [
			{
				"type": "header",
				"text": {
					"type": "plain_text",
					"text": "Error From Blnk 🐞",
					"emoji": true
				}
			},
			{
				"type": "section",
				"fields": [
					{
						"type": "mrkdwn",
						"text": "*Error:*\n%v"
					}
				]
			},
			{
				"type": "section",
				"fields": [
					{
						"type": "mrkdwn",
						"text": "*Time:*\n%v"
					}
				]
			}
		]
	}`, err.Error(), time.Now().Format(time.RFC822)))

	// Fetch the configuration, including the Slack webhook URL
	conf, err := config.Fetch()
	if err != nil {
		log.Println(err)
		return
	}

	// Convert the Slack message to a JSON request payload
	payload, err := request.ToJsonReq(&data)
	if err != nil {
		log.Println(err)
		return
	}

	// Create an HTTP request to send the Slack notification
	req, err := http.NewRequest("POST", conf.Notification.Slack.WebhookUrl, payload)
	if err != nil {
		log.Println(err)
		return
	}

	// Send the request and handle the response
	var response map[string]interface{}
	_, err = request.Call(req, &response)
	if err != nil {
		log.Println(err)
	}
}

// NotifyError sends an error notification through the configured notification system.
// It logs the error locally and sends a notification via Slack (if configured).
//
// Parameters:
// - systemError: The error to notify.
//
// This function runs the notification process asynchronously using a goroutine to avoid blocking.
func NotifyError(systemError error) {
	go func(systemError error) {
		// Log the error locally using logrus
		logrus.Error(systemError)

		// Fetch the configuration
		conf, err := config.Fetch()
		if err != nil {
			log.Println(err)
			return
		}

		// If Slack is configured, send the error notification to Slack
		if conf.Notification.Slack.WebhookUrl != "" {
			SlackNotification(systemError)
		}
	}(systemError)
}
