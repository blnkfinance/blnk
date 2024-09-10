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

func SlackNotification(err error) {
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

	conf, err := config.Fetch()
	if err != nil {
		return
	}
	payload, err := request.ToJsonReq(&data)

	if err != nil {
		log.Println(err)
	}

	req, err := http.NewRequest("POST", conf.Notification.Slack.WebhookUrl, payload)

	if err != nil {
		log.Println(err)
	}

	var response map[string]interface{}

	_, err = request.Call(req, &response)
	if err != nil {
		log.Println(err)
	}

}

func NotifyError(systemError error) {
	go func(systemError error) {
		logrus.Error(systemError)
		conf, err := config.Fetch()
		if err != nil {
			log.Println(err)
		}

		if conf.Notification.Slack.WebhookUrl != "" {
			SlackNotification(systemError)
		}
	}(systemError)
}
