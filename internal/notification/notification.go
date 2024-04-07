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
