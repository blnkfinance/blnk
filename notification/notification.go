package notification

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jerry-enebeli/blnk/request"

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

	payload, err := request.ToJsonReq(&data)

	if err != nil {
		log.Println(err)
	}

	req, err := http.NewRequest("POST", "https://hooks.slack.com/services/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", payload)

	if err != nil {
		log.Println(err)
	}

	var response map[string]interface{}

	err = request.Call(req, &response)
	if err != nil {
		log.Println(err)
	}

}

func WebhookNotification(systemError error) {
	conf, err := config.Fetch()
	if err != nil {
		log.Println(err)
	}

	data := map[string]interface{}{"error": systemError.Error()}
	payload, err := request.ToJsonReq(&data)

	if err != nil {
		log.Println(err)
	}

	req, err := http.NewRequest("POST", conf.Notification.WebHook.URL, payload)

	for i, i2 := range conf.Notification.WebHook.Headers {
		req.Header.Set(i, i2)
	}

	if err != nil {
		log.Println(err)
	}

	var response map[string]interface{}

	err = request.Call(req, &response)
	if err != nil {
		log.Println(err)
	}
}

func NotifyError(systemError error) {

	conf, err := config.Fetch()
	if err != nil {
		log.Println(err)
	}

	fmt.Println("got here", conf)
	if conf.Notification.Slack.WebhookURL != "" {
		SlackNotification(systemError)
	}

	if conf.Notification.WebHook.URL != "" {
		WebhookNotification(systemError)
	}

}
