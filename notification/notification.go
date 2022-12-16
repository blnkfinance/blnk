package notification

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/jerry-enebeli/saifu/request"

	"github.com/jerry-enebeli/saifu/config"
)

func SlackNotification(err error) {

	data := json.RawMessage(fmt.Sprintf(`{
	"blocks": [
		{
			"type": "header",
			"text": {
				"type": "plain_text",
				"text": "Error From Saifu 🐞",
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

func WebhookNotification(err error) {
	conf, err := config.Fetch()
	if err != nil {
		log.Println(err)
	}

	data := map[string]interface{}{"error": err.Error()}
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
	log.Println(systemError)

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
