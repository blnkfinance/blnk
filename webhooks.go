package blnk

import (
	"bytes"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/jerry-enebeli/blnk/config"
	"golang.org/x/net/context"

	"github.com/hibiken/asynq"
)

type NewWebhook struct {
	Event   string      `json:"event"`
	Payload interface{} `json:"data"`
}

func getEventFromStatus(status string) string {
	switch strings.ToLower(status) {
	case strings.ToLower(StatusQueued):
		return "transaction.queued"
	case strings.ToLower(StatusApplied):
		return "transaction.applied"
	case strings.ToLower(StatusScheduled):
		return "transaction.scheduled"
	case strings.ToLower(StatusInflight):
		return "transaction.inflight"
	case strings.ToLower(StatusVoid):
		return "transaction.void"
	case strings.ToLower(StatusRejected):
		return "transaction.rejected"
	default:
		return "transaction.unknown"
	}
}

func processHTTP(data NewWebhook) error {
	conf, err := config.Fetch()
	if err != nil {
		log.Println("Error fetching config:", err)
		return err
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		log.Println("Error marshaling data:", err)
		return err
	}
	payload := bytes.NewBuffer(jsonData)

	req, err := http.NewRequest("POST", conf.Notification.Webhook.Url, payload)
	if err != nil {
		log.Println("Error creating request:", err)
		return err
	}

	for key, value := range conf.Notification.Webhook.Headers {
		req.Header.Set(key, value)
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println("Error sending request:", err)
		return err
	}
	defer func(Body io.ReadCloser) {
		err := Body.Close()
		if err != nil {
			logrus.Error(err)
		}
	}(resp.Body)

	// Check if the status code is not in the 2XX success range
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		log.Printf("Request failed with status code: %d\n", resp.StatusCode)
		return nil
	}

	var response map[string]interface{}
	err = json.NewDecoder(resp.Body).Decode(&response)
	if err != nil {
		log.Println("Error decoding response:", err)
		return err
	}

	log.Println("Webhook notification sent successfully:", response)

	return nil
}

func SendWebhook(newWebhook NewWebhook) error {
	conf, err := config.Fetch()
	if err != nil {
		return err
	}

	if conf.Notification.Webhook.Url == "" {
		return nil
	}

	client := asynq.NewClient(asynq.RedisClientOpt{Addr: conf.Redis.Dns})
	payload, err := json.Marshal(newWebhook)
	if err != nil {
		log.Fatal(err)

		return err
	}
	taskOptions := []asynq.Option{asynq.Queue(WEBHOOK_QUEUE)}
	task := asynq.NewTask(WEBHOOK_QUEUE, payload, taskOptions...)
	info, err := client.Enqueue(task)
	if err != nil {
		log.Println(err, info)
		return err
	}
	return err
}

func ProcessWebhook(_ context.Context, task *asynq.Task) error {
	conf, err := config.Fetch()
	if err != nil {
		return err
	}

	if conf.Notification.Webhook.Url == "" {
		return nil
	}
	var payload NewWebhook
	if err := json.Unmarshal(task.Payload(), &payload); err != nil {
		log.Printf("Error unmarshaling task payload: %v", err)
		return err
	}
	log.Printf("Processing webhook: %+v\n", payload.Event)
	err = processHTTP(payload)
	if err != nil {
		return err
	}
	return nil
}
