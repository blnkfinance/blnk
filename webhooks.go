/*
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
package blnk

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/sirupsen/logrus"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/hibiken/asynq"
)

// NewWebhook represents the structure of a webhook notification.
// It includes an event type and associated payload data.
type NewWebhook struct {
	Event   string      `json:"event"` // The event type that triggered the webhook.
	Payload interface{} `json:"data"`  // The data associated with the event.
}

// getEventFromStatus maps a transaction status to a corresponding event string.
//
// Parameters:
// - status string: The status of the transaction.
//
// Returns:
// - string: The corresponding event string for the transaction status.
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

// processHTTP sends a webhook notification via HTTP POST request.
//
// Parameters:
// - data NewWebhook: The webhook notification data to send.
//
// Returns:
// - error: An error if the request or processing fails.
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

// SendWebhook enqueues a webhook notification task.
//
// Parameters:
// - newWebhook NewWebhook: The webhook notification data to enqueue.
//
// Returns:
// - error: An error if the task could not be enqueued.
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

// ProcessWebhook processes a webhook notification task from the queue.
//
// Parameters:
// - _ context.Context: The context for the operation.
// - task *asynq.Task: The task containing the webhook notification data.
//
// Returns:
// - error: An error if the webhook processing fails.
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
