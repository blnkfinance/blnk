package blnk

import (
	"bytes"
	"context"
	"encoding/json"
	"log"
	"net/http"

	"github.com/jerry-enebeli/blnk/config"

	"github.com/hibiken/asynq"
)

type NewWebhook struct {
	Event   string      `json:"event"`
	Payload interface{} `json:"data"`
}

func processHTTP(data interface{}) error {
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
	defer resp.Body.Close()

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

func SendWebhook(data interface{}) error {
	conf, err := config.Fetch()
	if err != nil {
		return err
	}
	client := asynq.NewClient(asynq.RedisClientOpt{Addr: conf.Redis.Dns})

	payload, err := json.Marshal(data)
	if err != nil {
		log.Fatal(err)

		return err
	}

	task := asynq.NewTask(WEBHOOK_QUEUE, payload)

	info, err := client.Enqueue(task)
	if err != nil {
		log.Println(err, info)
		return err
	}
	return err
}

func ProcessWebhook(client *asynq.Client) {
	mux := asynq.NewServeMux()
	mux.HandleFunc(WEBHOOK_QUEUE, func(ctx context.Context, task *asynq.Task) error {
		var payload interface{} // Adjust the type according to your needs
		if err := json.Unmarshal(task.Payload(), &payload); err != nil {
			log.Printf("Error unmarshaling task payload: %v\n", err)
			return err
		}
		log.Printf("Processing payload: %+v\n", payload)
		processHTTP(payload)
		return nil
	})
	conf, err := config.Fetch()
	if err != nil {
		return
	}
	worker := asynq.NewServer(
		asynq.RedisClientOpt{Addr: conf.Redis.Dns},
		asynq.Config{
			Concurrency: 10,
		},
	)
	// Start the worker with the mux as the task handler
	if err := worker.Run(mux); err != nil {
		log.Fatalf("Could not run worker: %v\n", err)
	}
}
