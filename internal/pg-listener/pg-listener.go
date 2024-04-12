package pg_listener

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/lib/pq"
)

type NotificationHandler interface {
	HandleNotification(table string, data map[string]interface{}) error
}

type ListenerConfig struct {
	PgConnStr string
	Interval  time.Duration
	Timeout   time.Duration
}

type DBListener struct {
	config  ListenerConfig
	handler NotificationHandler
}

type NotificationPayload struct {
	Table string                 `json:"table"`
	Data  map[string]interface{} `json:"data"`
}

func NewDBListener(config ListenerConfig, handler NotificationHandler) *DBListener {
	return &DBListener{
		config:  config,
		handler: handler,
	}
}

func (d *DBListener) Start() {
	listener := pq.NewListener(d.config.PgConnStr, 10*time.Second, d.config.Timeout, func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Fprintf(os.Stderr, "Listener error: %v\n", err)
			return
		}
	})
	err := listener.Listen("data_change")
	if err != nil {
		log.Fatalf("Error listening to PostgreSQL channel: %v", err)
	}

	fmt.Println("Start listening for PostgreSQL notifications on channel 'data_change'...")

	for {
		d.waitForNotification(listener)
	}
}

func (d *DBListener) waitForNotification(listener *pq.Listener) {
	select {
	case notification := <-listener.Notify:
		d.handleNotification(notification)
	case <-time.After(90 * time.Second):
		fmt.Println("Checking for notifications...")
	}
}

func (d *DBListener) handleNotification(notification *pq.Notification) {
	var payload NotificationPayload
	err := json.Unmarshal([]byte(notification.Extra), &payload)
	if err != nil {
		log.Printf("Error unmarshalling notification payload: %v", err)
		return
	}

	// Handle null values and special cases in payload.Data
	for key, value := range payload.Data {
		if value == nil {
			payload.Data[key] = "no value"
		} else if key == "id" {
			if floatValue, ok := value.(float64); ok {
				payload.Data[key] = strconv.FormatFloat(floatValue, 'f', -1, 64)
			}
		}
	}

	if err := d.handler.HandleNotification(payload.Table, payload.Data); err != nil {
		log.Printf("Error handling notification: %v", err)
	}
}
