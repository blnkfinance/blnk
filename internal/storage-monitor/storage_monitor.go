package storagemonitor

import (
	"fmt"
	"log"
	"sync"

	"github.com/shirou/gopsutil/disk"
)

// StorageLimitEvent represents the data sent when the storage limit is hit.
type StorageLimitEvent struct {
	Message string
}

// EventBroker handles the subscription and broadcasting of storage limit events.
type EventBroker struct {
	subscribers []chan StorageLimitEvent
	mu          sync.Mutex
}

var broker *EventBroker

func init() {
	broker = NewEventBroker()
}

// NewEventBroker initializes a new EventBroker.
func NewEventBroker() *EventBroker {
	return &EventBroker{}
}

// Subscribe adds a new subscriber to the broker.
func (b *EventBroker) Subscribe() chan StorageLimitEvent {
	b.mu.Lock()
	defer b.mu.Unlock()

	ch := make(chan StorageLimitEvent, 1) // Buffered channel
	b.subscribers = append(b.subscribers, ch)
	return ch
}

// Broadcast sends the event to all subscribers.
func (b *EventBroker) Broadcast(event StorageLimitEvent) {
	b.mu.Lock()
	defer b.mu.Unlock()

	for _, subscriber := range b.subscribers {
		// Non-blocking send with select
		select {
		case subscriber <- event:
		default:
			fmt.Println("Warning: subscriber channel is full. Event not sent.")
		}
	}
}

// checkDiskUsage checks the current disk usage and broadcasts an event if it exceeds the threshold.
func checkDiskUsage() {
	const diskUsageThreshold = 80.0
	usage, err := disk.Usage("/")
	if err != nil {
		log.Fatalf("Error getting disk usage: %v", err)
	}

	currentUsagePercent := usage.UsedPercent
	fmt.Printf("Current disk usage: %.2f%%\n", currentUsagePercent)

	if currentUsagePercent > diskUsageThreshold {
		broker.Broadcast(StorageLimitEvent{Message: "Disk usage exceeds threshold"})
	}
}

func startLoggerSubscriber(broker *EventBroker) {
	logSub := broker.Subscribe()
	go func() {
		for event := range logSub {
			log.Printf("Logger: %s\n", event.Message)
		}
	}()
}

func startAlertSystemSubscriber(broker *EventBroker) {
	alertSub := broker.Subscribe()
	go func() {
		for event := range alertSub {
			// Placeholder for alert sending logic
			fmt.Printf("Alert sent: %s\n", event.Message)
		}
	}()
}
