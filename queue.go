package blnk

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/jerry-enebeli/blnk/model"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jerry-enebeli/blnk/config"
)

var (
	producerInstance *kafka.Producer
	consumerInstance *kafka.Consumer
	mutex            sync.Mutex
	producerConfig   *kafka.ConfigMap
	consumerConfig   *kafka.ConfigMap
)

func getProducer(config *kafka.ConfigMap) (*kafka.Producer, error) {
	if producerInstance != nil {
		return producerInstance, nil
	}

	mutex.Lock()
	defer mutex.Unlock()

	if producerInstance == nil {
		p, err := kafka.NewProducer(config)
		if err != nil {
			return nil, err
		}
		producerInstance = p
	}

	return producerInstance, nil
}

func getConsumer(config *kafka.ConfigMap) (*kafka.Consumer, error) {
	if consumerInstance != nil {
		return consumerInstance, nil
	}

	mutex.Lock()
	defer mutex.Unlock()

	if consumerInstance == nil {
		c, err := kafka.NewConsumer(config)
		if err != nil {
			return nil, err
		}
		consumerInstance = c
	}

	return consumerInstance, nil
}

func initKafkaConfigs() {
	cnf, err := config.Fetch()
	if err != nil {
		return
	}
	producerConfig = &kafka.ConfigMap{
		"bootstrap.servers":  cnf.ConfluentKafka.Server,
		"sasl.mechanisms":    "PLAIN",
		"session.timeout.ms": 45000,
		"sasl.password":      cnf.ConfluentKafka.SecretKey,
		"sasl.username":      cnf.ConfluentKafka.ApiKey,
		"security.protocol":  "SASL_SSL",
		"acks":               "all",
	}

	consumerConfig = &kafka.ConfigMap{
		"bootstrap.servers":  cnf.ConfluentKafka.Server,
		"sasl.mechanisms":    "PLAIN",
		"session.timeout.ms": 45000,
		"sasl.password":      cnf.ConfluentKafka.SecretKey,
		"sasl.username":      cnf.ConfluentKafka.ApiKey,
		"security.protocol":  "SASL_SSL",
		"group.id":           "blnk-transactions",
		"auto.offset.reset":  "earliest",
	}
}

func readConfig() kafka.ConfigMap {
	if producerConfig == nil {
		initKafkaConfigs()
	}
	return *producerConfig

}

func enqueueKafka(transaction model.Transaction) error {
	readConfig()
	topic, err := getQueueName()
	if err != nil {
		return err
	}
	p, err := getProducer(producerConfig)
	if err != nil {
		return err
	}

	txnJSON, err := transaction.ToJSON()
	if err != nil {
		return err
	}

	err = p.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Key:            []byte(transaction.BalanceID), //ensures all transactions belonging to a balance goes to the same partition. this way they are consumed together and in sequence
		Value:          txnJSON,
	}, nil)

	if err != nil {
		return err
	}

	// Go-routine to handle message delivery reports and
	// possibly other event types (errors, stats, etc)
	go func() {
		for e := range p.Events() {
			switch ev := e.(type) {
			case *kafka.Message:
				if ev.TopicPartition.Error != nil {
					fmt.Printf("Failed to deliver message: %v\n", ev.TopicPartition)
				} else {
					fmt.Printf("Produced event to topic %s: key = %-10s value = %s\n",
						*ev.TopicPartition.Topic, string(ev.Key), string(ev.Value))
				}
			}
		}
	}()

	return nil
}

func Enqueue(transaction model.Transaction) error {
	cnf, err := config.Fetch()
	if err != nil {
		return err
	}
	if cnf.Queue.Queue == "kafka" {
		err := enqueueKafka(transaction)
		if err != nil {
			return err
		}
	}
	return nil
}

func dequeueKafka(messageChan chan model.Transaction) error {
	readConfig()
	c, err := getConsumer(consumerConfig)
	if err != nil {
		fmt.Printf("Failed to create consumer: %s", err)
	}
	topic, err := getQueueName()
	if err != nil {
		log.Printf("Error: Error getting queue name: %v", err)
	}
	err = c.SubscribeTopics([]string{topic}, nil)
	if err != nil {
		return err
	}
	for {
		ev := c.Poll(100)
		switch e := ev.(type) {
		case *kafka.Message:
			var transaction model.Transaction
			err := json.Unmarshal(e.Value, &transaction)
			if err != nil {
				return err
			}
			messageChan <- transaction
		case kafka.Error:
			fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
		default:
			//fmt.Printf("Ignored %v\n", e)
		}
	}
}

// dequeueDB fetches queued transactions from db
// process each transaction
func dequeueDB(messageChan chan model.Transaction, l Blnk) error {
	for {
		transaction, err := l.datasource.GetNextQueuedTransaction()
		if err != nil {
			return err
		}

		if transaction != nil {
			messageChan <- *transaction
		}

	}
}

func Dequeue(messageChan chan model.Transaction, l Blnk) error {
	cnf, err := config.Fetch()
	if err != nil {
		return err
	}
	if cnf.Queue.Queue == "db" {
		err := dequeueDB(messageChan, l)
		if err != nil {
			return err
		}
	} else if cnf.Queue.Queue == "kafka" {
		err := dequeueKafka(messageChan)
		if err != nil {
			return err
		}
	}
	return nil
}
