package pkg

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"sync"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/jerry-enebeli/blnk"
	config2 "github.com/jerry-enebeli/blnk/config"
)

var (
	producerInstance *kafka.Producer
	consumerInstance *kafka.Consumer
	mutex            sync.Mutex
	producerConfig   *kafka.ConfigMap
	consumerConfig   *kafka.ConfigMap
)

func GetProducer(config *kafka.ConfigMap) (*kafka.Producer, error) {
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

func GetConsumer(config *kafka.ConfigMap) (*kafka.Consumer, error) {
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
	config, err := config2.Fetch()
	if err != nil {
		return
	}
	producerConfig = &kafka.ConfigMap{
		"bootstrap.servers":  config.ConfluentKafka.Server,
		"sasl.mechanisms":    "PLAIN",
		"session.timeout.ms": 45000,
		"sasl.password":      config.ConfluentKafka.SecretKey,
		"sasl.username":      config.ConfluentKafka.APIKEY,
		"security.protocol":  "SASL_SSL",
		"acks":               "all",
	}

	consumerConfig = &kafka.ConfigMap{
		"bootstrap.servers":  config.ConfluentKafka.Server,
		"sasl.mechanisms":    "PLAIN",
		"session.timeout.ms": 45000,
		"sasl.password":      config.ConfluentKafka.SecretKey,
		"sasl.username":      config.ConfluentKafka.APIKEY,
		"security.protocol":  "SASL_SSL",
		"group.id":           "blnk-transactions",
		"auto.offset.reset":  "earliest",
	}
}
func ReadConfig() kafka.ConfigMap {
	if producerConfig == nil {
		initKafkaConfigs()
	}
	return *producerConfig

}

func Enqueue(transaction blnk.Transaction) error {
	ReadConfig()
	topic, err := getQueueName()
	if err != nil {
		return err
	}
	p, err := GetProducer(producerConfig)
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

func Dequeue(messageChan chan blnk.Transaction) error {
	ReadConfig()
	c, err := GetConsumer(consumerConfig)
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
			var transaction blnk.Transaction
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
