package config

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

//todo validate config before load. check for empty values in required fields(data source), trim white spaces, set default values(port)

const (
	DEFAULT_PORT = "5001"
)

var configStore atomic.Value

type Configuration struct {
	Port                  string `json:"port"`
	ProjectName           string `json:"project_name"`
	DefaultCurrency       string `json:"default_currency"`
	EndPointSecret        string `json:"end_point_secret"`
	SynchronizationMethod string `json:"synchronization_method"`
	DataSource            struct {
		Name string `json:"name"`
		Dns  string `json:"dns"`
	} `json:"data_source"`
	Account struct {
		DefaultBank string `json:"default_bank"`
	} `json:"account"`
	AccountNumberGeneration struct {
		HttpService struct {
			Url     string `json:"url"`
			Method  string `json:"method"`
			Headers struct {
				ContentType   string `json:"Content-Type"`
				Authorization string `json:"Authorization"`
			} `json:"headers"`
		} `json:"http_service"`
	} `json:"account_number_generation"`
	ConfluentKafka struct {
		Server       string `json:"server"`
		ApiKey       string `json:"api_key"`
		SecretKey    string `json:"secret_key"`
		QueueName    string `json:"queue_name"`
		PullWaitTime int    `json:"pull_wait_time"`
	} `json:"confluent_kafka"`
	Notification struct {
		Slack struct {
			WebhookUrl string `json:"webhook_url"`
		} `json:"slack"`
		Webhook struct {
			Url     string `json:"url"`
			Headers struct {
			} `json:"headers"`
		} `json:"webhook"`
	} `json:"notification"`
}

func loadConfigFromFile(file string) error {
	if file == "" {
		return errors.New("config json not passed")
	}
	var cnf Configuration
	_, err := os.Stat(file)
	if err != nil {
		return err
	}

	f, err := os.Open(file)

	if err != nil {
		return err
	}

	err = json.NewDecoder(f).Decode(&cnf)
	if err != nil {
		return err
	}

	err = validateAndAddDefaults(&cnf)
	if err != nil {
		return err
	}
	configStore.Store(&cnf)
	return err
}

func InitConfig(configFile string) error {
	logger()
	return loadConfigFromFile(configFile)
}

func Fetch() (*Configuration, error) {
	config := configStore.Load()
	c, ok := config.(*Configuration)
	if !ok {
		return nil, errors.New("config not loaded from file. Create a json file called blnk.json with your config ❌")
	}

	return c, nil
}

func validateAndAddDefaults(cnf *Configuration) error {
	// Check for empty values in required fields
	if cnf.ProjectName == "" {
		log.Println("Warning: Project name is empty. Setting a default name.")
		cnf.ProjectName = "Blnk Server"
	}

	if cnf.EndPointSecret == "" {
		log.Println("Error: Endpoint secret is empty. It's a required field.")
		return errors.New("Endpoint secret is required")
	}

	if cnf.ConfluentKafka.Server == "" {
		log.Println("Error: Confluent Kafka server is empty. It's a required field.")
		return errors.New("Confluent Kafka server is required")
	}

	if cnf.DataSource.Name == "" {
		log.Println("Error: Data source name is empty. It's a required field.")
		return errors.New("Data source name is required")
	}

	if cnf.DataSource.Dns == "" {
		log.Println("Error: Data source DNS is empty. It's a required field.")
		return errors.New("Data source DNS is required")
	}

	// Trim white spaces from fields
	cnf.ProjectName = strings.TrimSpace(cnf.ProjectName)
	cnf.Port = strings.TrimSpace(cnf.Port)
	cnf.DefaultCurrency = strings.TrimSpace(cnf.DefaultCurrency)
	cnf.EndPointSecret = strings.TrimSpace(cnf.EndPointSecret)
	cnf.ConfluentKafka.Server = strings.TrimSpace(cnf.ConfluentKafka.Server)
	cnf.ConfluentKafka.ApiKey = strings.TrimSpace(cnf.ConfluentKafka.ApiKey)
	cnf.ConfluentKafka.SecretKey = strings.TrimSpace(cnf.ConfluentKafka.SecretKey)
	cnf.ConfluentKafka.QueueName = strings.TrimSpace(cnf.ConfluentKafka.QueueName)
	cnf.DataSource.Name = strings.TrimSpace(cnf.DataSource.Name)
	cnf.DataSource.Dns = strings.TrimSpace(cnf.DataSource.Dns)

	// Set default value for Port if it's empty
	if cnf.Port == "" {
		cnf.Port = DEFAULT_PORT
		log.Printf("Warning: Port not specified in config. Setting default port: %s", DEFAULT_PORT)
	}

	return nil
}

func logger() {
	logger := logrus.New()
	log.SetOutput(logger.Writer())
}
