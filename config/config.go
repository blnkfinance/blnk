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

const (
	DEFAULT_PORT = "5001"
)

var configStore atomic.Value

type Configuration struct {
	Port        string `json:"port"`
	ProjectName string `json:"project_name"`
	DataSource  struct {
		Dns string `json:"dns"`
	} `json:"data_source"`
	Redis struct {
		Dns string `json:"dns"`
	}
	AccountNumberGeneration struct {
		EnableAutoGeneration bool `json:"enable_auto_generation"`
		HttpService          struct {
			Url     string `json:"url"`
			Timeout int    `json:"timeout"`
			Headers struct {
				Authorization string `json:"Authorization"`
			} `json:"headers"`
		} `json:"http_service"`
	} `json:"account_number_generation"`
	Queue struct {
		Queue string
	} `json:"queue"`
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
			Url     string            `json:"url"`
			Headers map[string]string `json:"headers"`
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

	if cnf.DataSource.Dns == "" {
		log.Println("Error: Data source DNS is empty. It's a required field.")
		return errors.New("data source DNS is required")
	}

	if cnf.Redis.Dns == "" {
		log.Println("Error: Redis DNS is empty. It's a required field.")
		return errors.New("redis DNS is required")
	}

	// Trim white spaces from fields
	cnf.ProjectName = strings.TrimSpace(cnf.ProjectName)
	cnf.Port = strings.TrimSpace(cnf.Port)
	cnf.ConfluentKafka.Server = strings.TrimSpace(cnf.ConfluentKafka.Server)
	cnf.ConfluentKafka.ApiKey = strings.TrimSpace(cnf.ConfluentKafka.ApiKey)
	cnf.ConfluentKafka.SecretKey = strings.TrimSpace(cnf.ConfluentKafka.SecretKey)
	cnf.ConfluentKafka.QueueName = strings.TrimSpace(cnf.ConfluentKafka.QueueName)
	cnf.DataSource.Dns = strings.TrimSpace(cnf.DataSource.Dns)
	cnf.Redis.Dns = strings.TrimSpace(cnf.Redis.Dns)

	// Set default value for Port if it's empty
	if cnf.Port == "" {
		cnf.Port = DEFAULT_PORT
		log.Printf("Warning: Port not specified in config. Setting default port: %s", DEFAULT_PORT)
	}

	// Set default value for Queue if it's empty
	if cnf.Queue.Queue == "" {
		cnf.Queue.Queue = "db"
		log.Println("Warning: Queue was not specified in config. Setting default queue: DB Queue(Postgres)")
	}

	return nil
}

// MockConfig sets a mock configuration for testing purposes.
func MockConfig(enableAutoGeneration bool, url string, authorizationToken string) {
	mockConfig := Configuration{
		Port:        "",
		ProjectName: "",
		Redis: struct {
			Dns string `json:"dns"`
		}{Dns: "redis://localhost"},
		DataSource: struct {
			Dns string `json:"dns"`
		}{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		AccountNumberGeneration: struct {
			EnableAutoGeneration bool `json:"enable_auto_generation"`
			HttpService          struct {
				Url     string `json:"url"`
				Timeout int    `json:"timeout"`
				Headers struct {
					Authorization string `json:"Authorization"`
				} `json:"headers"`
			} `json:"http_service"`
		}{
			EnableAutoGeneration: enableAutoGeneration,
			HttpService: struct {
				Url     string `json:"url"`
				Timeout int    `json:"timeout"`
				Headers struct {
					Authorization string `json:"Authorization"`
				} `json:"headers"`
			}{
				Url: url,
				Headers: struct {
					Authorization string `json:"Authorization"`
				}{
					Authorization: authorizationToken,
				},
			},
		},
		Queue: struct {
			Queue string
		}{
			Queue: "db",
		},
		ConfluentKafka: struct {
			Server       string `json:"server"`
			ApiKey       string `json:"api_key"`
			SecretKey    string `json:"secret_key"`
			QueueName    string `json:"queue_name"`
			PullWaitTime int    `json:"pull_wait_time"`
		}{},
		Notification: struct {
			Slack struct {
				WebhookUrl string `json:"webhook_url"`
			} `json:"slack"`
			Webhook struct {
				Url     string            `json:"url"`
				Headers map[string]string `json:"headers"`
			} `json:"webhook"`
		}{},
	}

	configStore.Store(&mockConfig)
}

func logger() {
	logger := logrus.New()
	log.SetOutput(logger.Writer())
}
