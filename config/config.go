/*
Copyright 2024 Blnk Finance Authors.

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

package config

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"strings"
	"sync/atomic"

	"github.com/kelseyhightower/envconfig"

	"github.com/sirupsen/logrus"
)

const (
	DEFAULT_PORT = "5001"
)

var ConfigStore atomic.Value

type ServerConfig struct {
	SSL       bool   `json:"ssl" envconfig:"BLNK_SERVER_SSL"`
	Secure    bool   `json:"secure" envconfig:"BLNK_SERVER_SECURE"`
	SecretKey string `json:"secret_key" envconfig:"BLNK_SERVER_SECRET_KEY"`
	Domain    string `json:"domain" envconfig:"BLNK_SERVER_SSL_DOMAIN"`
	Email     string `json:"ssl_email" envconfig:"BLNK_SERVER_SSL_EMAIL"`
	Port      string `json:"port" envconfig:"BLNK_SERVER_PORT"`
}

type DataSourceConfig struct {
	Dns string `json:"dns" envconfig:"BLNK_DATA_SOURCE_DNS"`
}

type RedisConfig struct {
	Dns string `json:"dns" envconfig:"BLNK_REDIS_DNS"`
}

type TypeSenseConfig struct {
	Dns string `json:"dns" envconfig:"BLNK_TYPESENSE_DNS"`
}

type AccountGenerationHttpService struct {
	Url     string `json:"url"`
	Timeout int    `json:"timeout"`
	Headers struct {
		Authorization string `json:"Authorization"`
	} `json:"headers"`
}
type AccountNumberGenerationConfig struct {
	EnableAutoGeneration bool                         `json:"enable_auto_generation"`
	HttpService          AccountGenerationHttpService `json:"http_service"`
}

type RateLimitConfig struct {
	RequestsPerSecond  *float64 `json:"requests_per_second" envconfig:"BLNK_RATE_LIMIT_RPS"`
	Burst              *int     `json:"burst" envconfig:"BLNK_RATE_LIMIT_BURST"`
	CleanupIntervalSec *int     `json:"cleanup_interval_sec" envconfig:"BLNK_RATE_LIMIT_CLEANUP_INTERVAL_SEC"`
}

type SlackWebhook struct {
	WebhookUrl string `json:"webhook_url"`
}

type Notification struct {
	Slack   SlackWebhook `json:"slack"`
	Webhook struct {
		Url     string            `json:"url"`
		Headers map[string]string `json:"headers"`
	} `json:"webhook"`
}

type Configuration struct {
	ProjectName             string                        `json:"project_name" envconfig:"BLNK_PROJECT_NAME"`
	BackupDir               string                        `json:"backup_dir" envconfig:"BLNK_BACKUP_DIR"`
	AwsAccessKeyId          string                        `json:"aws_access_key_id"`
	S3Endpoint              string                        `json:"s3_endpoint"`
	AwsSecretAccessKey      string                        `json:"aws_secret_access_key"`
	S3BucketName            string                        `json:"s3_bucket_name"`
	S3Region                string                        `json:"s3_region"`
	Server                  ServerConfig                  `json:"server"`
	DataSource              DataSourceConfig              `json:"data_source"`
	Redis                   RedisConfig                   `json:"redis"`
	TypeSense               TypeSenseConfig               `json:"typesense"`
	TypeSenseKey            string                        `json:"type_sense_key"`
	AccountNumberGeneration AccountNumberGenerationConfig `json:"account_number_generation"`
	Notification            Notification                  `json:"notification"`
	RateLimit               RateLimitConfig               `json:"rate_limit"`
}

func loadConfigFromFile(file string) error {
	var cnf Configuration
	_, err := os.Stat(file)
	if err == nil {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		err = json.NewDecoder(f).Decode(&cnf)
		if err != nil {
			return err
		}

	} else if errors.Is(err, os.ErrNotExist) {
		log.Println("config json not passed, will use env variables")
	}

	// override config from environment variables
	err = envconfig.Process("blnk", &cnf)
	if err != nil {
		return err
	}

	err = cnf.validateAndAddDefaults()
	if err != nil {
		return err
	}

	ConfigStore.Store(&cnf)
	return err
}

func InitConfig(configFile string) error {
	logger()
	return loadConfigFromFile(configFile)
}

func Fetch() (*Configuration, error) {
	config := ConfigStore.Load()
	c, ok := config.(*Configuration)
	if !ok {
		return nil, errors.New("config not loaded from file. Create a json file called blnk.json with your config ❌")
	}
	return c, nil
}

func (cnf *Configuration) validateAndAddDefaults() error {
	if cnf.ProjectName == "" {
		log.Println("Warning: Project name is empty. Setting a default name.")
		cnf.ProjectName = "Blnk Server"
	}

	if cnf.TypeSense.Dns == "" {
		cnf.TypeSense.Dns = "http://typesense:8108"
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
	cnf.Server.Port = strings.TrimSpace(cnf.Server.Port)
	cnf.DataSource.Dns = strings.TrimSpace(cnf.DataSource.Dns)
	cnf.Redis.Dns = strings.TrimSpace(cnf.Redis.Dns)

	// Set default value for Port if it's empty
	if cnf.Server.Port == "" {
		cnf.Server.Port = DEFAULT_PORT
		log.Printf("Warning: Port not specified in config. Setting default port: %s", DEFAULT_PORT)
	}

	// Rate limiting is disabled by default (when both RPS and Burst are nil)
	if cnf.RateLimit.RequestsPerSecond != nil && cnf.RateLimit.Burst == nil {
		defaultBurst := 2 * int(*cnf.RateLimit.RequestsPerSecond)
		cnf.RateLimit.Burst = &defaultBurst
		log.Printf("Warning: Rate limit burst not specified. Setting default value: %d", defaultBurst)
	}
	if cnf.RateLimit.RequestsPerSecond == nil && cnf.RateLimit.Burst != nil {
		defaultRPS := float64(*cnf.RateLimit.Burst) / 2
		cnf.RateLimit.RequestsPerSecond = &defaultRPS
		log.Printf("Warning: Rate limit RPS not specified. Setting default value: %.2f", defaultRPS)
	}

	// Set default cleanup interval if not specified
	if cnf.RateLimit.CleanupIntervalSec == nil {
		defaultCleanup := 10800 // 3 hours in seconds
		cnf.RateLimit.CleanupIntervalSec = &defaultCleanup
		log.Printf("Warning: Rate limit cleanup interval not specified. Setting default value: %d seconds", defaultCleanup)
	}

	return nil
}

// MockConfig sets a mock configuration for testing purposes.
func MockConfig(mockConfig *Configuration) {
	ConfigStore.Store(mockConfig)
}

func logger() {
	logger := logrus.New()
	log.SetOutput(logger.Writer())
}
