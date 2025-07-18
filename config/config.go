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
	"time"

	"github.com/kelseyhightower/envconfig"

	"github.com/sirupsen/logrus"
)

// Default constants
const (
	DEFAULT_PORT            = "5001"
	DEFAULT_CLEANUP_SEC     = 10800 // 3 hours in seconds
	DEFAULT_TYPESENSE_KEY   = "blnk-api-key"
	DEFAULT_MONITORING_PORT = "5004"
)

// Default values for different configurations
var (
	defaultTransaction = TransactionConfig{
		BatchSize:        100000,
		MaxQueueSize:     1000,
		MaxWorkers:       10,
		LockDuration:     30 * time.Minute,
		IndexQueuePrefix: "transactions",
	}

	defaultReconciliation = ReconciliationConfig{
		DefaultStrategy:  "one_to_one",
		ProgressInterval: 100,
		MaxRetries:       3,
		RetryDelay:       5 * time.Second,
	}

	defaultQueue = QueueConfig{
		TransactionQueue:    "new:transaction",
		WebhookQueue:        "new:webhook",
		IndexQueue:          "new:index",
		InflightExpiryQueue: "new:inflight-expiry",
		NumberOfQueues:      20,
		MonitoringPort:      DEFAULT_MONITORING_PORT,
	}
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
	Dns           string `json:"dns" envconfig:"BLNK_REDIS_DNS"`
	SkipTLSVerify bool   `json:"skip_tls_verify" envconfig:"BLNK_REDIS_SKIP_TLS_VERIFY"`
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
	WebhookUrl string `json:"webhook_url" envconfig:"BLNK_SLACK_WEBHOOK_URL"`
}

type WebhookConfig struct {
	Url     string            `json:"url" envconfig:"BLNK_WEBHOOK_URL"`
	Headers map[string]string `json:"headers" envconfig:"BLNK_WEBHOOK_HEADERS"`
}

type Notification struct {
	Slack   SlackWebhook  `json:"slack"`
	Webhook WebhookConfig `json:"webhook"`
}

type TransactionConfig struct {
	BatchSize        int           `json:"batch_size" envconfig:"BLNK_TRANSACTION_BATCH_SIZE"`
	MaxQueueSize     int           `json:"max_queue_size" envconfig:"BLNK_TRANSACTION_MAX_QUEUE_SIZE"`
	MaxWorkers       int           `json:"max_workers" envconfig:"BLNK_TRANSACTION_MAX_WORKERS"`
	LockDuration     time.Duration `json:"lock_duration" envconfig:"BLNK_TRANSACTION_LOCK_DURATION"`
	IndexQueuePrefix string        `json:"index_queue_prefix" envconfig:"BLNK_TRANSACTION_INDEX_QUEUE_PREFIX"`
}

type ReconciliationConfig struct {
	DefaultStrategy  string        `json:"default_strategy" envconfig:"BLNK_RECONCILIATION_DEFAULT_STRATEGY"`
	ProgressInterval int           `json:"progress_interval" envconfig:"BLNK_RECONCILIATION_PROGRESS_INTERVAL"`
	MaxRetries       int           `json:"max_retries" envconfig:"BLNK_RECONCILIATION_MAX_RETRIES"`
	RetryDelay       time.Duration `json:"retry_delay" envconfig:"BLNK_RECONCILIATION_RETRY_DELAY"`
}

type QueueConfig struct {
	TransactionQueue        string `json:"transaction_queue" envconfig:"BLNK_QUEUE_TRANSACTION"`
	WebhookQueue            string `json:"webhook_queue" envconfig:"BLNK_QUEUE_WEBHOOK"`
	IndexQueue              string `json:"index_queue" envconfig:"BLNK_QUEUE_INDEX"`
	InflightExpiryQueue     string `json:"inflight_expiry_queue" envconfig:"BLNK_QUEUE_INFLIGHT_EXPIRY"`
	NumberOfQueues          int    `json:"number_of_queues" envconfig:"BLNK_QUEUE_NUMBER_OF_QUEUES"`
	InsufficientFundRetries bool   `json:"insufficient_fund_retries" envconfig:"BLNK_QUEUE_INSUFFICIENT_FUND_RETRIES"`
	MaxRetryAttempts        int    `json:"max_retry_attempts" envconfig:"BLNK_QUEUE_MAX_RETRY_ATTEMPTS"`
	MonitoringPort          string `json:"monitoring_port" envconfig:"BLNK_QUEUE_MONITORING_PORT"`
}

type Configuration struct {
	ProjectName             string                        `json:"project_name" envconfig:"BLNK_PROJECT_NAME"`
	BackupDir               string                        `json:"backup_dir" envconfig:"BLNK_BACKUP_DIR"`
	AwsAccessKeyId          string                        `json:"aws_access_key_id" envconfig:"BLNK_AWS_ACCESS_KEY_ID"`
	S3Endpoint              string                        `json:"s3_endpoint" envconfig:"BLNK_S3_ENDPOINT"`
	AwsSecretAccessKey      string                        `json:"aws_secret_access_key" envconfig:"BLNK_AWS_SECRET_ACCESS_KEY"`
	S3BucketName            string                        `json:"s3_bucket_name" envconfig:"BLNK_S3_BUCKET_NAME"`
	S3Region                string                        `json:"s3_region" envconfig:"BLNK_S3_REGION"`
	Server                  ServerConfig                  `json:"server"`
	DataSource              DataSourceConfig              `json:"data_source"`
	Redis                   RedisConfig                   `json:"redis"`
	TypeSense               TypeSenseConfig               `json:"typesense"`
	TypeSenseKey            string                        `json:"type_sense_key" envconfig:"BLNK_TYPESENSE_KEY"`
	TokenizationSecret      string                        `json:"tokenization_secret" envconfig:"BLNK_TOKENIZATION_SECRET"`
	AccountNumberGeneration AccountNumberGenerationConfig `json:"account_number_generation"`
	Notification            Notification                  `json:"notification"`
	RateLimit               RateLimitConfig               `json:"rate_limit"`
	EnableTelemetry         bool                          `json:"enable_telemetry" envconfig:"BLNK_ENABLE_TELEMETRY"`
	Transaction             TransactionConfig             `json:"transaction"`
	Reconciliation          ReconciliationConfig          `json:"reconciliation"`
	Queue                   QueueConfig                   `json:"queue"`
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
	if err := cnf.validateRequiredFields(); err != nil {
		return err
	}

	cnf.setDefaultValues()
	cnf.trimWhitespace()
	cnf.setupRateLimiting()

	// Validate tokenization secret length (AES-256 requires 32 bytes)
	if len(cnf.TokenizationSecret) > 0 && len(cnf.TokenizationSecret) != 32 {
		log.Println("Warning: Tokenization secret should be 32 bytes for AES-256 encryption")
	}

	return nil
}

func (cnf *Configuration) validateRequiredFields() error {
	if cnf.DataSource.Dns == "" {
		return errors.New("data source DNS is required")
	}

	if cnf.Redis.Dns == "" {
		return errors.New("redis DNS is required")
	}

	return nil
}

func (cnf *Configuration) setDefaultValues() {
	// Project defaults
	if cnf.ProjectName == "" {
		cnf.ProjectName = "Blnk Server"
		log.Println("Warning: Project name is empty. Setting a default name.")
	}

	// Server defaults
	if cnf.Server.Port == "" {
		cnf.Server.Port = DEFAULT_PORT
		log.Printf("Warning: Port not specified in config. Setting default port: %s", DEFAULT_PORT)
	}

	if cnf.TypeSenseKey == "" {
		cnf.TypeSenseKey = DEFAULT_TYPESENSE_KEY
	}

	// Tokenization defaults
	if cnf.TokenizationSecret == "" {
		cnf.TokenizationSecret = "blnk-default-tokenization-key!!!!"
		log.Println("Warning: No tokenization secret provided. Using default key. This is NOT recommended for production.")
	}

	// Set module defaults
	cnf.setTransactionDefaults()
	cnf.setReconciliationDefaults()
	cnf.setQueueDefaults()

	// Enable telemetry by default
	if !cnf.EnableTelemetry {
		cnf.EnableTelemetry = true
		log.Println("Warning: Telemetry setting not specified. Enabling by default.")
	}
}

func (cnf *Configuration) setTransactionDefaults() {
	if cnf.Transaction.BatchSize == 0 {
		cnf.Transaction.BatchSize = defaultTransaction.BatchSize
	}
	if cnf.Transaction.MaxQueueSize == 0 {
		cnf.Transaction.MaxQueueSize = defaultTransaction.MaxQueueSize
	}
	if cnf.Transaction.MaxWorkers == 0 {
		cnf.Transaction.MaxWorkers = defaultTransaction.MaxWorkers
	}
	if cnf.Transaction.LockDuration == 0 {
		cnf.Transaction.LockDuration = defaultTransaction.LockDuration
	} else {
		cnf.Transaction.LockDuration = cnf.Transaction.LockDuration * time.Second
	}
	if cnf.Transaction.IndexQueuePrefix == "" {
		cnf.Transaction.IndexQueuePrefix = defaultTransaction.IndexQueuePrefix
	}
}

func (cnf *Configuration) setReconciliationDefaults() {
	if cnf.Reconciliation.DefaultStrategy == "" {
		cnf.Reconciliation.DefaultStrategy = defaultReconciliation.DefaultStrategy
	}
	if cnf.Reconciliation.ProgressInterval == 0 {
		cnf.Reconciliation.ProgressInterval = defaultReconciliation.ProgressInterval
	}
	if cnf.Reconciliation.MaxRetries == 0 {
		cnf.Reconciliation.MaxRetries = defaultReconciliation.MaxRetries
	}
	if cnf.Reconciliation.RetryDelay == 0 {
		cnf.Reconciliation.RetryDelay = defaultReconciliation.RetryDelay
	}
}

func (cnf *Configuration) setQueueDefaults() {
	if cnf.Queue.TransactionQueue == "" {
		cnf.Queue.TransactionQueue = defaultQueue.TransactionQueue
	}
	if cnf.Queue.WebhookQueue == "" {
		cnf.Queue.WebhookQueue = defaultQueue.WebhookQueue
	}
	if cnf.Queue.IndexQueue == "" {
		cnf.Queue.IndexQueue = defaultQueue.IndexQueue
	}
	if cnf.Queue.InflightExpiryQueue == "" {
		cnf.Queue.InflightExpiryQueue = defaultQueue.InflightExpiryQueue
	}
	if cnf.Queue.NumberOfQueues == 0 {
		cnf.Queue.NumberOfQueues = defaultQueue.NumberOfQueues
	}
	if cnf.Queue.MonitoringPort == "" {
		cnf.Queue.MonitoringPort = defaultQueue.MonitoringPort
	}
}

func (cnf *Configuration) trimWhitespace() {
	cnf.ProjectName = strings.TrimSpace(cnf.ProjectName)
	cnf.Server.Port = strings.TrimSpace(cnf.Server.Port)
	cnf.DataSource.Dns = strings.TrimSpace(cnf.DataSource.Dns)
	cnf.Redis.Dns = strings.TrimSpace(cnf.Redis.Dns)
}

func (cnf *Configuration) setupRateLimiting() {
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
	if cnf.RateLimit.CleanupIntervalSec == nil {
		defaultCleanup := DEFAULT_CLEANUP_SEC
		cnf.RateLimit.CleanupIntervalSec = &defaultCleanup
		log.Printf("Warning: Rate limit cleanup interval not specified. Setting default value: %d seconds", defaultCleanup)
	}
}

// MockConfig sets a mock configuration for testing purposes.
func MockConfig(mockConfig *Configuration) {
	err := mockConfig.validateAndAddDefaults()
	if err != nil {
		log.Printf("Error setting mock config: %v", err)
		return
	}
	ConfigStore.Store(mockConfig)
}

func logger() {
	logger := logrus.New()
	log.SetOutput(logger.Writer())
}
