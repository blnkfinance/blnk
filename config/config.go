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

type AccountNumberGenerationConfig struct {
	EnableAutoGeneration bool `json:"enable_auto_generation"`
	HttpService          struct {
		Url     string `json:"url"`
		Timeout int    `json:"timeout"`
		Headers struct {
			Authorization string `json:"Authorization"`
		} `json:"headers"`
	} `json:"http_service"`
}

type Notification struct {
	Slack struct {
		WebhookUrl string `json:"webhook_url"`
	} `json:"slack"`
	Webhook struct {
		Url     string            `json:"url"`
		Headers map[string]string `json:"headers"`
	} `json:"webhook"`
}

type OtelGrafanaCloud struct {
	OtelExporterOtlpProtocol string `json:"OTEL_EXPORTER_OTLP_PROTOCOL"`
	OtelExporterOtlpEndpoint string `json:"OTEL_EXPORTER_OTLP_ENDPOINT"`
	OtelExporterOtlpHeaders  string `json:"OTEL_EXPORTER_OTLP_HEADERS"`
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
	OtelGrafanaCloud        OtelGrafanaCloud              `json:"otel_grafana_cloud"`
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

func SetGrafanaExporterEnvs() error {
	cnf, err := Fetch()
	if err != nil {
		return err
	}
	err = os.Setenv("OTEL_EXPORTER_OTLP_PROTOCOL", cnf.OtelGrafanaCloud.OtelExporterOtlpProtocol)
	if err != nil {
		return err
	}
	err = os.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", cnf.OtelGrafanaCloud.OtelExporterOtlpEndpoint)
	if err != nil {
		return err
	}
	err = os.Setenv("OTEL_EXPORTER_OTLP_HEADERS", cnf.OtelGrafanaCloud.OtelExporterOtlpHeaders)
	if err != nil {
		return err
	}

	return nil
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

	return nil
}

// MockConfig sets a mock configuration for testing purposes.
func MockConfig(enableAutoGeneration bool, url string, authorizationToken string) {
	mockConfig := Configuration{
		ProjectName: "",
		Redis:       RedisConfig{Dns: "localhost:6379"},
		DataSource:  DataSourceConfig{Dns: "postgres://postgres:@localhost:5432/blnk?sslmode=disable"},
		AccountNumberGeneration: AccountNumberGenerationConfig{
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
	}

	ConfigStore.Store(&mockConfig)
}

func logger() {
	logger := logrus.New()
	log.SetOutput(logger.Writer())
}
