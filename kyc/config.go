package kyc

import (
	"os"

	"gopkg.in/yaml.v3"
)

type ProviderConfig struct {
	Name            string          `yaml:"name"`
	Enabled         bool            `yaml:"enabled"`
	APIKey          string          `yaml:"api_key"`
	APISecret       string          `yaml:"api_secret,omitempty"`
	AuthType        string          `yaml:"auth_type"`
	AuthHeader      string          `yaml:"auth_header"`
	BaseURL         string          `yaml:"base_url"`
	Endpoints       EndpointsConfig `yaml:"endpoints"`
	RequestConfig   RequestConfig   `yaml:"request_config,omitempty"`
	ResponseMapping ResponseMapping `yaml:"response_mapping"`
}

type EndpointsConfig struct {
	CreateCheck    string `yaml:"create_check"`
	GetStatus      string `yaml:"get_status"`
	UploadDocument string `yaml:"upload_document,omitempty"`
}

type RequestConfig struct {
	ContentType          string            `yaml:"content_type,omitempty"`
	IdentityFieldMapping map[string]string `yaml:"identity_field_mapping,omitempty"`
}

type ResponseMapping struct {
	StatusField    string   `yaml:"status_field"`
	ReferenceField string   `yaml:"reference_field"`
	VerifiedValues []string `yaml:"verified_values"`
	FailedValues   []string `yaml:"failed_values"`
	PendingValues  []string `yaml:"pending_values"`
	ReviewValues   []string `yaml:"review_values"`
}

type KYCConfig struct {
	Providers []ProviderConfig `yaml:"providers"`
}

func LoadConfig(filepath string) (*KYCConfig, error) {
	data, err := os.ReadFile(filepath)
	if err != nil {
		return nil, err
	}

	var config KYCConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}

	return &config, nil
}

func LoadConfigFromBytes(data []byte) (*KYCConfig, error) {
	var config KYCConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, err
	}
	return &config, nil
}
