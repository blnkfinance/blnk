package kyc

import (
	"fmt"
	"os"
	"strings"

	"github.com/sirupsen/logrus"
)

func (e *WorkflowEngine) LoadProvidersFromConfig(configPath string) error {
	config, err := LoadConfig(configPath)
	if err != nil {
		return fmt.Errorf("failed to load KYC config: %w", err)
	}

	return e.loadProvidersFromConfigData(config)
}

func (e *WorkflowEngine) LoadProvidersFromConfigBytes(data []byte) error {
	config, err := LoadConfigFromBytes(data)
	if err != nil {
		return fmt.Errorf("failed to parse KYC config: %w", err)
	}

	return e.loadProvidersFromConfigData(config)
}

func (e *WorkflowEngine) loadProvidersFromConfigData(config *KYCConfig) error {
	for _, providerConfig := range config.Providers {
		if !providerConfig.Enabled {
			logrus.Infof("KYC provider %s is disabled, skipping", providerConfig.Name)
			continue
		}

		providerConfig.APIKey = expandEnvVar(providerConfig.APIKey)
		providerConfig.APISecret = expandEnvVar(providerConfig.APISecret)

		if err := validateProviderConfig(providerConfig); err != nil {
			logrus.Warnf("Invalid config for provider %s: %v", providerConfig.Name, err)
			continue
		}

		provider := newConfigurableProviderAdapter(providerConfig)
		e.RegisterProvider(provider)

		logrus.Infof("Loaded KYC provider from config: %s", providerConfig.Name)
	}

	return nil
}

func expandEnvVar(value string) string {
	if strings.HasPrefix(value, "${") && strings.HasSuffix(value, "}") {
		envName := value[2 : len(value)-1]
		if envValue := os.Getenv(envName); envValue != "" {
			return envValue
		}
	}
	return value
}

func validateProviderConfig(config ProviderConfig) error {
	if config.Name == "" {
		return fmt.Errorf("provider name is required")
	}
	if config.APIKey == "" {
		return fmt.Errorf("api_key is required")
	}
	if config.BaseURL == "" {
		return fmt.Errorf("base_url is required")
	}
	if config.Endpoints.CreateCheck == "" {
		return fmt.Errorf("endpoints.create_check is required")
	}
	if config.Endpoints.GetStatus == "" {
		return fmt.Errorf("endpoints.get_status is required")
	}
	if config.ResponseMapping.StatusField == "" {
		return fmt.Errorf("response_mapping.status_field is required")
	}
	if config.ResponseMapping.ReferenceField == "" {
		return fmt.Errorf("response_mapping.reference_field is required")
	}
	return nil
}

func (e *WorkflowEngine) GetRegisteredProviders() []string {
	names := make([]string, 0, len(e.providers))
	for name := range e.providers {
		names = append(names, name)
	}
	return names
}
