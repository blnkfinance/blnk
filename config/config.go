package config

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"sync/atomic"

	"github.com/sirupsen/logrus"
)

//todo validate config before load. check for empty values in required fields(data source), trim white spaces, set default values(port)

var configStore atomic.Value

type Configuration struct {
	Name            string `json:"project_name,omitempty"`
	Port            string `json:"port,omitempty"`
	DefaultCurrency string `json:"default_currency,omitempty"`
	EndPointSecret  string `json:"end_point_secret"`
	DataSource      struct {
		Name string `json:"name,omitempty"`
		DNS  string `json:"dns,omitempty"`
	} `json:"data_source"`
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
		return nil, errors.New("config not loaded from file. Create a json file called saifu.json with your config ❌")
	}

	return c, nil
}

func logger() {
	logger := logrus.New()
	log.SetOutput(logger.Writer())
}
