package config

import (
	"errors"
	"log/slog"
	"os"

	"gopkg.in/yaml.v3"
)

var (
	readConfigFileError      = errors.New("failed to read configuration file")
	unmarshalConfigFileError = errors.New("failed to unmarshal configuration file")
	configFileNotFoundError  = errors.New("configuration file not found")
	noProvidersDefinedError  = errors.New("no providers defined")
	noQueuesDefinedError     = errors.New("no queues defined")
)

// Config is the main configuration struct
type Config struct {
	// Providers is a list of sources that will be connected to
	Providers []*ProviderConfig `yaml:"providers"`

	// Queues is a list of queues that will be consumed
	Queues []*QueueConfig `yaml:"queues"`
}

// LoadConfig loads the configuration from the config.yaml file
func LoadConfig() (*Config, error) {
	configPath := os.Getenv("KONSUME_CONFIG_PATH")
	if len(configPath) == 0 {
		configPath = "config.yaml"
	}
	slog.Info("Loading configuration from", "path", configPath)

	// Read the yaml configuration file and unmarshal it into the Config struct
	data, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, configFileNotFoundError
		}
		return nil, readConfigFileError
	}
	cfg := &Config{}
	err = yaml.Unmarshal(data, cfg)
	if err != nil {
		return nil, unmarshalConfigFileError
	}

	// Validate the configuration
	err = cfg.ValidateAll()
	if err != nil {
		return nil, err
	}

	return cfg, nil
}

// ValidateAll validates the configuration
func (c *Config) ValidateAll() error {
	if len(c.Providers) == 0 {
		return noProvidersDefinedError
	}
	if len(c.Queues) == 0 {
		return noQueuesDefinedError
	}

	for _, p := range c.Providers {
		err := p.validateProvider()
		if err != nil {
			return err
		}
	}

	for _, q := range c.Queues {
		err := q.validateQueue(c.Providers)
		if err != nil {
			return err
		}
	}
	return nil
}
