package config

import (
	"errors"
	"io"
	"log"
	"os"
	"path/filepath"
	"testing"
)

func TestLoadConfig(t *testing.T) {
	log.SetOutput(io.Discard)
	dir := t.TempDir()

	tests := []struct {
		name           string
		configPath     string
		pathAndContent map[string]string
		expectedError  error
	}{
		{
			name:           "should throw error if config file is not found",
			configPath:     "",
			pathAndContent: map[string]string{},
			expectedError:  configFileNotFoundError,
		},
		{
			name:       "should throw error if config file is not valid yaml",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": "invalid yaml",
			},
			expectedError: unmarshalConfigFileError,
		},
		{
			name:       "should throw error if config file has no providers",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
queues:
  - name: "test"
    provider: "test-provider"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: noProvidersDefinedError,
		},
		{
			name:       "should throw error if config file has no queues",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
`,
			},
			expectedError: noQueuesDefinedError,
		},
		{
			name:       "should throw error if config file has invalid provider type",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "invalid-type"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: invalidProviderTypeError,
		},
		{
			name:       "should throw error if provider name is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: ""
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: providerNameNotDefinedError,
		},
		{
			name:       "should throw error if provider type is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: providerTypeNotDefinedError,
		},
		{
			name:       "should throw error if amqp provider config is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: amqpConfigNotDefinedError,
		},
		{
			name:       "should throw error if amqp host is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: amqpHostNotDefinedError,
		},
		{
			name:       "should throw error if amqp port is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: amqpPortNotDefinedError,
		},
		{
			name:       "should throw error if amqp username is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: amqpUsernameNotDefinedError,
		},
		{
			name:       "should throw error if amqp password is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: amqpPasswordNotDefinedError,
		},
		{
			name:       "should throw error if kafka provider config is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "kafka"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: kafkaConfigNotDefinedError,
		},
		{
			name:       "should throw error if kafka brokers are not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "kafka-queue"
    type: "kafka"
    kafka-config:
      topic: "topic1"
      group: "group1"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: brokersNotDefinedError,
		},
		{
			name:       "should throw error if kafka topic are not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "kafka-queue"
    type: "kafka"
    kafka-config:
      brokers:
        - "kafka:9092"
      group: "group1"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: topicNotDefinedError,
		},
		{
			name:       "should throw error if kafka group is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "kafka-queue"
    type: "kafka"
    kafka-config:
      brokers:
        - "kafka:9092"
      topic: "group1"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: groupNotDefinedError,
		},
		{
			name:       "should throw error if queue name is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: queueNameNotDefinedError,
		},
		{
			name:       "should throw error if queue provider is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: queueProviderNotDefinedError,
		},
		{
			name:       "should throw error if queue provider does not exist in providers list",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "invalid-provider"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: queueProviderDoesNotExistError,
		},
		{
			name:       "should throw error if routes are not defined for queue",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
`,
			},
			expectedError: noRoutesDefinedError,
		},
		{
			name:       "should throw error if route name is not defined for queue",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - url: "http://localhost:8080"
`,
			},
			expectedError: routeNameNotDefinedError,
		},
		{
			name:       "should throw error if route url is not defined for queue",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
`,
			},
			expectedError: urlNotDefinedError,
		},
		{
			name:       "should throw error if type is graphql and body is not defined for queue route config",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        type: "graphql"
        url: "http://localhost:8080"
`,
			},
			expectedError: bodyNotDefinedError,
		},
		{
			name:       "should throw error if type is graphql and body is not valid for queue route config",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        type: "graphql"
        url: "http://localhost:8080"
        body:
          invalid: "invalid"
`,
			},
			expectedError: invalidBodyForGraphQLError,
		},
		{
			name:       "should throw error if type is graphql and body does not contain string for query or mutation for queue route config",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        type: "graphql"
        url: "http://localhost:8080"
        body:
          mutation: 1
`,
			},
			expectedError: bodyNotContainsStringForGraphQLError,
		},
		{
			name:       "should throw error if max retries is not defined for queue retry config",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    retry:
      enabled: true
      interval: 1s
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: maxRetriesNotDefinedError,
		},
		{
			name:       "should throw error if interval is not defined for queue retry config",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    retry:
      enabled: true
      max_retries: 2
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: intervalNotDefinedError,
		},
		{
			name:       "should throw error if invalid strategy is defined for queue retry config",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    retry:
      enabled: true
      max_retries: 2
      interval: 1s
      strategy: "invalid-strategy"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: invalidStrategyError,
		},
		{
			name:       "should load config file successfully",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: nil,
		},
		{
			name:       "should throw error if stomp provider config is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "activemq"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: stompConfigNotDefinedError,
		},
		{
			name:       "should throw error if stomp host is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "activemq"
    stomp-config:
      port: 61613
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: stompHostNotDefinedError,
		},
		{
			name:       "should throw error if stomp port is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "activemq"
    stomp-config:
      host: "127.0.0.1"
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: stompPortNotDefinedError,
		},
		{
			name:       "should throw error if stomp username is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "activemq"
    stomp-config:
      host: "127.0.0.1"
      port: 61613
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: stompUsernameNotDefinedError,
		},
		{
			name:       "should throw error if stomp password is not defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "activemq"
    stomp-config:
      host: "127.0.0.1"
      port: 61613
      username: "user"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
`,
			},
			expectedError: stompPasswordNotDefinedError,
		},
		{
			name:       "should throw error metrics are enabled but invalid port is defined",
			configPath: "./config.yaml",
			pathAndContent: map[string]string{
				"config.yaml": `
providers:
  - name: "test-queue"
    type: "rabbitmq"
    amqp-config:
      host: "rabbitmq"
      port: 5672
      username: "user"
      password: "password"
queues:
  - name: "test"
    provider: "test-queue"
    routes:
      - name: "test-route"
        url: "http://localhost:8080"
metrics:
  enabled: true
  port: 65536
`,
			},
			expectedError: invalidMetricsPortError,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			configFile := filepath.Join(dir, "config.yaml")
			if len(tc.configPath) == 0 {
				os.Remove(configFile) // Ensure the file does not exist for the test
			} else {
				for path, content := range tc.pathAndContent {
					err := os.WriteFile(filepath.Join(dir, path), []byte(content), 0644)
					if err != nil {
						t.Fatal(err)
					}
				}
			}

			os.Setenv("KONSUME_CONFIG_PATH", configFile)
			_, err := LoadConfig()
			if !errors.Is(err, tc.expectedError) {
				t.Errorf("expected error %v, got %v", tc.expectedError, err)
			}
		})
	}
}
