package e2e

import (
	"fmt"
	"os"
	"testing"
	"time"

	konsume "github.com/bugrakocabay/konsume/cmd"
	"github.com/bugrakocabay/konsume/pkg/common"
	"github.com/bugrakocabay/konsume/pkg/config"
)

func TestKonsumeWithKafkaHTTP(t *testing.T) {
	mockServer, url, requestCapture := setupMockServer(t)
	defer mockServer.Close()

	tests := []TestCase{
		{
			Description: "Test with single message",
			KonsumeConfig: &config.Config{
				Providers: []*config.ProviderConfig{
					{
						Name: "kafka-queue",
						Type: "kafka",
						KafkaConfig: &config.KafkaConfig{
							Topic: "kafka-http-1",
							Brokers: []string{
								"127.0.0.1:29092",
							},
							Group: "test-group-1",
						},
					},
				},
				Queues: []*config.QueueConfig{
					{
						Name:     "kafka-queue-1",
						Provider: "kafka-queue",
						Routes: []*config.RouteConfig{
							{
								Name: "test-route",
								URL:  fmt.Sprintf("%s/200", url),
							},
						},
					},
				},
			},
			SetupMessage: SetupMessage{
				QueueName: "kafka-queue-1",
				Message:   []byte("{\"id\": 0, \"name\": \"test\"}"),
			},
			ExpectedResult: []HTTPRequestExpectation{
				{
					URL:    "/200",
					Body:   "{\"id\": 0, \"name\": \"test\"}",
					Method: "POST",
				},
			},
		},
		{
			Description: "Test with single message dynamic body",
			KonsumeConfig: &config.Config{
				Providers: []*config.ProviderConfig{
					{
						Name: "kafka-queue",
						Type: "kafka",
						KafkaConfig: &config.KafkaConfig{
							Topic: "kafka-http-2",
							Brokers: []string{
								"127.0.0.1:29092",
							},
							Group: "test-group",
						},
					},
				},
				Queues: []*config.QueueConfig{
					{
						Name:     "kafka-queue-2",
						Provider: "kafka-queue",
						Routes: []*config.RouteConfig{
							{
								Name: "test-route",
								URL:  fmt.Sprintf("%s/200", url),
								Body: map[string]interface{}{
									"some-id":   "{{id}}",
									"some-name": "{{name}}",
								},
							},
						},
					},
				},
			},
			SetupMessage: SetupMessage{
				QueueName: "kafka-queue-2",
				Message:   []byte("{\"id\": 1, \"name\": \"test\"}"),
			},
			ExpectedResult: []HTTPRequestExpectation{
				{
					URL:    "/200",
					Body:   "{\"some-id\":1,\"some-name\":\"test\"}",
					Method: "POST",
				},
			},
		},
		{
			Description: "Test with single message fixed retry strategy",
			KonsumeConfig: &config.Config{
				Providers: []*config.ProviderConfig{
					{
						Name: "kafka-queue",
						Type: "kafka",
						KafkaConfig: &config.KafkaConfig{
							Topic: "kafka-http-3",
							Brokers: []string{
								"127.0.0.1:29092",
							},
							Group: "test-group",
						},
					},
				},
				Queues: []*config.QueueConfig{
					{
						Name:     "kafka-queue-3",
						Provider: "kafka-queue",
						Retry: &config.RetryConfig{
							Enabled:         true,
							MaxRetries:      2,
							Strategy:        common.RetryStrategyFixed,
							ThresholdStatus: 500,
							Interval:        1 * time.Second,
						},
						Routes: []*config.RouteConfig{
							{
								Name: "test-route",
								URL:  fmt.Sprintf("%s/500", url),
							},
						},
					},
				},
			},
			SetupMessage: SetupMessage{
				QueueName: "kafka-queue-3",
				Message:   []byte("{\"id\": 1, \"name\": \"test\"}"),
			},
			ExpectedResult: []HTTPRequestExpectation{
				{
					URL:    "/500",
					Body:   "{\"id\": 1, \"name\": \"test\"}",
					Method: "POST",
				},
				{
					URL:    "/500",
					Body:   "{\"id\": 1, \"name\": \"test\"}",
					Method: "POST",
				},
				{
					URL:    "/500",
					Body:   "{\"id\": 1, \"name\": \"test\"}",
					Method: "POST",
				},
			},
		},
		{
			Description: "Test with single message expo retry strategy",
			KonsumeConfig: &config.Config{
				Providers: []*config.ProviderConfig{
					{
						Name: "kafka-queue",
						Type: "kafka",
						KafkaConfig: &config.KafkaConfig{
							Topic: "kafka-http-4",
							Brokers: []string{
								"127.0.0.1:29092",
							},
							Group: "test-group",
						},
					},
				},
				Queues: []*config.QueueConfig{
					{
						Name:     "kafka-queue-4",
						Provider: "kafka-queue",
						Retry: &config.RetryConfig{
							Enabled:         true,
							MaxRetries:      2,
							Strategy:        common.RetryStrategyExpo,
							ThresholdStatus: 500,
							Interval:        1 * time.Second,
						},
						Routes: []*config.RouteConfig{
							{
								Name: "test-route",
								URL:  fmt.Sprintf("%s/500", url),
							},
						},
					},
				},
			},
			SetupMessage: SetupMessage{
				QueueName: "kafka-queue-4",
				Message:   []byte("{\"id\": 1, \"name\": \"test\"}"),
			},
			ExpectedResult: []HTTPRequestExpectation{
				{
					URL:    "/500",
					Body:   "{\"id\": 1, \"name\": \"test\"}",
					Method: "POST",
				},
				{
					URL:    "/500",
					Body:   "{\"id\": 1, \"name\": \"test\"}",
					Method: "POST",
				},
				{
					URL:    "/500",
					Body:   "{\"id\": 1, \"name\": \"test\"}",
					Method: "POST",
				},
			},
		},
		{
			Description: "Test with single message rand retry strategy",
			KonsumeConfig: &config.Config{
				Providers: []*config.ProviderConfig{
					{
						Name: "kafka-queue",
						Type: "kafka",
						KafkaConfig: &config.KafkaConfig{
							Topic: "kafka-http-5",
							Brokers: []string{
								"127.0.0.1:29092",
							},
							Group: "test-group",
						},
					},
				},
				Queues: []*config.QueueConfig{
					{
						Name:     "kafka-queue-5",
						Provider: "kafka-queue",
						Retry: &config.RetryConfig{
							Enabled:         true,
							MaxRetries:      2,
							Strategy:        common.RetryStrategyRand,
							ThresholdStatus: 500,
							Interval:        1 * time.Second,
						},
						Routes: []*config.RouteConfig{
							{
								Name: "test-route",
								URL:  fmt.Sprintf("%s/500", url),
							},
						},
					},
				},
			},
			SetupMessage: SetupMessage{
				QueueName: "kafka-queue-5",
				Message:   []byte("{\"id\": 1, \"name\": \"test\"}"),
			},
			ExpectedResult: []HTTPRequestExpectation{
				{
					URL:    "/500",
					Body:   "{\"id\": 1, \"name\": \"test\"}",
					Method: "POST",
				},
				{
					URL:    "/500",
					Body:   "{\"id\": 1, \"name\": \"test\"}",
					Method: "POST",
				},
				{
					URL:    "/500",
					Body:   "{\"id\": 1, \"name\": \"test\"}",
					Method: "POST",
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Description, func(t *testing.T) {
			requestCapture.ReceivedRequests = nil
			// Setting up the config file
			configFilePath, cleanup := writeConfigToFile(test.KonsumeConfig)
			defer cleanup()
			os.Setenv("KONSUME_CONFIG_PATH", configFilePath)

			// Running konsume and waiting for it to consume the message
			go konsume.Execute()
			time.Sleep(2 * time.Second)

			// Pushing the message to the queue
			conn, err := connectToKafka(test.KonsumeConfig.Providers[0].KafkaConfig.Brokers[0], test.KonsumeConfig.Providers[0].KafkaConfig.Topic)
			if err != nil {
				t.Fatalf("Failed to connect to Kafka: %v", err)
			}
			defer conn.Close()
			err = pushMessageToKafka(conn, test.SetupMessage.Message)
			if err != nil {
				t.Fatalf("Failed to push message to Kafka: %v", err)
			}
			sleep(test)

			// Checking the captured requests
			requestCapture.Mutex.Lock()
			defer requestCapture.Mutex.Unlock()
			if len(requestCapture.ReceivedRequests) != len(test.ExpectedResult) {
				t.Fatalf("Expected %d HTTP requests, but got %d", len(test.ExpectedResult), len(requestCapture.ReceivedRequests))
			}
			for i, expectedRequest := range test.ExpectedResult {
				if requestCapture.ReceivedRequests[i].URL != expectedRequest.URL {
					t.Errorf("Expected URL: %s, but got: %s", expectedRequest.URL, requestCapture.ReceivedRequests[i].URL)
				}
				if requestCapture.ReceivedRequests[i].Method != expectedRequest.Method {
					t.Errorf("Expected method: %s, but got: %s", expectedRequest.Method, requestCapture.ReceivedRequests[i].Method)
				}
				if requestCapture.ReceivedRequests[i].Body != expectedRequest.Body {
					t.Errorf("Expected body: %s, but got: %s", expectedRequest.Body, requestCapture.ReceivedRequests[i].Body)
				}
			}
		})
	}
}
