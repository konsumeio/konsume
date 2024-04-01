package e2e

import (
	"context"
	"os"
	"testing"
	"time"

	konsume "github.com/bugrakocabay/konsume/cmd"
	"github.com/bugrakocabay/konsume/pkg/config"
)

func TestKonsumeWithKafkaMongo(t *testing.T) {
	tests := []TestCase{
		{
			Description: "Test with single message",
			KonsumeConfig: &config.Config{
				Providers: []*config.ProviderConfig{
					{
						Name: "kafka-queue",
						Type: "kafka",
						KafkaConfig: &config.KafkaConfig{
							Topic: "kafka-mongo-1",
							Brokers: []string{
								"127.0.0.1:29092",
							},
							Group: "test-group",
						},
					},
				},
				Databases: []*config.DatabaseConfig{
					{
						Name:             "mongo-database",
						Type:             "mongodb",
						ConnectionString: "mongodb://localhost:27017",
						Database:         "mynewdb",
					},
				},
				Queues: []*config.QueueConfig{
					{
						Name:     "kafka-queue-1",
						Provider: "kafka-queue",
						DatabaseRoutes: []*config.DatabaseRouteConfig{
							{
								Name:       "test-db-route",
								Provider:   "mongo-database",
								Collection: collectionName,
								Mapping: map[string]string{
									"car_brand": "brand",
									"car_model": "model",
									"car_year":  "year",
								},
							},
						},
					},
				},
			},
			SetupMessage: SetupMessage{
				QueueName: "kafka-queue-1",
				Message:   []byte("{\"car_brand\": \"test\", \"car_model\": \"test\", \"car_year\": 2021}"),
			},
			ExpectedQuery: DBQueryExpectation{
				Table: collectionName,
				Data: map[string]any{
					"brand": "test",
					"model": "test",
					"year":  2021,
				},
			},
		},
	}

	for _, test := range tests {
		t.Run(test.Description, func(t *testing.T) {
			// Connect to mongo
			connStr := test.KonsumeConfig.Databases[0].ConnectionString
			db, err := connectToMongo(connStr, test.KonsumeConfig.Databases[0].Database)
			if err != nil {
				t.Fatalf("Failed to connect to MongoDB: %v", err)
			}
			err = createCollection(db, collectionName)
			if err != nil {
				t.Fatalf("Failed to create collection: %v", err)
			}
			defer dropCollection(db, collectionName)

			// Setting up the config file
			configFilePath, cleanup := writeConfigToFile(test.KonsumeConfig)
			defer cleanup()
			os.Setenv("KONSUME_CONFIG_PATH", configFilePath)
			os.Setenv("KONSUME_PLUGIN_PATH", "../../plugins")

			// Running konsume and waiting for it to initialize
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

			// Verify the result
			cursor, err := queryCollection(db, collectionName)
			if err != nil {
				t.Fatalf("Failed to query collection: %v", err)
			}
			var data map[string]any
			for cursor.Next(context.Background()) {
				if err = cursor.Decode(&data); err != nil {
					t.Fatalf("Failed to decode data: %v", err)
				}
				break
			}
			expected := test.ExpectedQuery.Data
			if data["brand"] != expected["brand"] {
				t.Fatalf("Expected brand: %v, got: %v", expected["brand"], data["brand"])
			}
			if data["model"] != expected["model"] {
				t.Fatalf("Expected model: %v, got: %v", expected["model"], data["model"])
			}
			if data["year"].(float64) != float64(expected["year"].(int)) {
				t.Fatalf("Expected year: %v, got: %v", expected["year"], data["year"])
			}
		})
	}
}
