package e2e

import (
	"os"
	"testing"
	"time"

	konsume "github.com/bugrakocabay/konsume/cmd"
	"github.com/bugrakocabay/konsume/pkg/common"
	"github.com/bugrakocabay/konsume/pkg/config"
)

func TestKonsumeWithKafkaPostgres(t *testing.T) {
	tests := []TestCase{
		{
			Description: "Test with single message",
			KonsumeConfig: &config.Config{
				Providers: []*config.ProviderConfig{
					{
						Name: "kafka-queue",
						Type: "kafka",
						KafkaConfig: &config.KafkaConfig{
							Topic: "kafka-postgres-1",
							Brokers: []string{
								"127.0.0.1:29092",
							},
							Group: "test-group",
						},
					},
				},
				Databases: []*config.DatabaseConfig{
					{
						Name:             "sql-database",
						Type:             common.DatabaseTypePostgresql,
						ConnectionString: "postgres://postgres:mysecretpassword@localhost:5432/mynewdatabase?sslmode=disable",
					},
				},
				Queues: []*config.QueueConfig{
					{
						Name:     "kafka-queue-1",
						Provider: "kafka-queue",
						DatabaseRoutes: []*config.DatabaseRouteConfig{
							{
								Name:     "test-db-route",
								Provider: "sql-database",
								Table:    tableName,
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
				Table: tableName,
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
			// connect to postgres
			pgConnStr := test.KonsumeConfig.Databases[0].ConnectionString
			db, err := connectToPostgres(pgConnStr)
			if err != nil {
				t.Fatalf("Failed to connect to postgres: %v", err)
			}
			defer db.Close()
			err = createTable(db, tableName, map[string]string{
				"brand": "text",
				"model": "text",
				"year":  "integer",
			})
			if err != nil {
				t.Fatalf("Failed to create table: %v", err)
			}
			defer dropTable(db, tableName)

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
			rows, err := queryTable(db, tableName)
			if err != nil {
				t.Fatalf("Failed to query table: %v", err)
			}
			var brand string
			var model string
			var year int
			if !rows.Next() {
				t.Fatalf("Expected at least 1 row, but found none")
			}
			err = rows.Scan(&brand, &model, &year)
			if err != nil {
				t.Fatalf("Failed to scan row: %v", err)
			}
			if rows.Next() {
				t.Fatalf("Expected exactly 1 row, but found more")
			}
			rows.Close()
			expected := test.ExpectedQuery.Data
			if brand != expected["brand"].(string) {
				t.Errorf("Expected brand: %s, got: %s", expected["brand"].(string), brand)
			}
			if model != expected["model"].(string) {
				t.Errorf("Expected model: %s, got: %s", expected["model"].(string), model)
			}
			if year != expected["year"].(int) {
				t.Errorf("Expected year: %d, got: %d", expected["year"].(int), year)
			}
		})
	}
}
