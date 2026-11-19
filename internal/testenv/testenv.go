// Package testenv starts the systems that integration tests run against, in Docker. It is imported
// only by tests, so it never ends up in the xdq binary.
//
// Integration tests are skipped with go test -short, which needs no Docker.
package testenv

import (
	"testing"

	"github.com/testcontainers/testcontainers-go"
	kafkacontainer "github.com/testcontainers/testcontainers-go/modules/kafka"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"
)

const (
	// KafkaImage is Confluent Platform 8.1, which is Kafka 4.1: it supports MAX_TIMESTAMP and
	// KIP-848 consumer groups.
	KafkaImage    = "confluentinc/confluent-local:8.1.0"
	PostgresImage = "postgres:17-alpine"
)

// RequireDocker skips the test with go test -short.
func RequireDocker(t *testing.T) {
	t.Helper()
	if testing.Short() {
		t.Skip("integration test: needs Docker")
	}
}

// StartKafka starts a single PLAINTEXT broker that is removed when the test ends, and returns
// its bootstrap addresses.
func StartKafka(t *testing.T) []string {
	t.Helper()
	RequireDocker(t)
	container, err := kafkacontainer.Run(t.Context(), KafkaImage)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}
	brokers, err := container.Brokers(t.Context())
	if err != nil {
		t.Fatalf("brokers: %v", err)
	}

	return brokers
}

// StartPostgres starts a server with one database, owned by username, that is removed when the
// test ends. The container is returned so that a test can read its DSN or stop it.
func StartPostgres(t *testing.T, database, username, password string) *postgrescontainer.PostgresContainer {
	t.Helper()
	RequireDocker(t)
	container, err := postgrescontainer.Run(t.Context(), PostgresImage,
		postgrescontainer.WithDatabase(database),
		postgrescontainer.WithUsername(username),
		postgrescontainer.WithPassword(password),
		postgrescontainer.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}

	return container
}
