package cli

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/testenv"
)

const integrationConfig = `version: 1
sources:
  main:
    kafka:
      brokers: [BROKER]
  orders-db:
    postgres:
      dsn: ${env:XDQ_TEST_ORDERS_DSN}
chains:
  order-flow:
    from:
      - topic: orders.created
`

// TestCheckAndLockEndToEnd runs xdq check and xdq lock against a real Kafka and Postgres, the way
// a CI pre-flight step would, and breaks the chain step by step. The steps depend on each other.
// There is no Kubernetes source: group members stay unresolved endpoints.
func TestCheckAndLockEndToEnd(t *testing.T) {
	brokers := testenv.StartKafka(t)
	postgres := testenv.StartPostgres(t, "orders", "xdq", "pa55-Word")
	dsn, err := postgres.ConnectionString(t.Context(), "sslmode=disable")
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("XDQ_TEST_ORDERS_DSN", dsn)
	configPath := writeConfig(t, strings.Replace(integrationConfig, "BROKER", brokers[0], 1))
	lockPath := filepath.Join(filepath.Dir(configPath), "xdq.lock")

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	admin := kadm.NewClient(client)
	_, err = admin.CreateTopic(t.Context(), 1, 1, nil, "orders.created")
	if err != nil {
		t.Fatal(err)
	}
	produce(t, client, 5)
	billing := consumeAll(t, brokers, 5)

	t.Run("healthy", func(t *testing.T) {
		result, code := checkJSON(t, configPath)
		if code != exitCodeOK || result.Status != report.StatusOK {
			t.Errorf("exit code %d, status %s, findings %v; want 0, ok", code, result.Status, ruleNames(result))
		}
		for _, source := range result.Sources {
			if source.Status != report.StatusOK {
				t.Errorf("source %s is %s: %s", source.Name, source.Status, source.Error)
			}
		}
	})

	t.Run("lock is byte-identical on an unchanged system", func(t *testing.T) {
		first := lockFile(t, configPath, lockPath)
		second := lockFile(t, configPath, lockPath)
		if !bytes.Equal(first, second) {
			t.Errorf("lockfiles differ:\n%s\n---\n%s", first, second)
		}
		want := []byte("{from: topic:main/orders.created, kind: consumer, to: group:main/billing}")
		if !bytes.Contains(first, want) {
			t.Errorf("lockfile does not contain %s:\n%s", want, first)
		}
	})

	t.Run("healthy with the lockfile", func(t *testing.T) {
		result, code := checkJSON(t, configPath)
		if code != exitCodeOK || len(result.Findings) != 0 {
			t.Errorf("exit code %d, findings %v; want 0, none", code, ruleNames(result))
		}
	})

	// The consumer leaves its group and new orders arrive: the group keeps its offsets and lags.
	t.Run("stalled consumer", func(t *testing.T) {
		billing.Close()
		produce(t, client, 3)

		result, code := checkJSON(t, configPath)
		if code != exitCodeErrors || !slices.Contains(ruleNames(result), "stalled-consumer") {
			t.Errorf("exit code %d, findings %v; want 1 with stalled-consumer", code, ruleNames(result))
		}
	})

	t.Run("removed consumer edge is drift", func(t *testing.T) {
		deleted, err := admin.DeleteGroup(t.Context(), "billing")
		if err == nil {
			err = deleted.Err
		}
		if err != nil {
			t.Fatalf("delete group: %v", err)
		}

		result, code := checkJSON(t, configPath)
		if code != exitCodeErrors || !slices.Contains(ruleNames(result), "drift-edge-removed") {
			t.Errorf("exit code %d, findings %v; want 1 with drift-edge-removed", code, ruleNames(result))
		}
	})

	t.Run("unavailable source", func(t *testing.T) {
		err := postgres.Stop(t.Context(), nil)
		if err != nil {
			t.Fatal(err)
		}

		result, code := checkJSON(t, configPath)
		if code != exitCodeIncomplete {
			t.Errorf("exit code = %d, want %d", code, exitCodeIncomplete)
		}
		failed := slices.IndexFunc(result.Sources, func(source report.SourceResult) bool { return source.Name == "orders-db" })
		if failed < 0 || result.Sources[failed].Status != report.StatusUnknown {
			t.Errorf("sources = %+v, want orders-db unknown", result.Sources)
		}
	})
}

// checkJSON runs xdq check with JSON output and returns the report and the exit code.
func checkJSON(t *testing.T, configPath string) (report.Report, int) {
	t.Helper()
	stdout, code, _ := runCommand(t, "check", "--config", configPath, "--timeout", "5s", "--output", "json")
	var result report.Report
	err := json.Unmarshal([]byte(stdout), &result)
	if err != nil {
		t.Fatalf("stdout is not a report: %v\n%s", err, stdout)
	}

	return result, code
}

// lockFile runs xdq lock and returns the lockfile it wrote.
func lockFile(t *testing.T, configPath, lockPath string) []byte {
	t.Helper()
	stdout, code, err := runCommand(t, "lock", "--config", configPath, "--timeout", "5s")
	if code != exitCodeOK {
		t.Fatalf("lock: exit code %d, %v\n%s", code, err, stdout)
	}
	content, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatal(err)
	}

	return content
}

func ruleNames(result report.Report) []string {
	var names []string
	for _, finding := range result.Findings {
		names = append(names, finding.Rule)
	}

	return names
}

func produce(t *testing.T, client *kgo.Client, count int) {
	t.Helper()
	records := make([]*kgo.Record, 0, count)
	for range count {
		records = append(records, &kgo.Record{Topic: "orders.created", Value: []byte("order")})
	}
	err := client.ProduceSync(t.Context(), records...).FirstErr()
	if err != nil {
		t.Fatalf("produce: %v", err)
	}
}

// consumeAll starts the billing consumer, reads count records and commits them. The consumer
// stays a member of its group until it is closed.
func consumeAll(t *testing.T, brokers []string, count int) *kgo.Client {
	t.Helper()
	consumer, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumerGroup("billing"),
		kgo.ConsumeTopics("orders.created"),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Close)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	consumed := 0
	for consumed < count {
		fetches := consumer.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("billing consumed %d of %d records", consumed, count)
		}
		consumed += fetches.NumRecords()
	}
	err = consumer.CommitUncommittedOffsets(ctx)
	if err != nil {
		t.Fatalf("commit: %v", err)
	}

	return consumer
}
