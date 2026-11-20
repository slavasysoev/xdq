package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const validConfig = `
version: 1
sources:
  main:
    kafka:
      brokers: [kafka-test-1:9092, kafka-test-2:9092]
      sasl:
        mechanism: SCRAM-SHA-512
        username: xdq-readonly
        password: ${env:XDQ_TEST_KAFKA_PASSWORD}
  test:
    kubernetes:
      context: test-cluster
      namespaces: [stand-a, stand-b]
  orders-db:
    postgres:
      dsn: ${file:secrets/dsn}
chains:
  order-flow:
    from:
      - topic: orders.created
      - workload: payment-gateway
        source: test
rules:
  group-multiple-workloads:
    severity: off
  lagging-consumer:
    threshold: 5000
  dead-letter-records:
    window: 30m
    suffixes: [.dlt, -failed]
  unresolved-client:
    ignore:
      - endpoint: 10.20.0.0/16
      - endpoint: 10.8.0.5
      - group: billing
`

const (
	kafkaPassword = "kafka-pa55word"
	postgresDSN   = "postgres://xdq:pg-pa55word@db/orders"
)

// writeSecretFile creates secrets/dsn under a temp dir, ending with a newline as real secret files do.
func writeSecretFile(t *testing.T) string {
	t.Helper()
	baseDir := t.TempDir()
	err := os.MkdirAll(filepath.Join(baseDir, "secrets"), 0o700)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(filepath.Join(baseDir, "secrets", "dsn"), []byte(postgresDSN+"\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	return baseDir
}

func TestLoadValidConfig(t *testing.T) {
	t.Setenv("XDQ_TEST_KAFKA_PASSWORD", kafkaPassword)
	baseDir := writeSecretFile(t)
	path := filepath.Join(baseDir, "xdq.yaml")
	err := os.WriteFile(path, []byte(validConfig), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	config, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	kafka := config.Sources["main"].Kafka
	if kafka.SASL.Password.Value() != kafkaPassword {
		t.Errorf("kafka password = %q, want %q", kafka.SASL.Password.Value(), kafkaPassword)
	}
	if config.Sources["orders-db"].Postgres.DSN.Value() != postgresDSN {
		t.Errorf("dsn = %q, want %q", config.Sources["orders-db"].Postgres.DSN.Value(), postgresDSN)
	}
	if config.Sources["test"].Kubernetes.Environment != EnvironmentNamespace {
		t.Errorf("default environment = %q, want %q", config.Sources["test"].Kubernetes.Environment, EnvironmentNamespace)
	}
	if config.Sources["test"].Type() != TypeKubernetes {
		t.Errorf("type = %q, want %q", config.Sources["test"].Type(), TypeKubernetes)
	}
	if config.Rules["group-multiple-workloads"].Severity != SeverityOff {
		t.Errorf("severity = %q, want off", config.Rules["group-multiple-workloads"].Severity)
	}
	if *config.Rules["lagging-consumer"].Threshold != 5000 {
		t.Errorf("threshold = %d, want 5000", *config.Rules["lagging-consumer"].Threshold)
	}
	deadLetter := config.Rules["dead-letter-records"]
	if deadLetter.Window != 30*time.Minute || len(deadLetter.Suffixes) != 2 || deadLetter.Suffixes[1] != "-failed" {
		t.Errorf("dead-letter-records = %+v, want window 30m and two suffixes", deadLetter)
	}
	ignore := config.Rules["unresolved-client"].Ignore
	if len(ignore) != 3 || ignore[0].Endpoint != "10.20.0.0/16" || ignore[2].Group != "billing" {
		t.Errorf("ignore = %+v, want endpoint range, endpoint address and group billing", ignore)
	}
	if len(config.Chains["order-flow"].From) != 2 {
		t.Errorf("chain entries = %d, want 2", len(config.Chains["order-flow"].From))
	}
}

func TestTLSPathsAreRelativeToConfig(t *testing.T) {
	baseDir := t.TempDir()
	text := "version: 1\nsources: {main: {kafka: {brokers: [k:9092], tls: {caFile: certs/ca.pem}}}}"

	config, err := parse([]byte(text), baseDir)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	want := filepath.Join(baseDir, "certs", "ca.pem")
	if config.Sources["main"].Kafka.TLS.CAFile != want {
		t.Errorf("caFile = %q, want %q", config.Sources["main"].Kafka.TLS.CAFile, want)
	}
}

func TestPrintedConfigHidesSecrets(t *testing.T) {
	t.Setenv("XDQ_TEST_KAFKA_PASSWORD", kafkaPassword)
	config, err := parse([]byte(validConfig), writeSecretFile(t))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}

	printed := fmt.Sprintf("%+v %+v %+v",
		*config.Sources["main"].Kafka.SASL, *config.Sources["orders-db"].Postgres, config.Sources["main"].Kafka.SASL.Password)
	for _, secret := range []string{kafkaPassword, postgresDSN} {
		if strings.Contains(printed, secret) {
			t.Errorf("printed config contains secret %q: %s", secret, printed)
		}
	}
	if !strings.Contains(printed, "${env:XDQ_TEST_KAFKA_PASSWORD}") {
		t.Errorf("printed config does not show the reference: %s", printed)
	}
}

func TestLoadInvalidConfig(t *testing.T) {
	tests := []struct {
		name string
		yaml string
		// wantErrors must all appear in the error message.
		wantErrors []string
	}{
		{
			name:       "empty file",
			yaml:       "",
			wantErrors: []string{"config is empty"},
		},
		{
			name:       "wrong version",
			yaml:       "version: 2\nsources: {db: {postgres: {dsn: '${env:XDQ_TEST_DSN}'}}}",
			wantErrors: []string{"version: must be 1, got 2"},
		},
		{
			name:       "no sources",
			yaml:       "version: 1",
			wantErrors: []string{"sources: at least one source is required"},
		},
		{
			name:       "unknown source type",
			yaml:       "version: 1\nsources: {queue: {rabbitmq: {url: amqp://host}}}",
			wantErrors: []string{"line 2: unknown field rabbitmq"},
		},
		{
			name:       "unknown field is a typo, not ignored",
			yaml:       "version: 1\nsources: {test: {kubernetes: {namespces: [stand-a]}}}",
			wantErrors: []string{"line 2: unknown field namespces"},
		},
		{
			name:       "two types in one source",
			yaml:       "version: 1\nsources: {mixed: {kafka: {brokers: [k:9092]}, postgres: {dsn: '${env:XDQ_TEST_DSN}'}}}",
			wantErrors: []string{"sources.mixed: exactly one of kafka, kubernetes, postgres must be set"},
		},
		{
			name:       "source without type",
			yaml:       "version: 1\nsources: {empty: {}}",
			wantErrors: []string{"sources.empty: exactly one of"},
		},
		{
			name:       "source name with slash",
			yaml:       "version: 1\nsources: {main/eu: {kafka: {brokers: [k:9092]}}}",
			wantErrors: []string{"sources.main/eu: name must match"},
		},
		{
			name:       "kafka without brokers",
			yaml:       "version: 1\nsources: {main: {kafka: {}}}",
			wantErrors: []string{"sources.main.kafka.brokers: at least one broker is required"},
		},
		{
			name: "bad sasl",
			yaml: "version: 1\nsources: {main: {kafka: {brokers: [k:9092], sasl: {mechanism: MD5}}}}",
			wantErrors: []string{
				"sources.main.kafka.sasl.mechanism: must be one of PLAIN, SCRAM-SHA-256, SCRAM-SHA-512",
				"sources.main.kafka.sasl.username: is required",
				"sources.main.kafka.sasl.password: is required",
			},
		},
		{
			name:       "client certificate without key",
			yaml:       "version: 1\nsources: {main: {kafka: {brokers: [k:9092], tls: {certFile: client.pem}}}}",
			wantErrors: []string{"sources.main.kafka.tls: certFile and keyFile must be set together"},
		},
		{
			name:       "unknown reference scheme",
			yaml:       "version: 1\nsources: {db: {postgres: {dsn: '${vault:db}'}}}",
			wantErrors: []string{"sources.db.postgres.dsn: must be a reference"},
		},
		{
			name:       "bad kubernetes environment",
			yaml:       "version: 1\nsources: {test: {kubernetes: {environment: 'label:'}}}",
			wantErrors: []string{"sources.test.kubernetes.environment: must be"},
		},
		{
			name: "chain selectors",
			yaml: `
version: 1
sources: {main: {kafka: {brokers: [k:9092]}}}
chains:
  flow:
    from:
      - {topic: a, group: b}
      - {topic: a, source: missing}
      - {workload: billing, source: main}
  empty: {}
`,
			wantErrors: []string{
				"chains.empty.from: at least one entry is required",
				"chains.flow.from[0]: exactly one of topic, group, workload, database must be set",
				`chains.flow.from[1].source: unknown source "missing"`,
				`chains.flow.from[2].source: source "main" is kafka, expected kubernetes`,
			},
		},
		{
			name:       "bad severity",
			yaml:       "version: 1\nsources: {main: {kafka: {brokers: [k:9092]}}}\nrules: {lagging-consumer: {severity: critical}}",
			wantErrors: []string{"rules.lagging-consumer.severity: must be error, warning or off"},
		},
		{
			name: "ignore selectors",
			yaml: `
version: 1
sources: {main: {kafka: {brokers: [k:9092]}}}
rules:
  unresolved-client:
    ignore:
      - {}
      - {group: a, endpoint: 10.0.0.1}
      - {endpoint: not-an-address}
      - {endpoint: 10.0.0.1, source: main}
      - {group: a, source: missing}
`,
			wantErrors: []string{
				"rules.unresolved-client.ignore[0]: exactly one of topic, group, workload, database must be set",
				"rules.unresolved-client.ignore[1]: exactly one of topic, group, workload, database, endpoint must be set",
				"rules.unresolved-client.ignore[2].endpoint: must be an IP address or a CIDR range",
				"rules.unresolved-client.ignore[3]: exactly one of topic, group, workload, database, endpoint must be set",
				`rules.unresolved-client.ignore[4].source: unknown source "missing"`,
			},
		},
		{
			name:       "missing env var",
			yaml:       "version: 1\nsources: {db: {postgres: {dsn: '${env:XDQ_TEST_UNSET}'}}}",
			wantErrors: []string{"sources.db.postgres.dsn: environment variable XDQ_TEST_UNSET is not set"},
		},
		{
			name:       "missing secret file",
			yaml:       "version: 1\nsources: {db: {postgres: {dsn: '${file:no-such-file}'}}}",
			wantErrors: []string{"sources.db.postgres.dsn: read secret file"},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parse([]byte(test.yaml), t.TempDir())
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			for _, want := range test.wantErrors {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error does not contain %q:\n%v", want, err)
				}
			}
		})
	}
}

func TestErrorsNeverContainSecrets(t *testing.T) {
	const inlinedPassword = "inl1ned-pa55word"
	tests := []struct {
		name string
		yaml string
	}{
		{
			name: "inlined password is rejected without being repeated",
			yaml: "version: 1\nsources: {main: {kafka: {brokers: [k:9092], sasl: {mechanism: PLAIN, username: xdq, password: " +
				inlinedPassword + "}}}}",
		},
		{
			name: "inlined dsn is rejected without being repeated",
			yaml: "version: 1\nsources: {db: {postgres: {dsn: 'postgres://xdq:" + inlinedPassword + "@db/orders'}}}",
		},
		{
			name: "resolved secret is not repeated when another secret fails",
			yaml: `
version: 1
sources:
  main: {kafka: {brokers: [k:9092], sasl: {mechanism: PLAIN, username: xdq, password: '${env:XDQ_TEST_KAFKA_PASSWORD}'}}}
  db: {postgres: {dsn: '${env:XDQ_TEST_UNSET}'}}
`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Setenv("XDQ_TEST_KAFKA_PASSWORD", kafkaPassword)
			_, err := parse([]byte(test.yaml), t.TempDir())
			if err == nil {
				t.Fatal("expected an error, got nil")
			}
			for _, secret := range []string{inlinedPassword, kafkaPassword} {
				if strings.Contains(err.Error(), secret) {
					t.Errorf("error contains secret %q:\n%v", secret, err)
				}
			}
		})
	}
}

func TestSecretFileTrailingNewline(t *testing.T) {
	tests := []struct {
		name    string
		content string
		want    string
	}{
		{name: "no newline", content: "value", want: "value"},
		{name: "unix newline", content: "value\n", want: "value"},
		{name: "windows newline", content: "value\r\n", want: "value"},
		{name: "only one newline removed", content: "value\n\n", want: "value\n"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "secret")
			err := os.WriteFile(path, []byte(test.content), 0o600)
			if err != nil {
				t.Fatal(err)
			}
			secret := Secret{reference: "${file:" + path + "}"}

			err = secret.resolve("")
			if err != nil {
				t.Fatalf("resolve: %v", err)
			}
			if secret.Value() != test.want {
				t.Errorf("value = %q, want %q", secret.Value(), test.want)
			}
		})
	}
}
