package config

import (
	"slices"
	"strings"
)

// KafkaConfig is a Kafka cluster.
type KafkaConfig struct {
	Brokers []string    `yaml:"brokers"`
	SASL    *SASLConfig `yaml:"sasl"`
	TLS     *TLSConfig  `yaml:"tls"`
}

// TLSConfig enables TLS. An empty block (tls: {}) uses the system CA pool. Relative paths are
// relative to the config file.
type TLSConfig struct {
	// CAFile is a PEM bundle that replaces the system CA pool.
	CAFile string `yaml:"caFile"`
	// CertFile and KeyFile are a PEM client certificate and key for mTLS. Both or neither are set.
	CertFile string `yaml:"certFile"`
	KeyFile  string `yaml:"keyFile"`
}

// SASL mechanisms.
const (
	SASLPlain       = "PLAIN"
	SASLScramSHA256 = "SCRAM-SHA-256"
	SASLScramSHA512 = "SCRAM-SHA-512"
)

var saslMechanisms = []string{SASLPlain, SASLScramSHA256, SASLScramSHA512}

// SASLConfig is Kafka SASL authentication.
type SASLConfig struct {
	// Mechanism is one of the SASL constants.
	Mechanism string `yaml:"mechanism"`
	Username  string `yaml:"username"`
	Password  Secret `yaml:"password"`
}

// Type implements SourceSettings.
func (*KafkaConfig) Type() string {
	return TypeKafka
}

func (kafka *KafkaConfig) validate(validation *validation, path string) {
	if len(kafka.Brokers) == 0 {
		validation.fail(path+".brokers", "at least one broker is required")
	}
	if kafka.TLS != nil && (kafka.TLS.CertFile == "") != (kafka.TLS.KeyFile == "") {
		validation.fail(path+".tls", "certFile and keyFile must be set together")
	}
	if kafka.SASL == nil {
		return
	}
	if !slices.Contains(saslMechanisms, kafka.SASL.Mechanism) {
		validation.fail(path+".sasl.mechanism", "must be one of %s", strings.Join(saslMechanisms, ", "))
	}
	if kafka.SASL.Username == "" {
		validation.fail(path+".sasl.username", "is required")
	}
	validation.secret(path+".sasl.password", kafka.SASL.Password)
}

func (kafka *KafkaConfig) resolve(baseDir, path string) error {
	if kafka.TLS != nil {
		kafka.TLS.CAFile = resolvePath(baseDir, kafka.TLS.CAFile)
		kafka.TLS.CertFile = resolvePath(baseDir, kafka.TLS.CertFile)
		kafka.TLS.KeyFile = resolvePath(baseDir, kafka.TLS.KeyFile)
	}
	if kafka.SASL == nil {
		return nil
	}

	return resolveSecret(baseDir, path+".sasl.password", &kafka.SASL.Password)
}
