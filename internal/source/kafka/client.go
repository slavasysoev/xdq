package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/slavasysoev/xdq/internal/config"
)

// clientID is how xdq introduces itself to the brokers.
const clientID = "xdq"

// clientOptions builds the franz-go options. There are no consumer or producer options:
// xdq only issues admin requests.
func clientOptions(settings config.KafkaConfig) ([]kgo.Opt, error) {
	options := []kgo.Opt{
		kgo.SeedBrokers(settings.Brokers...),
		kgo.ClientID(clientID),
	}
	if settings.TLS != nil {
		tlsConfig, err := buildTLSConfig(*settings.TLS)
		if err != nil {
			return nil, err
		}
		options = append(options, kgo.DialTLSConfig(tlsConfig))
	}
	if settings.SASL != nil {
		mechanism, err := saslMechanism(*settings.SASL)
		if err != nil {
			return nil, err
		}
		options = append(options, kgo.SASL(mechanism))
	}

	return options, nil
}

// buildTLSConfig leaves ServerName empty: franz-go sets it to the host of each broker.
func buildTLSConfig(settings config.TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{MinVersion: tls.VersionTLS12}
	if settings.CAFile != "" {
		pem, err := os.ReadFile(settings.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read CA file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(pem) {
			return nil, fmt.Errorf("CA file %s contains no PEM certificates", settings.CAFile)
		}
		tlsConfig.RootCAs = pool
	}
	if settings.CertFile != "" {
		certificate, err := tls.LoadX509KeyPair(settings.CertFile, settings.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("load client certificate: %w", err)
		}
		tlsConfig.Certificates = []tls.Certificate{certificate}
	}

	return tlsConfig, nil
}

func saslMechanism(settings config.SASLConfig) (sasl.Mechanism, error) {
	username := settings.Username
	password := settings.Password.Value()
	switch settings.Mechanism {
	case config.SASLPlain:
		return plain.Auth{User: username, Pass: password}.AsMechanism(), nil
	case config.SASLScramSHA256:
		return scram.Auth{User: username, Pass: password}.AsSha256Mechanism(), nil
	case config.SASLScramSHA512:
		return scram.Auth{User: username, Pass: password}.AsSha512Mechanism(), nil
	}

	return nil, errors.New("unsupported SASL mechanism " + settings.Mechanism)
}
