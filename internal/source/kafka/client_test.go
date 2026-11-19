package kafka

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"math/big"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/slavasysoev/xdq/internal/config"
)

// writeCertificate writes a self-signed certificate and its key as PEM files.
func writeCertificate(t *testing.T) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	template := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "xdq-test"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	directory := t.TempDir()
	certFile = filepath.Join(directory, "cert.pem")
	keyFile = filepath.Join(directory, "key.pem")
	err = os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile(keyFile, pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: keyDER}), 0o600)
	if err != nil {
		t.Fatal(err)
	}

	return certFile, keyFile
}

func TestBuildTLSConfig(t *testing.T) {
	certFile, keyFile := writeCertificate(t)

	tlsConfig, err := buildTLSConfig(config.TLSConfig{CAFile: certFile, CertFile: certFile, KeyFile: keyFile})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsConfig.RootCAs == nil {
		t.Error("RootCAs not set from caFile")
	}
	if len(tlsConfig.Certificates) != 1 {
		t.Errorf("client certificates = %d, want 1", len(tlsConfig.Certificates))
	}
}

func TestBuildTLSConfigWithoutFilesUsesSystemPool(t *testing.T) {
	tlsConfig, err := buildTLSConfig(config.TLSConfig{})
	if err != nil {
		t.Fatalf("buildTLSConfig: %v", err)
	}
	if tlsConfig.RootCAs != nil || len(tlsConfig.Certificates) != 0 {
		t.Error("expected system CA pool and no client certificate")
	}
}

func TestBuildTLSConfigErrors(t *testing.T) {
	notPEM := filepath.Join(t.TempDir(), "not.pem")
	err := os.WriteFile(notPEM, []byte("not a certificate"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	tests := []struct {
		name     string
		settings config.TLSConfig
	}{
		{name: "missing CA file", settings: config.TLSConfig{CAFile: filepath.Join(t.TempDir(), "missing.pem")}},
		{name: "CA file without certificates", settings: config.TLSConfig{CAFile: notPEM}},
		{name: "broken client key pair", settings: config.TLSConfig{CertFile: notPEM, KeyFile: notPEM}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := buildTLSConfig(test.settings)
			if err == nil {
				t.Error("expected an error, got nil")
			}
		})
	}
}

func TestSASLMechanism(t *testing.T) {
	for _, name := range []string{"PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"} {
		t.Run(name, func(t *testing.T) {
			mechanism, err := saslMechanism(config.SASLConfig{Mechanism: name, Username: "xdq"})
			if err != nil {
				t.Fatalf("saslMechanism: %v", err)
			}
			if mechanism.Name() != name {
				t.Errorf("mechanism = %q, want %q", mechanism.Name(), name)
			}
		})
	}
}
