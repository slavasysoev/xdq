package kafka

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/moby/moby/api/types/network"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/testenv"
)

// apacheKafkaImage is plain Apache Kafka: its config is written by the test, unlike the
// confluent-local image of testenv.StartKafka, which only has a PLAINTEXT listener.
const apacheKafkaImage = "apache/kafka:4.1.0"

const (
	kafkaUser     = "xdq"
	kafkaPassword = "xdq-secret"
)

// securedKafka is a broker with one listener per way to authenticate.
type securedKafka struct {
	// sasl is SASL_PLAINTEXT with PLAIN, SCRAM-SHA-256 and SCRAM-SHA-512.
	sasl string
	// saslTLS is SASL_SSL with SCRAM-SHA-512. The server certificate is signed by the test CA.
	saslTLS string
	// mutualTLS is SSL that requires a client certificate signed by the test CA.
	mutualTLS string
	// certificates is the directory with ca.pem, client-cert.pem and client-key.pem.
	certificates string
}

// TestDiscoverWithAuth connects to a real broker in every auth mode of §6.1. A wrong credential
// must fail with its own reason, not with a timeout.
func TestDiscoverWithAuth(t *testing.T) {
	broker := startSecuredKafka(t)
	caFile := filepath.Join(broker.certificates, "ca.pem")
	clientTLS := fmt.Sprintf("{caFile: '%s', certFile: '%s', keyFile: '%s'}", caFile,
		filepath.Join(broker.certificates, "client-cert.pem"), filepath.Join(broker.certificates, "client-key.pem"))
	tests := []struct {
		name      string
		settings  string
		password  string
		wantError string
	}{
		{name: "SASL PLAIN", settings: saslSettings(broker.sasl, config.SASLPlain, "")},
		{name: "SCRAM-SHA-256", settings: saslSettings(broker.sasl, config.SASLScramSHA256, "")},
		{name: "SCRAM-SHA-512 over TLS with a custom CA", settings: saslSettings(broker.saslTLS, config.SASLScramSHA512, "tls: {caFile: '"+caFile+"'}")},
		{name: "mutual TLS", settings: "brokers: [" + broker.mutualTLS + "]\ntls: " + clientTLS},
		{
			name:      "wrong password",
			settings:  saslSettings(broker.sasl, config.SASLScramSHA512, ""),
			password:  "wrong",
			wantError: "SASL_AUTHENTICATION_FAILED",
		},
		{
			name:      "TLS without the CA",
			settings:  saslSettings(broker.saslTLS, config.SASLScramSHA512, "tls: {}"),
			wantError: "certificate signed by unknown authority",
		},
		{
			name:      "mutual TLS without a client certificate",
			settings:  "brokers: [" + broker.mutualTLS + "]\ntls: {caFile: '" + caFile + "'}",
			wantError: "tls: bad certificate",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			password := kafkaPassword
			if test.password != "" {
				password = test.password
			}
			kafkaSource, err := New(sourceName, loadKafkaSettings(t, test.settings, password))
			if err != nil {
				t.Fatalf("New: %v", err)
			}
			t.Cleanup(func() {
				closeErr := kafkaSource.Close()
				if closeErr != nil {
					t.Errorf("Close: %v", closeErr)
				}
			})
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()

			_, err = kafkaSource.Discover(ctx)

			if test.wantError == "" && err != nil {
				t.Errorf("Discover: %v", err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Errorf("error = %v, want it to contain %q", err, test.wantError)
			}
		})
	}
}

func saslSettings(broker, mechanism, tls string) string {
	settings := fmt.Sprintf("brokers: [%s]\nsasl: {mechanism: %s, username: %s, password: '${env:XDQ_TEST_KAFKA_PASSWORD}'}\n", broker, mechanism, kafkaUser)

	return settings + tls
}

// loadKafkaSettings loads the source the way users configure it: the password is an env reference.
func loadKafkaSettings(t *testing.T, settings, password string) config.KafkaConfig {
	t.Helper()
	t.Setenv("XDQ_TEST_KAFKA_PASSWORD", password)
	indented := strings.ReplaceAll(settings, "\n", "\n      ")
	content := "version: 1\nsources:\n  " + sourceName + ":\n    kafka:\n      " + indented + "\n"
	path := filepath.Join(t.TempDir(), "xdq.yaml")
	err := os.WriteFile(path, []byte(content), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config:\n%s\n%v", content, err)
	}

	return *loaded.Sources[sourceName].Kafka
}

// startSecuredKafka starts a broker that is removed when the test ends.
//
// A broker returns its advertised address to clients, so it must know the host port before it
// starts. The test takes free ports from the OS and maps each to the same port in the container.
// This needs a local Docker: with a remote DOCKER_HOST the broker is not on localhost.
func startSecuredKafka(t *testing.T) securedKafka {
	t.Helper()
	testenv.RequireDocker(t)
	directory := t.TempDir()
	writeCertificates(t, directory)
	ports := []string{freePort(t), freePort(t), freePort(t)}
	properties := fmt.Sprintf(serverProperties, ports[0], ports[1], ports[2], ports[0], ports[1], ports[2], kafkaUser, kafkaPassword)
	scram := fmt.Sprintf("name=%s,password=%s", kafkaUser, kafkaPassword)
	// The SCRAM credentials are created when the storage is formatted, before the broker starts.
	script := "/opt/kafka/bin/kafka-storage.sh format -t MkU3OEVBNTcwNTJENDM2Qg -c /tmp/xdq/server.properties" +
		" --add-scram 'SCRAM-SHA-256=[" + scram + "]' --add-scram 'SCRAM-SHA-512=[" + scram + "]'" +
		" && exec /opt/kafka/bin/kafka-server-start.sh /tmp/xdq/server.properties"

	exposed := make([]string, 0, len(ports))
	bindings := network.PortMap{}
	for _, port := range ports {
		exposed = append(exposed, port+"/tcp")
		bindings[network.MustParsePort(port+"/tcp")] = []network.PortBinding{
			{HostIP: netip.MustParseAddr("127.0.0.1"), HostPort: port},
		}
	}
	kafkaContainer, err := testcontainers.Run(t.Context(), apacheKafkaImage,
		testcontainers.WithFiles(
			containerFile(t, filepath.Join(directory, "server.pem"), "/tmp/xdq/server.pem"),
			containerFile(t, filepath.Join(directory, "ca.pem"), "/tmp/xdq/ca.pem"),
			testcontainers.ContainerFile{Reader: strings.NewReader(properties), ContainerFilePath: "/tmp/xdq/server.properties", FileMode: 0o644},
		),
		testcontainers.WithEntrypoint("sh", "-c", script),
		testcontainers.WithExposedPorts(exposed...),
		testcontainers.WithHostConfigModifier(func(hostConfig *container.HostConfig) {
			hostConfig.PortBindings = bindings
		}),
		testcontainers.WithWaitStrategy(wait.ForLog("Kafka Server started").WithStartupTimeout(2*time.Minute)),
	)
	testcontainers.CleanupContainer(t, kafkaContainer)
	if err != nil {
		t.Fatalf("start kafka: %v", err)
	}

	return securedKafka{
		sasl:         "localhost:" + ports[0],
		saslTLS:      "localhost:" + ports[1],
		mutualTLS:    "localhost:" + ports[2],
		certificates: directory,
	}
}

// serverProperties configures a single node that is both broker and controller. The broker
// talks to itself over INTERNAL, which is not mapped to the host. Arguments: the three host ports
// twice, then the PLAIN user and password.
const serverProperties = `process.roles=broker,controller
node.id=1
controller.quorum.voters=1@localhost:9093
controller.listener.names=CONTROLLER
inter.broker.listener.name=INTERNAL
listeners=CONTROLLER://:9093,INTERNAL://:9092,SASL://:%s,SASLSSL://:%s,MTLS://:%s
advertised.listeners=INTERNAL://localhost:9092,SASL://localhost:%s,SASLSSL://localhost:%s,MTLS://localhost:%s
listener.security.protocol.map=CONTROLLER:PLAINTEXT,INTERNAL:PLAINTEXT,SASL:SASL_PLAINTEXT,SASLSSL:SASL_SSL,MTLS:SSL
log.dirs=/tmp/kraft-logs
offsets.topic.replication.factor=1
transaction.state.log.replication.factor=1
transaction.state.log.min.isr=1
listener.name.sasl.sasl.enabled.mechanisms=PLAIN,SCRAM-SHA-256,SCRAM-SHA-512
listener.name.sasl.plain.sasl.jaas.config=org.apache.kafka.common.security.plain.PlainLoginModule required user_%s="%s";
listener.name.sasl.scram-sha-256.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.sasl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.saslssl.sasl.enabled.mechanisms=SCRAM-SHA-512
listener.name.saslssl.scram-sha-512.sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required;
listener.name.mtls.ssl.client.auth=required
ssl.keystore.type=PEM
ssl.keystore.location=/tmp/xdq/server.pem
ssl.truststore.type=PEM
ssl.truststore.location=/tmp/xdq/ca.pem
`

func containerFile(t *testing.T, hostPath, containerPath string) testcontainers.ContainerFile {
	t.Helper()
	content, err := os.ReadFile(hostPath)
	if err != nil {
		t.Fatal(err)
	}

	return testcontainers.ContainerFile{Reader: strings.NewReader(string(content)), ContainerFilePath: containerPath, FileMode: 0o644}
}

// freePort returns a TCP port that is free on the host at the moment of the call.
func freePort(t *testing.T) string {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := strconv.Itoa(listener.Addr().(*net.TCPAddr).Port)
	err = listener.Close()
	if err != nil {
		t.Fatal(err)
	}

	return port
}

// writeCertificates writes a test CA (ca.pem), the broker's key and certificate for localhost
// (server.pem, the PEM keystore format of Kafka) and a client key and certificate.
func writeCertificates(t *testing.T, directory string) {
	t.Helper()
	caKey := newKey(t)
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "xdq test CA"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign,
	}
	caCertificate := sign(t, caTemplate, caTemplate, caKey, caKey)
	writePEM(t, filepath.Join(directory, "ca.pem"), pemCertificate(caCertificate))

	serverKey := newKey(t)
	serverCertificate := sign(t, &x509.Certificate{
		SerialNumber: big.NewInt(2),
		Subject:      pkix.Name{CommonName: "localhost"},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1)},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature | x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
	}, caTemplate, serverKey, caKey)
	writePEM(t, filepath.Join(directory, "server.pem"), pemKey(t, serverKey)+pemCertificate(serverCertificate))

	clientKey := newKey(t)
	clientCertificate := sign(t, &x509.Certificate{
		SerialNumber: big.NewInt(3),
		Subject:      pkix.Name{CommonName: kafkaUser},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
	}, caTemplate, clientKey, caKey)
	writePEM(t, filepath.Join(directory, "client-cert.pem"), pemCertificate(clientCertificate))
	writePEM(t, filepath.Join(directory, "client-key.pem"), pemKey(t, clientKey))
}

func newKey(t *testing.T) *rsa.PrivateKey {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}

	return key
}

// sign returns the DER certificate of template, signed by the parent certificate and its key.
func sign(t *testing.T, template, parent *x509.Certificate, key, parentKey *rsa.PrivateKey) []byte {
	t.Helper()
	certificate, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		t.Fatal(err)
	}

	return certificate
}

func pemCertificate(certificate []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate}))
}

// pemKey encodes the key as unencrypted PKCS#8, the only key format Kafka reads from PEM.
func pemKey(t *testing.T, key *rsa.PrivateKey) string {
	t.Helper()
	encoded, err := x509.MarshalPKCS8PrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}

	return string(pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: encoded}))
}

func writePEM(t *testing.T, path, content string) {
	t.Helper()
	err := os.WriteFile(path, []byte(content), 0o600)
	if err != nil {
		t.Fatal(err)
	}
}
