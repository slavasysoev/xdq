package postgres

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5"
	"github.com/testcontainers/testcontainers-go"
	postgrescontainer "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/source"
)

const (
	postgresImage = "postgres:17-alpine"
	sourceName    = "orders-db"
	// password is used for every role in the tests; errors must never show it.
	password = "pa55-Word"
)

// Compile-time check that Source implements the source contract.
var _ source.Source = (*Source)(nil)

func TestBuildSnapshot(t *testing.T) {
	server := serverState{
		databases: []string{"postgres", "orders"},
		clients: []clientSessions{
			{database: "orders", address: "10.0.0.2", user: "billing", sessions: 2},
			{database: "orders", address: "10.0.0.1", user: "reports", sessions: 1},
			{database: "orders", address: "10.0.0.1", user: "billing", sessions: 3},
			{database: "postgres", address: "10.0.0.1", user: "billing", sessions: 1},
			// Created between the two queries: known only from its session.
			{database: "audit", address: "10.0.0.2", user: "audit", sessions: 1},
		},
	}

	snapshot := buildSnapshot(sourceName, server)

	var nodes []graph.NodeID
	for _, node := range snapshot.Nodes {
		nodes = append(nodes, node.ID)
	}
	wantNodes := []graph.NodeID{
		graph.DatabaseID(sourceName, "audit"), graph.DatabaseID(sourceName, "orders"), graph.DatabaseID(sourceName, "postgres"),
		graph.EndpointID("10.0.0.1"), graph.EndpointID("10.0.0.2"),
	}
	if !reflect.DeepEqual(nodes, wantNodes) {
		t.Errorf("nodes = %v, want %v", nodes, wantNodes)
	}
	var edges []string
	for _, edge := range snapshot.Edges {
		var details []string
		for _, evidence := range edge.Evidence {
			details = append(details, evidence.Detail)
		}
		edges = append(edges, string(edge.From)+" -> "+string(edge.To)+": "+strings.Join(details, "; "))
	}
	wantEdges := []string{
		"endpoint:10.0.0.1 -> database:orders-db/orders: 3 sessions as user billing; 1 session as user reports",
		"endpoint:10.0.0.1 -> database:orders-db/postgres: 1 session as user billing",
		"endpoint:10.0.0.2 -> database:orders-db/audit: 1 session as user audit",
		"endpoint:10.0.0.2 -> database:orders-db/orders: 2 sessions as user billing",
	}
	if !reflect.DeepEqual(edges, wantEdges) {
		t.Errorf("edges =\n%s\nwant\n%s", strings.Join(edges, "\n"), strings.Join(wantEdges, "\n"))
	}
}

func TestErrorsHidePassword(t *testing.T) {
	tests := []struct {
		name string
		dsn  string
	}{
		{name: "invalid dsn", dsn: "postgres://xdq:" + password + "@db:not-a-port/orders"},
		{name: "invalid key-value dsn", dsn: "host=db password=" + password + " port=not-a-port"},
		{name: "connection refused", dsn: "postgres://xdq:" + password + "@127.0.0.1:1/orders?connect_timeout=2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			postgresSource, err := openSource(t, test.dsn)
			if err == nil {
				_, err = postgresSource.Discover(t.Context())
			}
			if err == nil {
				t.Fatal("want an error")
			}
			if strings.Contains(err.Error(), password) {
				t.Errorf("error shows the password: %v", err)
			}
		})
	}
}

// TestDiscover runs against a real server: the role app holds two sessions to the database orders,
// and xdq connects to the database postgres as a role with pg_monitor.
func TestDiscover(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test: needs Docker")
	}
	ctx := t.Context()
	container, err := postgrescontainer.Run(ctx, postgresImage,
		postgrescontainer.WithDatabase("orders"),
		postgrescontainer.WithUsername("app"),
		postgrescontainer.WithPassword(password),
		postgrescontainer.BasicWaitStrategies(),
	)
	testcontainers.CleanupContainer(t, container)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	appDSN, err := container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		t.Fatalf("connection string: %v", err)
	}
	// The two sessions of app stay open until the test ends. The setup runs in the first one, so
	// that no other session briefly shows up.
	setup := connect(t, appDSN)
	connect(t, appDSN)
	for _, statement := range []string{
		"CREATE ROLE monitor LOGIN PASSWORD '" + password + "'",
		"GRANT pg_monitor TO monitor",
		"CREATE ROLE plain LOGIN PASSWORD '" + password + "'",
	} {
		_, err := setup.Exec(ctx, statement)
		if err != nil {
			t.Fatalf("%s: %v", statement, err)
		}
	}

	t.Run("with pg_monitor", func(t *testing.T) {
		snapshot := discover(t, roleDSN(t, appDSN, "monitor"))

		orders := graph.DatabaseID(sourceName, "orders")
		var toOrders, toPostgres []graph.Edge
		for _, edge := range snapshot.Edges {
			switch edge.To {
			case orders:
				toOrders = append(toOrders, edge)
			case graph.DatabaseID(sourceName, "postgres"):
				toPostgres = append(toPostgres, edge)
			}
		}
		if len(toOrders) != 1 || toOrders[0].From.Kind() != graph.NodeEndpoint || toOrders[0].Kind != graph.EdgeConnection {
			t.Fatalf("edges to orders = %+v, want one connection from an endpoint", toOrders)
		}
		if detail := toOrders[0].Evidence[0].Detail; detail != "2 sessions as user app" {
			t.Errorf("evidence = %q, want %q", detail, "2 sessions as user app")
		}
		// The only session to the database postgres is xdq's own.
		if len(toPostgres) != 0 {
			t.Errorf("edges to postgres = %+v, want none: xdq's own session must be excluded", toPostgres)
		}
	})

	t.Run("without pg_monitor", func(t *testing.T) {
		postgresSource, err := openSource(t, roleDSN(t, appDSN, "plain"))
		if err != nil {
			t.Fatalf("New: %v", err)
		}

		_, err = postgresSource.Discover(t.Context())

		if err == nil || !strings.Contains(err.Error(), "grant pg_monitor to plain") {
			t.Errorf("error = %v, want it to ask for pg_monitor", err)
		}
	})
}

// loadSettings loads a config with the DSN the way users configure it: as an env reference.
func loadSettings(t *testing.T, dsn string) config.PostgresConfig {
	t.Helper()
	t.Setenv("XDQ_TEST_POSTGRES_DSN", dsn)
	path := filepath.Join(t.TempDir(), "xdq.yaml")
	err := os.WriteFile(path, []byte("version: 1\nsources:\n  "+sourceName+":\n    postgres: {dsn: '${env:XDQ_TEST_POSTGRES_DSN}'}\n"), 0o600)
	if err != nil {
		t.Fatal(err)
	}
	loaded, err := config.Load(path)
	if err != nil {
		t.Fatalf("load config: %v", err)
	}

	return *loaded.Sources[sourceName].Postgres
}

// openSource creates a source that is closed when the test ends.
func openSource(t *testing.T, dsn string) (*Source, error) {
	t.Helper()
	postgresSource, err := New(sourceName, loadSettings(t, dsn))
	if err != nil {
		return nil, err
	}
	t.Cleanup(func() {
		closeErr := postgresSource.Close()
		if closeErr != nil {
			t.Errorf("Close: %v", closeErr)
		}
	})

	return postgresSource, nil
}

func discover(t *testing.T, dsn string) graph.Snapshot {
	t.Helper()
	postgresSource, err := openSource(t, dsn)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	snapshot, err := postgresSource.Discover(t.Context())
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	return snapshot
}

// roleDSN connects as the role to the database postgres instead of orders.
func roleDSN(t *testing.T, appDSN, role string) string {
	t.Helper()
	dsn := strings.Replace(appDSN, "app:"+password, role+":"+password, 1)
	dsn = strings.Replace(dsn, "/orders?", "/postgres?", 1)
	if dsn == appDSN {
		t.Fatalf("unexpected connection string format: %s", appDSN)
	}

	return dsn
}

// connect opens a session that stays open until the test ends.
func connect(t *testing.T, dsn string) *pgx.Conn {
	t.Helper()
	connection, err := pgx.Connect(t.Context(), dsn)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	t.Cleanup(func() {
		_ = connection.Close(context.Background())
	})

	return connection
}
