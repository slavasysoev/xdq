// Package postgres discovers the databases of a Postgres server and the clients connected to them.
// It reads the system views pg_database and pg_stat_activity only, in a read-only session: it never
// reads table data and never changes anything.
package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
)

// applicationName marks the session of xdq in pg_stat_activity and in the server logs.
const applicationName = "xdq"

// Source is a Postgres server.
type Source struct {
	name string
	pool *pgxpool.Pool
}

// New creates a source. It does not connect: the pool connects on the first query and reconnects
// after a broken connection.
func New(name string, settings config.PostgresConfig) (*Source, error) {
	poolConfig, err := pgxpool.ParseConfig(settings.DSN.Value())
	if err != nil {
		// The parse error is not wrapped: it may quote the DSN, and the DSN holds the password.
		return nil, fmt.Errorf("postgres source %s: invalid dsn, see the pgx connection string format", name)
	}
	// One session is enough: the queries run one after another.
	poolConfig.MaxConns = 1
	// The server rejects any write in this session, whatever xdq does by mistake.
	poolConfig.ConnConfig.RuntimeParams["default_transaction_read_only"] = "on"
	poolConfig.ConnConfig.RuntimeParams["application_name"] = applicationName
	pool, err := pgxpool.NewWithConfig(context.Background(), poolConfig)
	if err != nil {
		return nil, fmt.Errorf("postgres source %s: %w", name, err)
	}

	return &Source{name: name, pool: pool}, nil
}

// Name returns the name of the source in the config.
func (source *Source) Name() string {
	return source.name
}

// Type returns config.TypePostgres.
func (source *Source) Type() string {
	return config.TypePostgres
}

// Discover reads the databases and the client sessions of the server.
func (source *Source) Discover(ctx context.Context) (graph.Snapshot, error) {
	server, err := fetch(ctx, source.pool)
	if err != nil {
		return graph.Snapshot{}, fmt.Errorf("postgres source %s: %w", source.name, err)
	}

	return buildSnapshot(source.name, server), nil
}

// Close closes the connection to the server.
func (source *Source) Close() error {
	source.pool.Close()

	return nil
}
