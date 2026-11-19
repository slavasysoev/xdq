package postgres

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// serverState is what the server reported, before it is turned into a snapshot.
type serverState struct {
	databases []string
	clients   []clientSessions
}

// clientSessions counts the sessions of one role from one address to one database.
type clientSessions struct {
	database string
	address  string
	user     string
	sessions int
}

// Postgres shows the client address of other roles' sessions only to members of
// pg_read_all_stats (pg_monitor includes it). This is the same check the server makes.
const privilegeQuery = `SELECT current_user, pg_has_role(current_user, 'pg_read_all_stats', 'USAGE')`

const databasesQuery = `SELECT datname FROM pg_database WHERE datallowconn AND NOT datistemplate`

// sessionsQuery counts client sessions. Replication and background processes have another
// backend_type. Sessions over a Unix socket have no address and cannot be matched to a workload.
const sessionsQuery = `
SELECT datname, host(client_addr), coalesce(usename, ''), count(*)
FROM pg_stat_activity
WHERE backend_type = 'client backend' AND pid <> pg_backend_pid() AND client_addr IS NOT NULL
GROUP BY datname, client_addr, usename`

func fetch(ctx context.Context, pool *pgxpool.Pool) (serverState, error) {
	var (
		user       string
		privileged bool
	)
	err := pool.QueryRow(ctx, privilegeQuery).Scan(&user, &privileged)
	if err != nil {
		return serverState{}, err
	}
	if !privileged {
		// Without it the clients of other roles would silently be missing from the graph.
		return serverState{}, fmt.Errorf("role %s cannot see the client addresses of other roles' sessions; grant pg_monitor to %s", user, user)
	}

	rows, err := pool.Query(ctx, databasesQuery)
	if err != nil {
		return serverState{}, fmt.Errorf("list databases: %w", err)
	}
	databases, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return serverState{}, fmt.Errorf("list databases: %w", err)
	}

	rows, err = pool.Query(ctx, sessionsQuery)
	if err != nil {
		return serverState{}, fmt.Errorf("list sessions: %w", err)
	}
	clients, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (clientSessions, error) {
		var client clientSessions
		err := row.Scan(&client.database, &client.address, &client.user, &client.sessions)

		return client, err
	})
	if err != nil {
		return serverState{}, fmt.Errorf("list sessions: %w", err)
	}

	return serverState{databases: databases, clients: clients}, nil
}
