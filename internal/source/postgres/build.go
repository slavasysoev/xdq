package postgres

import (
	"cmp"
	"fmt"
	"slices"

	"github.com/slavasysoev/xdq/internal/graph"
)

// buildSnapshot turns the server state into database and endpoint nodes and one connection edge
// per address and database. The output is sorted, so the same state always gives the same snapshot.
func buildSnapshot(sourceName string, server serverState) graph.Snapshot {
	snapshot := graph.NewSnapshot()
	clients := slices.Clone(server.clients)
	// Sorted by address and database first, so the sessions of one edge are next to each other.
	slices.SortFunc(clients, func(left, right clientSessions) int {
		return cmp.Or(cmp.Compare(left.address, right.address), cmp.Compare(left.database, right.database),
			cmp.Compare(left.user, right.user))
	})

	// A database created between the two queries is known only from its sessions.
	databases := slices.Clone(server.databases)
	for _, client := range clients {
		databases = append(databases, client.database)
		addConnection(&snapshot, sourceName, client)
	}
	slices.Sort(databases)
	for _, name := range slices.Compact(databases) {
		snapshot.Nodes = append(snapshot.Nodes, graph.NewDatabase(sourceName, name))
	}
	snapshot.Sort()

	return snapshot
}

// addConnection adds the sessions of a client to the edge from its address to the database. Every
// role gets its own evidence. The endpoint node is added with the first edge from its address.
func addConnection(snapshot *graph.Snapshot, sourceName string, client clientSessions) {
	endpointID := graph.EndpointID(client.address)
	databaseID := graph.DatabaseID(sourceName, client.database)
	evidence := graph.Evidence{Source: sourceName, Detail: sessionsDetail(client)}

	last := len(snapshot.Edges) - 1
	if last >= 0 && snapshot.Edges[last].From == endpointID && snapshot.Edges[last].To == databaseID {
		snapshot.Edges[last].Evidence = append(snapshot.Edges[last].Evidence, evidence)

		return
	}
	if last < 0 || snapshot.Edges[last].From != endpointID {
		snapshot.Nodes = append(snapshot.Nodes, graph.NewEndpoint(client.address))
	}
	snapshot.Edges = append(snapshot.Edges, graph.Edge{
		From:       endpointID,
		To:         databaseID,
		Kind:       graph.EdgeConnection,
		Confidence: graph.ConfidenceObserved,
		Evidence:   []graph.Evidence{evidence},
	})
}

// sessionsDetail tells how many sessions a role holds, e.g. "2 sessions as user billing".
func sessionsDetail(client clientSessions) string {
	noun := "sessions"
	if client.sessions == 1 {
		noun = "session"
	}

	return fmt.Sprintf("%d %s as user %s", client.sessions, noun, client.user)
}
