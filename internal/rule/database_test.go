package rule

import (
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

var ordersDatabase = graph.DatabaseID("db", "orders")

// connectedTo is the database orders with one connection per client, plus an unresolved one.
func connectedTo(workloads ...graph.Node) graph.Snapshot {
	snapshot := graph.NewSnapshot()
	snapshot.Nodes = []graph.Node{graph.NewDatabase("db", "orders"), graph.NewEndpoint("192.168.1.20")}
	for _, workload := range workloads {
		snapshot.Nodes = append(snapshot.Nodes, workload)
		snapshot.Edges = append(snapshot.Edges, graph.Edge{From: workload.ID, To: ordersDatabase, Kind: graph.EdgeConnection})
	}
	// An unresolved client has no environment: unresolved-client reports it.
	snapshot.Edges = append(snapshot.Edges, graph.Edge{From: laptop, To: ordersDatabase, Kind: graph.EdgeConnection})
	snapshot.Sort()

	return snapshot
}

func TestDatabaseSpansEnvironments(t *testing.T) {
	billingA := workloadIn("stand-a", "billing")
	billingB := workloadIn("stand-b", "billing")
	reportsA := workloadIn("stand-a", "reports")
	tests := []struct {
		name         string
		snapshot     graph.Snapshot
		wantSubjects [][]graph.NodeID
	}{
		{name: "one environment", snapshot: connectedTo(billingA, reportsA)},
		{name: "only unresolved clients", snapshot: connectedTo()},
		{
			name:         "two environments",
			snapshot:     connectedTo(billingA, reportsA, billingB),
			wantSubjects: [][]graph.NodeID{{ordersDatabase, billingA.ID, reportsA.ID, billingB.ID}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			findings := checkDatabaseSpansEnvironments(Input{Graph: test.snapshot}, settings{name: "database-spans-environments"})

			assertSubjects(t, findings, report.SeverityError, test.wantSubjects)
		})
	}
}
