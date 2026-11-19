package tui

import (
	"reflect"
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

func TestBuildTree(t *testing.T) {
	stalled := graph.NewGroup("main", "stalled")
	cycleA := graph.NewTopic("main", "ping")
	cycleB := graph.NewGroup("main", "pong")
	snapshot := graph.NewSnapshot()
	snapshot.Nodes = []graph.Node{orders, billing, stalled, workload, cycleA, cycleB}
	snapshot.Edges = []graph.Edge{
		{From: orders.ID, To: billing.ID, Kind: graph.EdgeConsumer},
		{From: orders.ID, To: stalled.ID, Kind: graph.EdgeConsumer},
		{From: billing.ID, To: workload.ID, Kind: graph.EdgeMembership},
		// Two parents: the workload is drawn in full once.
		{From: stalled.ID, To: workload.ID, Kind: graph.EdgeMembership},
		// A cycle without a root.
		{From: cycleA.ID, To: cycleB.ID, Kind: graph.EdgeConsumer},
		{From: cycleB.ID, To: cycleA.ID, Kind: graph.EdgeProducer},
	}
	snapshot.Sort()
	chain := report.NewChain("flow", graph.Subgraph{Snapshot: snapshot, Gaps: []graph.NodeID{workload.ID}})

	var got []string
	for _, line := range buildTree(chain) {
		text := line.prefix + string(line.edge) + " " + string(line.node.ID)
		if line.repeated {
			text += " (repeated)"
		}
		got = append(got, text)
	}

	want := []string{
		" topic:main/orders.created",
		"├ consumer group:main/billing",
		"│ └ membership workload:test/stand-a/deployment/billing",
		"└ consumer group:main/stalled",
		"  └ membership workload:test/stand-a/deployment/billing (repeated)",
		" group:main/pong",
		"└ producer topic:main/ping",
		"  └ consumer group:main/pong (repeated)",
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("tree =\n%v\nwant\n%v", got, want)
	}
}
