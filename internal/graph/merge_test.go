package graph

import (
	"reflect"
	"testing"
)

func TestMerge(t *testing.T) {
	kafka := Snapshot{
		Nodes: []Node{NewTopic("main", "orders"), NewEndpoint("10.0.0.1")},
		Edges: []Edge{{From: TopicID("main", "orders"), Kind: EdgeConsumer, To: GroupID("main", "billing")}},
		Observations: Observations{
			Topics: map[NodeID]TopicObservation{TopicID("main", "orders"): {Partitions: 3}},
		},
	}
	postgres := Snapshot{
		Nodes: []Node{NewEndpoint("10.0.0.1"), NewDatabase("db", "orders")},
		Edges: []Edge{{From: EndpointID("10.0.0.1"), Kind: EdgeConnection, To: DatabaseID("db", "orders")}},
	}

	merged := Merge(postgres, kafka)

	wantNodes := []NodeID{DatabaseID("db", "orders"), EndpointID("10.0.0.1"), TopicID("main", "orders")}
	var gotNodes []NodeID
	for _, node := range merged.Nodes {
		gotNodes = append(gotNodes, node.ID)
	}
	if !reflect.DeepEqual(gotNodes, wantNodes) {
		t.Errorf("nodes = %v, want %v", gotNodes, wantNodes)
	}
	if len(merged.Edges) != 2 || merged.Edges[0].From != EndpointID("10.0.0.1") {
		t.Errorf("edges = %+v, want 2 sorted by From", merged.Edges)
	}
	if merged.Observations.Topics[TopicID("main", "orders")].Partitions != 3 {
		t.Error("topic observation lost")
	}
}

func TestMergeIsDeterministic(t *testing.T) {
	first := Snapshot{Nodes: []Node{NewTopic("a", "x")}}
	second := Snapshot{Nodes: []Node{NewTopic("b", "y")}}

	if !reflect.DeepEqual(Merge(first, second), Merge(second, first)) {
		t.Error("merge result depends on the snapshot order")
	}
}
