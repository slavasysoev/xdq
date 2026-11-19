package resolve

import (
	"reflect"
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
)

func TestInferProducers(t *testing.T) {
	orders := graph.NewTopic("main", "orders.created")
	invoices := graph.NewTopic("main", "invoices.created")
	invoicesOther := graph.NewTopic("other", "invoices.created")
	snapshot := graph.NewSnapshot()
	snapshot.Nodes = []graph.Node{orders, invoices, invoicesOther, group, billing}
	snapshot.Edges = []graph.Edge{
		{From: orders.ID, To: group.ID, Kind: graph.EdgeConsumer},
		{From: group.ID, To: billing.ID, Kind: graph.EdgeMembership},
	}
	snapshot.ConfigValues = []graph.ConfigValue{
		{Workload: billing.ID, Origin: "env KAFKA_IN_TOPIC", Value: "orders.created"},
		{Workload: billing.ID, Origin: "env KAFKA_OUT_TOPIC", Value: "invoices.created"},
		{Workload: billing.ID, Origin: "configmap app/application.yml", Value: "out: invoices.created\nother: unknown.topic"},
	}
	snapshot.Sort()

	resolved := Resolve(snapshot)

	evidence := []graph.Evidence{
		{Source: "test", Detail: "configmap app/application.yml mentions invoices.created"},
		{Source: "test", Detail: "env KAFKA_OUT_TOPIC=invoices.created"},
	}
	want := []graph.Edge{
		{From: billing.ID, To: invoices.ID, Kind: graph.EdgeProducer, Confidence: graph.ConfidenceInferred, Evidence: evidence},
		{From: billing.ID, To: invoicesOther.ID, Kind: graph.EdgeProducer, Confidence: graph.ConfidenceInferred, Evidence: evidence},
	}
	var got []graph.Edge
	for _, edge := range resolved.Edges {
		if edge.Kind == graph.EdgeProducer {
			got = append(got, edge)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("producer edges =\n%+v\nwant\n%+v\n(the consumed orders.created must not be a producer edge)", got, want)
	}
}
