package graph

import (
	"reflect"
	"testing"
)

func workloadIn(environment, name string) Node {
	workload := NewWorkload("test", environment, WorkloadDeployment, name)
	workload.Environment = environment

	return workload
}

// chainGraph is the order flow of stand-a, with a member of stand-b in the billing group:
//
//	orders → billing group → billing (stand-a) → invoices → mailer group → mailer (stand-a)
//	                       → billing (stand-b) → payments
//	                       → laptop endpoint
//	billing (stand-a) → orders database
//	invoices → audit group → audit (stand-a) → orders (a cycle)
type chainGraph struct {
	snapshot                              Snapshot
	orders, invoices, payments, unrelated Node
	billingGroup, mailerGroup, auditGroup Node
	billingA, billingB, mailer, audit     Node
	laptop, database                      Node
}

func newChainGraph() chainGraph {
	graph := chainGraph{
		orders:       NewTopic("main", "orders"),
		invoices:     NewTopic("main", "invoices"),
		payments:     NewTopic("main", "payments"),
		unrelated:    NewTopic("main", "unrelated"),
		billingGroup: NewGroup("main", "billing"),
		mailerGroup:  NewGroup("main", "mailer"),
		auditGroup:   NewGroup("main", "audit"),
		billingA:     workloadIn("stand-a", "billing"),
		billingB:     workloadIn("stand-b", "billing"),
		mailer:       workloadIn("stand-a", "mailer"),
		audit:        workloadIn("stand-a", "audit"),
		laptop:       NewEndpoint("192.168.1.20"),
		database:     NewDatabase("db", "orders"),
	}
	edge := func(from, to Node, kind EdgeKind) Edge {
		return Edge{From: from.ID, To: to.ID, Kind: kind}
	}
	snapshot := NewSnapshot()
	snapshot.Nodes = []Node{
		graph.orders, graph.invoices, graph.payments, graph.unrelated,
		graph.billingGroup, graph.mailerGroup, graph.auditGroup,
		graph.billingA, graph.billingB, graph.mailer, graph.audit, graph.laptop, graph.database,
	}
	snapshot.Edges = []Edge{
		edge(graph.orders, graph.billingGroup, EdgeConsumer),
		edge(graph.billingGroup, graph.billingA, EdgeMembership),
		edge(graph.billingGroup, graph.billingB, EdgeMembership),
		edge(graph.billingGroup, graph.laptop, EdgeMembership),
		edge(graph.billingA, graph.invoices, EdgeProducer),
		edge(graph.billingA, graph.database, EdgeConnection),
		edge(graph.billingB, graph.payments, EdgeProducer),
		edge(graph.invoices, graph.mailerGroup, EdgeConsumer),
		edge(graph.mailerGroup, graph.mailer, EdgeMembership),
		edge(graph.invoices, graph.auditGroup, EdgeConsumer),
		edge(graph.auditGroup, graph.audit, EdgeMembership),
		edge(graph.audit, graph.orders, EdgeProducer),
	}
	snapshot.Observations.Topics[graph.orders.ID] = TopicObservation{Partitions: 3}
	snapshot.Observations.Topics[graph.unrelated.ID] = TopicObservation{Partitions: 1}
	snapshot.Sort()
	graph.snapshot = snapshot

	return graph
}

func TestExtractChain(t *testing.T) {
	chain := newChainGraph()

	extracted := ExtractChain(chain.snapshot, []NodeID{chain.orders.ID}, "stand-a")

	t.Run("nodes", func(t *testing.T) {
		want := nodeSet(chain.orders, chain.invoices, chain.billingGroup, chain.mailerGroup, chain.auditGroup,
			chain.billingA, chain.billingB, chain.mailer, chain.audit, chain.laptop, chain.database)
		if got := nodeSet(extracted.Snapshot.Nodes...); !reflect.DeepEqual(got, want) {
			t.Errorf("nodes = %v, want %v", got, want)
		}
	})
	t.Run("workload of another environment is not expanded", func(t *testing.T) {
		for _, edge := range extracted.Snapshot.Edges {
			if edge.From == chain.billingB.ID {
				t.Errorf("edge %s → %s of stand-b is in the chain", edge.From, edge.To)
			}
		}
	})
	t.Run("gaps", func(t *testing.T) {
		want := []NodeID{chain.mailer.ID}
		if !reflect.DeepEqual(extracted.Gaps, want) {
			t.Errorf("gaps = %v, want %v", extracted.Gaps, want)
		}
	})
	t.Run("observations of chain nodes only", func(t *testing.T) {
		_, hasOrders := extracted.Snapshot.Observations.Topics[chain.orders.ID]
		_, hasUnrelated := extracted.Snapshot.Observations.Topics[chain.unrelated.ID]
		if !hasOrders || hasUnrelated {
			t.Errorf("topic observations = %v, want orders only", extracted.Snapshot.Observations.Topics)
		}
	})
}

func TestExtractChainWithoutEnvironmentExpandsEveryWorkload(t *testing.T) {
	chain := newChainGraph()

	extracted := ExtractChain(chain.snapshot, []NodeID{chain.orders.ID}, "")

	if !nodeSet(extracted.Snapshot.Nodes...)[chain.payments.ID] {
		t.Error("payments, produced by billing of stand-b, is not in the chain")
	}
}

func TestExtractChainFromTheMiddle(t *testing.T) {
	chain := newChainGraph()

	extracted := ExtractChain(chain.snapshot, []NodeID{chain.mailerGroup.ID}, "stand-a")

	want := nodeSet(chain.mailerGroup, chain.mailer)
	if got := nodeSet(extracted.Snapshot.Nodes...); !reflect.DeepEqual(got, want) {
		t.Errorf("nodes = %v, want %v: the walk follows the direction of the edges", got, want)
	}
}

func TestExtractConnected(t *testing.T) {
	chain := newChainGraph()

	extracted := ExtractConnected(chain.snapshot, []NodeID{chain.mailer.ID}, "stand-a")

	nodes := nodeSet(extracted.Snapshot.Nodes...)
	if !nodes[chain.orders.ID] || !nodes[chain.billingB.ID] {
		t.Errorf("nodes = %v, want the upstream topic orders and the member of stand-b", nodes)
	}
	if nodes[chain.payments.ID] || nodes[chain.unrelated.ID] {
		t.Errorf("nodes = %v, want no payments (behind stand-b) and no unrelated topic", nodes)
	}
}

func TestSelectorMatches(t *testing.T) {
	billingA := workloadIn("stand-a", "billing")
	tests := []struct {
		name        string
		selector    Selector
		node        Node
		environment string
		want        bool
	}{
		{name: "topic", selector: Selector{Kind: NodeTopic, Name: "orders"}, node: NewTopic("main", "orders"), want: true},
		{name: "other name", selector: Selector{Kind: NodeTopic, Name: "orders"}, node: NewTopic("main", "invoices")},
		{name: "other kind", selector: Selector{Kind: NodeGroup, Name: "orders"}, node: NewTopic("main", "orders")},
		{name: "source", selector: Selector{Kind: NodeTopic, Name: "orders", Source: "main"}, node: NewTopic("main", "orders"), want: true},
		{name: "other source", selector: Selector{Kind: NodeTopic, Name: "orders", Source: "other"}, node: NewTopic("main", "orders")},
		{
			name: "workload in environment", selector: Selector{Kind: NodeWorkload, Name: "billing"},
			node: billingA, environment: "stand-a", want: true,
		},
		{name: "workload in other environment", selector: Selector{Kind: NodeWorkload, Name: "billing"}, node: billingA, environment: "stand-b"},
		{name: "workload, no environment", selector: Selector{Kind: NodeWorkload, Name: "billing"}, node: billingA, want: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.selector.Matches(test.node, test.environment); got != test.want {
				t.Errorf("Matches = %v, want %v", got, test.want)
			}
		})
	}
}

func nodeSet(nodes ...Node) map[NodeID]bool {
	set := make(map[NodeID]bool, len(nodes))
	for _, node := range nodes {
		set[node.ID] = true
	}

	return set
}
