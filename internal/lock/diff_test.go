package lock

import (
	"reflect"
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

func TestDiff(t *testing.T) {
	typo := graph.TopicID("main", "orders.creatd")
	invoices := graph.TopicID("main", "invoices.created")
	locked := New("stand-a", []report.Chain{
		chainOf("order-flow", edge(orders, graph.EdgeConsumer, billing), edge(billing, graph.EdgeMembership, pod)),
		chainOf("retired"),
	})

	tests := []struct {
		name   string
		chains []report.Chain
		want   []report.Finding
	}{
		{
			name: "unchanged, endpoints ignored",
			chains: []report.Chain{chainOf("order-flow",
				edge(orders, graph.EdgeConsumer, billing),
				edge(billing, graph.EdgeMembership, pod),
				edge(billing, graph.EdgeMembership, graph.EndpointID("10.0.0.9")),
			), chainOf("retired")},
		},
		{
			name: "renamed topic",
			chains: []report.Chain{chainOf("order-flow",
				edge(typo, graph.EdgeConsumer, billing),
				edge(billing, graph.EdgeMembership, pod),
			), chainOf("retired")},
			want: []report.Finding{
				{
					Rule: RuleEdgeRemoved, Severity: report.SeverityError, Subjects: []graph.NodeID{orders, billing}, Chain: "order-flow",
					Message: "locked consumer edge is no longer discovered\npossibly renamed or mistyped: topic:main/orders.created -> topic:main/orders.creatd",
				},
				{
					Rule: RuleEdgeAdded, Severity: report.SeverityWarning, Subjects: []graph.NodeID{typo, billing}, Chain: "order-flow",
					Message: "consumer edge is not in the lockfile",
				},
			},
		},
		{
			name: "added edge, removed edge without a similar node",
			chains: []report.Chain{chainOf("order-flow",
				edge(orders, graph.EdgeConsumer, billing),
				edge(pod, graph.EdgeProducer, invoices),
			), chainOf("retired")},
			want: []report.Finding{
				{
					Rule: RuleEdgeRemoved, Severity: report.SeverityError, Subjects: []graph.NodeID{billing, pod}, Chain: "order-flow",
					Message: "locked membership edge is no longer discovered",
				},
				{
					Rule: RuleEdgeAdded, Severity: report.SeverityWarning, Subjects: []graph.NodeID{pod, invoices}, Chain: "order-flow",
					Message: "producer edge is not in the lockfile",
				},
			},
		},
		{
			name: "chain not locked, locked chain not configured",
			chains: []report.Chain{
				chainOf("order-flow", edge(orders, graph.EdgeConsumer, billing), edge(billing, graph.EdgeMembership, pod)),
				chainOf("new-flow"),
			},
			want: []report.Finding{
				{
					Rule: RuleChainNotLocked, Severity: report.SeverityWarning, Chain: "new-flow",
					Message: "chain is not in the lockfile; run xdq lock to add it",
				},
				{
					Rule: RuleChainNotConfigured, Severity: report.SeverityWarning, Chain: "retired",
					Message: "chain is in the lockfile but not in the config; run xdq lock to remove it",
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Diff(locked, test.chains)
			if !reflect.DeepEqual(got, test.want) {
				t.Errorf("Diff() =\n%+v\nwant\n%+v", got, test.want)
			}
		})
	}
}

func TestDiffRemovedConnectionIsWarning(t *testing.T) {
	database := graph.DatabaseID("orders-db", "orders")
	locked := New("stand-a", []report.Chain{chainOf("order-flow", edge(pod, graph.EdgeConnection, database))})

	got := Diff(locked, []report.Chain{chainOf("order-flow")})

	want := []report.Finding{{
		Rule: RuleEdgeRemoved, Severity: report.SeverityWarning, Subjects: []graph.NodeID{pod, database}, Chain: "order-flow",
		Message: "locked connection edge is no longer discovered; the client may be idle or not running now",
	}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Diff() =\n%+v\nwant\n%+v", got, want)
	}
}
