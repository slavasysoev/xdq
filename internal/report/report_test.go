package report

import (
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
)

func TestWorst(t *testing.T) {
	tests := []struct {
		name     string
		statuses []Status
		want     Status
	}{
		{name: "none", statuses: nil, want: StatusOK},
		{name: "ok", statuses: []Status{StatusOK, StatusOK}, want: StatusOK},
		{name: "unknown over ok", statuses: []Status{StatusOK, StatusUnknown}, want: StatusUnknown},
		{name: "warning over unknown", statuses: []Status{StatusUnknown, StatusWarning}, want: StatusWarning},
		{name: "error over all", statuses: []Status{StatusWarning, StatusError, StatusUnknown}, want: StatusError},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := Worst(test.statuses...)
			if got != test.want {
				t.Errorf("Worst(%v) = %s, want %s", test.statuses, got, test.want)
			}
		})
	}
}

func TestIsCompleteAndHasErrors(t *testing.T) {
	available := SourceResult{Name: "main", Status: StatusOK}
	unavailable := SourceResult{Name: "db", Status: StatusUnknown}
	tests := []struct {
		name         string
		report       Report
		wantComplete bool
		wantErrors   bool
	}{
		{name: "clean", report: Report{Sources: []SourceResult{available}}, wantComplete: true},
		{
			name:         "warnings only",
			report:       Report{Sources: []SourceResult{available}, Findings: []Finding{{Severity: SeverityWarning}}},
			wantComplete: true,
		},
		{
			name:         "error finding",
			report:       Report{Sources: []SourceResult{available}, Findings: []Finding{{Severity: SeverityError}}},
			wantComplete: true,
			wantErrors:   true,
		},
		{name: "unavailable source", report: Report{Sources: []SourceResult{available, unavailable}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if got := test.report.IsComplete(); got != test.wantComplete {
				t.Errorf("IsComplete() = %t, want %t", got, test.wantComplete)
			}
			if got := test.report.HasErrors(); got != test.wantErrors {
				t.Errorf("HasErrors() = %t, want %t", got, test.wantErrors)
			}
		})
	}
}

func TestNew(t *testing.T) {
	topic := graph.NewTopic("main", "orders")
	group := graph.NewGroup("main", "billing")
	database := graph.NewDatabase("db", "orders")
	subscription := graph.Edge{
		From: topic.ID, Kind: graph.EdgeConsumer, To: group.ID,
		Evidence: []graph.Evidence{{Source: "main"}},
	}
	snapshot := graph.Snapshot{Nodes: []graph.Node{topic, group, database}, Edges: []graph.Edge{subscription}}

	t.Run("worst finding on a node sets node, chain and report status", func(t *testing.T) {
		report := New("",
			[]SourceResult{{Name: "main", Status: StatusOK}, {Name: "db", Status: StatusOK}},
			[]Chain{NewChain("all", snapshot)},
			[]Finding{
				{Severity: SeverityWarning, Subjects: []graph.NodeID{group.ID}},
				{Severity: SeverityError, Subjects: []graph.NodeID{group.ID}},
			})

		chain := report.Chains[0]
		if chain.Nodes[1].ID != group.ID || chain.Nodes[1].Status != StatusError {
			t.Errorf("group status = %s, want error", chain.Nodes[1].Status)
		}
		if chain.Nodes[0].Status != StatusOK {
			t.Errorf("topic status = %s, want ok", chain.Nodes[0].Status)
		}
		if chain.Status != StatusError || report.Status != StatusError {
			t.Errorf("chain = %s, report = %s, want error", chain.Status, report.Status)
		}
	})

	t.Run("unavailable source makes its items and every chain unknown", func(t *testing.T) {
		report := New("",
			[]SourceResult{{Name: "main", Status: StatusUnknown}, {Name: "db", Status: StatusOK}},
			[]Chain{NewChain("all", snapshot)},
			nil)

		chain := report.Chains[0]
		wantNodes := map[graph.NodeID]Status{topic.ID: StatusUnknown, group.ID: StatusUnknown, database.ID: StatusOK}
		for _, node := range chain.Nodes {
			if node.Status != wantNodes[node.ID] {
				t.Errorf("status of %s = %s, want %s", node.ID, node.Status, wantNodes[node.ID])
			}
		}
		if chain.Edges[0].Status != StatusUnknown {
			t.Errorf("edge status = %s, want unknown", chain.Edges[0].Status)
		}
		if report.Status != StatusUnknown {
			t.Errorf("report status = %s, want unknown", report.Status)
		}
	})

	t.Run("empty chain of a failed run is unknown", func(t *testing.T) {
		report := New("",
			[]SourceResult{{Name: "main", Status: StatusUnknown}},
			[]Chain{NewChain("all", graph.Snapshot{})},
			nil)

		if report.Status != StatusUnknown {
			t.Errorf("report status = %s, want unknown", report.Status)
		}
	})

	t.Run("no findings is an empty list, not null", func(t *testing.T) {
		report := New("", nil, nil, nil)

		if report.Findings == nil {
			t.Error("findings = nil, want an empty list")
		}
	})
}
