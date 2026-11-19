package rule

import (
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// referenceGraph has the topics with records orders.created, payments and logs, the empty
// auto-created orders.creatd, and the workload billing whose configuration is value.
func referenceGraph(value string) graph.Snapshot {
	snapshot := graph.NewSnapshot()
	billingWorkload := workloadIn("stand-a", "billing")
	snapshot.Nodes = []graph.Node{billingWorkload}
	for name, endOffset := range map[string]int64{"orders.created": 10, "orders.creatd": 0, "payments": 5, "logs": 5, "_schemas": 5} {
		topic := graph.NewTopic("main", name)
		snapshot.Nodes = append(snapshot.Nodes, topic)
		snapshot.Observations.Topics[topic.ID] = graph.TopicObservation{Partitions: 1, EndOffsets: map[int32]int64{0: endOffset}}
	}
	snapshot.ConfigValues = []graph.ConfigValue{{Workload: billingWorkload.ID, Origin: "env TOPIC", Value: value}}
	snapshot.Sort()

	return snapshot
}

func TestNearMissTopic(t *testing.T) {
	billingWorkload := workloadIn("stand-a", "billing").ID
	ordersCreated := graph.TopicID("main", "orders.created")
	typo := graph.TopicID("main", "orders.creatd")
	tests := []struct {
		name         string
		value        string
		wantSubjects [][]graph.NodeID
		wantMessage  string
	}{
		{name: "exact topic in use", value: "orders.created"},
		{
			name:         "auto-created typo topic",
			value:        "orders.creatd",
			wantSubjects: [][]graph.NodeID{{billingWorkload, ordersCreated, typo}},
			wantMessage:  "config names orders.creatd, a topic that has never received a record; did you mean orders.created?",
		},
		{
			name:         "typo without a topic, two edits on a long name",
			value:        "orders.crated.",
			wantSubjects: [][]graph.NodeID{{billingWorkload, ordersCreated}},
			wantMessage:  "config names orders.crated., which is not a topic; did you mean orders.created?",
		},
		{name: "two edits on a short name", value: "paymnet"},
		{
			name:         "one edit on a short name",
			value:        "payment",
			wantSubjects: [][]graph.NodeID{{billingWorkload, graph.TopicID("main", "payments")}},
		},
		{name: "short topics are not suggested", value: "log"},
		{name: "internal topics are not suggested", value: "_schema"},
		{name: "far from every topic", value: "spring"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			findings := checkNearMissTopic(Input{Graph: referenceGraph(test.value)}, settings{name: "near-miss-topic"})

			assertSubjects(t, findings, report.SeverityWarning, test.wantSubjects)
			if test.wantMessage != "" && len(findings) == 1 && findings[0].Message != test.wantMessage {
				t.Errorf("message = %q, want %q", findings[0].Message, test.wantMessage)
			}
		})
	}
}

func TestEditDistance(t *testing.T) {
	tests := []struct {
		left, right string
		limit, want int
	}{
		{left: "orders.created", right: "orders.created", limit: 2, want: 0},
		{left: "orders.creatd", right: "orders.created", limit: 2, want: 1},
		{left: "orders-created", right: "orders.created", limit: 2, want: 1},
		{left: "ordres.created", right: "orders.created", limit: 2, want: 2},
		{left: "orders", right: "invoices", limit: 2, want: 3},
		{left: "a", right: "abcd", limit: 2, want: 3},
		{left: "", right: "ab", limit: 2, want: 2},
	}
	for _, test := range tests {
		got := editDistance(test.left, test.right, test.limit)
		if got != test.want {
			t.Errorf("editDistance(%q, %q, %d) = %d, want %d", test.left, test.right, test.limit, got, test.want)
		}
	}
}
