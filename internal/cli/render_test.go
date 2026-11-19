package cli

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"

	"github.com/charmbracelet/colorprofile"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

var update = flag.Bool("update", false, "rewrite the golden files in testdata")

// healthyReport is a run where every source answered and nothing is wrong.
func healthyReport() *report.Report {
	snapshot := graph.Merge(graph.Snapshot{
		Nodes: []graph.Node{graph.NewTopic("main", "orders.created"), graph.NewGroup("main", "billing")},
		Edges: []graph.Edge{{
			From:       graph.TopicID("main", "orders.created"),
			To:         graph.GroupID("main", "billing"),
			Kind:       graph.EdgeConsumer,
			Confidence: graph.ConfidenceObserved,
			Evidence:   []graph.Evidence{{Source: "main", Detail: "assigned partitions 0,1,2"}},
		}},
	})
	result := report.New("",
		[]report.SourceResult{{Name: "main", Type: "kafka", Status: report.StatusOK, DurationMs: 120}},
		[]report.Chain{report.NewChain("all", snapshot)},
		nil)
	return &result
}

// failingReport has an unavailable source and findings of both severities, added out of order.
func failingReport() *report.Report {
	healthy := healthyReport()
	sources := append(healthy.Sources, report.SourceResult{
		Name: "orders-db", Type: "postgres", Status: report.StatusUnknown, DurationMs: 10000,
		Error: "context deadline exceeded",
	})
	findings := []report.Finding{
		{
			Rule:     "empty-subscription",
			Severity: report.SeverityWarning,
			Subjects: []graph.NodeID{graph.GroupID("main", "billing"), graph.TopicID("main", "orders.created")},
			Message:  "subscribed to a topic that has never received a record",
		},
		{
			Rule:     "stalled-consumer",
			Severity: report.SeverityError,
			Subjects: []graph.NodeID{graph.GroupID("main", "billing")},
			Message:  "lag 4 and no active members\nlast state: Empty",
			Evidence: []graph.Evidence{{Source: "main", Detail: "committed offsets on partitions 0,1"}},
		},
	}
	result := report.New("", sources, healthy.Chains, findings)

	return &result
}

func TestWriteText(t *testing.T) {
	tests := map[string]*report.Report{
		"healthy": healthyReport(),
		"failing": failingReport(),
	}
	for name, result := range tests {
		t.Run(name, func(t *testing.T) {
			var output bytes.Buffer
			// The NoTTY profile strips colors, as for output redirected to a file.
			err := writeText(&colorprofile.Writer{Forward: &output, Profile: colorprofile.NoTTY}, result)
			if err != nil {
				t.Fatal(err)
			}
			assertGolden(t, name+".txt", output.Bytes())
		})
	}
}

func TestWriteJSON(t *testing.T) {
	var output bytes.Buffer
	err := writeJSON(&output, failingReport())
	if err != nil {
		t.Fatal(err)
	}
	assertGolden(t, "failing.json", output.Bytes())
}

// assertGolden compares output with testdata/<name>. Run `go test ./internal/cli -update`
// to rewrite the file after an intended change, then review the diff.
func assertGolden(t *testing.T, name string, output []byte) {
	t.Helper()
	path := filepath.Join("testdata", name)
	if *update {
		err := os.WriteFile(path, output, 0o600)
		if err != nil {
			t.Fatal(err)
		}
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read golden file: %v (run with -update to create it)", err)
	}
	if !bytes.Equal(output, want) {
		t.Errorf("output differs from %s:\n--- got ---\n%s\n--- want ---\n%s", path, output, want)
	}
}
