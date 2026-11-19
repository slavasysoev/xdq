package lock

import (
	"errors"
	"io/fs"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

var (
	orders  = graph.TopicID("main", "orders.created")
	billing = graph.GroupID("main", "billing")
	pod     = graph.WorkloadID("test", "stand-a", graph.WorkloadDeployment, "billing")
)

// chainOf makes a report chain of the edges. Only the edges matter to the lockfile.
func chainOf(name string, edges ...graph.Edge) report.Chain {
	return report.NewChain(name, graph.Subgraph{Snapshot: graph.Snapshot{Edges: edges}})
}

func edge(from graph.NodeID, kind graph.EdgeKind, to graph.NodeID) graph.Edge {
	return graph.Edge{
		From: from, Kind: kind, To: to,
		Confidence: graph.ConfidenceObserved,
		Evidence:   []graph.Evidence{{Source: "main", Detail: "volatile detail"}},
	}
}

func TestEncode(t *testing.T) {
	consumer := edge(orders, graph.EdgeConsumer, billing)
	membership := edge(billing, graph.EdgeMembership, pod)
	unresolved := edge(billing, graph.EdgeMembership, graph.EndpointID("10.0.0.9"))
	first := New("stand-a", []report.Chain{chainOf("order-flow", consumer, membership, unresolved), chainOf("audit")})
	second := New("stand-a", []report.Chain{chainOf("audit"), chainOf("order-flow", unresolved, membership, consumer)})

	want := header + `version: 1
environment: stand-a
chains:
  audit:
    edges: []
  order-flow:
    edges:
      - {from: group:main/billing, kind: membership, to: workload:test/stand-a/deployment/billing}
      - {from: topic:main/orders.created, kind: consumer, to: group:main/billing}
`
	if got := string(first.Encode()); got != want {
		t.Errorf("Encode() =\n%s\nwant\n%s", got, want)
	}
	if string(first.Encode()) != string(second.Encode()) {
		t.Errorf("the same chains in another order give another file:\n%s\n%s", first.Encode(), second.Encode())
	}
}

func TestEncodeParseRoundTrip(t *testing.T) {
	odd := graph.GroupID("main", `odd: group #1, "quoted"`)
	file := New("stand a", []report.Chain{
		chainOf("true", edge(orders, graph.EdgeConsumer, odd)),
		chainOf("order-flow", edge(orders, graph.EdgeConsumer, billing)),
	})

	parsed, err := parse(file.Encode())
	if err != nil {
		t.Fatalf("parse: %v\n%s", err, file.Encode())
	}
	if !reflect.DeepEqual(parsed, file) {
		t.Errorf("parsed =\n%+v\nwant\n%+v\ntext:\n%s", parsed, file, file.Encode())
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name      string
		text      string
		wantError string
	}{
		{name: "empty", text: "", wantError: "lockfile is empty"},
		{name: "unknown field", text: "version: 1\nchains: {}\nreplicas: 3\n", wantError: "replicas"},
		{name: "unsupported version", text: "version: 2\nchains: {}\n", wantError: "unsupported version 2"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := parse([]byte(test.text))
			if err == nil || !strings.Contains(err.Error(), test.wantError) {
				t.Errorf("error = %v, want it to contain %q", err, test.wantError)
			}
		})
	}
}

func TestWriteRead(t *testing.T) {
	path := filepath.Join(t.TempDir(), DefaultName)
	file := New("stand-a", []report.Chain{chainOf("order-flow", edge(orders, graph.EdgeConsumer, billing))})

	err := Write(path, file)
	if err != nil {
		t.Fatal(err)
	}
	read, err := Read(path)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(read, file) {
		t.Errorf("read = %+v, want %+v", read, file)
	}

	_, err = Read(filepath.Join(t.TempDir(), "missing.lock"))
	if !errors.Is(err, fs.ErrNotExist) {
		t.Errorf("error = %v, want fs.ErrNotExist", err)
	}
}
