package engine

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/rule"
	"github.com/slavasysoev/xdq/internal/source"
)

// fakeSource is a named source that returns what its discover function returns.
type fakeSource struct {
	name       string
	sourceType string
	discover   func(ctx context.Context) (graph.Snapshot, error)
}

func (fake fakeSource) Name() string {
	return fake.name
}

func (fake fakeSource) Type() string {
	return fake.sourceType
}

func (fake fakeSource) Discover(ctx context.Context) (graph.Snapshot, error) {
	return fake.discover(ctx)
}

func (fake fakeSource) Close() error {
	return nil
}

func returning(name, sourceType string, snapshot graph.Snapshot, err error) fakeSource {
	return fakeSource{
		name:       name,
		sourceType: sourceType,
		discover:   func(context.Context) (graph.Snapshot, error) { return snapshot, err },
	}
}

var (
	kafkaSnapshot = graph.Snapshot{
		Nodes: []graph.Node{graph.NewTopic("main", "orders"), graph.NewEndpoint("10.0.0.1")},
	}
	postgresSnapshot = graph.Snapshot{
		Nodes: []graph.Node{graph.NewDatabase("db", "orders"), graph.NewEndpoint("10.0.0.1")},
	}
)

func TestRunAllSourcesAvailable(t *testing.T) {
	engine := New([]source.Source{
		returning("main", "kafka", kafkaSnapshot, nil),
		returning("db", "postgres", postgresSnapshot, nil),
	}, &rule.Set{}, time.Second)

	result := engine.Run(t.Context(), "stand-a")

	if result.Status != report.StatusOK || !result.IsComplete() || result.HasErrors() {
		t.Errorf("status = %s, complete = %t, errors = %t, want ok, complete, no errors",
			result.Status, result.IsComplete(), result.HasErrors())
	}
	if result.SchemaVersion != report.SchemaVersion || result.Environment != "stand-a" {
		t.Errorf("schema = %d, environment = %q", result.SchemaVersion, result.Environment)
	}
	if len(result.Chains) != 1 || result.Chains[0].Name != chainAll {
		t.Fatalf("chains = %+v, want the implicit chain %q", result.Chains, chainAll)
	}
	if len(result.Chains[0].Nodes) != 3 {
		t.Errorf("nodes = %d, want 3: the shared endpoint is merged", len(result.Chains[0].Nodes))
	}
	for _, sourceResult := range result.Sources {
		if sourceResult.Status != report.StatusOK || sourceResult.Error != "" {
			t.Errorf("source %s = %+v, want ok", sourceResult.Name, sourceResult)
		}
	}
}

func TestRunOneSourceFails(t *testing.T) {
	engine := New([]source.Source{
		returning("main", "kafka", kafkaSnapshot, nil),
		returning("db", "postgres", postgresSnapshot, errors.New("connection refused")),
	}, &rule.Set{}, time.Second)

	result := engine.Run(t.Context(), "")

	if result.Status != report.StatusUnknown || result.IsComplete() {
		t.Errorf("status = %s, complete = %t, want unknown, incomplete", result.Status, result.IsComplete())
	}
	failed := result.Sources[1]
	if failed.Name != "db" || failed.Type != "postgres" || failed.Status != report.StatusUnknown || failed.Error != "connection refused" {
		t.Errorf("failed source = %+v, want db unknown with the error", failed)
	}
	if result.Sources[0].Status != report.StatusOK {
		t.Errorf("available source = %+v, want ok", result.Sources[0])
	}
	if len(result.Chains[0].Nodes) != 2 {
		t.Errorf("nodes = %d, want 2 from the available source", len(result.Chains[0].Nodes))
	}
}

func TestRunSourceTimeout(t *testing.T) {
	hanging := fakeSource{name: "main", sourceType: "kafka", discover: func(ctx context.Context) (graph.Snapshot, error) {
		<-ctx.Done()

		return graph.Snapshot{}, ctx.Err()
	}}
	engine := New([]source.Source{hanging}, &rule.Set{}, 10*time.Millisecond)

	result := engine.Run(t.Context(), "")

	sourceResult := result.Sources[0]
	if sourceResult.Status != report.StatusUnknown || !strings.Contains(sourceResult.Error, "deadline exceeded") {
		t.Errorf("source = %+v, want unknown with deadline exceeded", sourceResult)
	}
}

// TestRunInParallel fails when sources run one after another: each source waits until all
// have started, so a sequential run makes the first one time out.
func TestRunInParallel(t *testing.T) {
	var started sync.WaitGroup
	started.Add(2)
	allStarted := make(chan struct{})
	go func() {
		started.Wait()
		close(allStarted)
	}()
	discover := func(ctx context.Context) (graph.Snapshot, error) {
		started.Done()
		select {
		case <-allStarted:
			return graph.Snapshot{}, nil
		case <-ctx.Done():
			return graph.Snapshot{}, ctx.Err()
		}
	}
	engine := New([]source.Source{
		fakeSource{name: "first", sourceType: "kafka", discover: discover},
		fakeSource{name: "second", sourceType: "kafka", discover: discover},
	}, &rule.Set{}, 5*time.Second)

	result := engine.Run(t.Context(), "")

	for _, sourceResult := range result.Sources {
		if sourceResult.Status != report.StatusOK {
			t.Errorf("source %s = %+v, want ok: sources did not run in parallel", sourceResult.Name, sourceResult)
		}
	}
}

func TestRunCanRepeat(t *testing.T) {
	engine := New([]source.Source{returning("main", "kafka", kafkaSnapshot, nil)}, &rule.Set{}, time.Second)

	first := engine.Run(t.Context(), "")
	second := engine.Run(t.Context(), "")

	if len(first.Chains[0].Nodes) != len(second.Chains[0].Nodes) {
		t.Error("a second run gives a different graph")
	}
}

func TestRunChecksRules(t *testing.T) {
	stalled := graph.NewSnapshot()
	stalled.Nodes = []graph.Node{graph.NewTopic("main", "orders"), graph.NewGroup("main", "billing")}
	stalled.Observations.Groups[graph.GroupID("main", "billing")] = graph.GroupObservation{
		State: "Empty",
		Lag:   map[graph.NodeID]int64{graph.TopicID("main", "orders"): 4},
	}
	rules, err := rule.NewSet(nil)
	if err != nil {
		t.Fatal(err)
	}
	engine := New([]source.Source{returning("main", "kafka", stalled, nil)}, rules, time.Second)

	result := engine.Run(t.Context(), "")

	if !result.HasErrors() || result.Findings[0].Rule != "stalled-consumer" {
		t.Fatalf("findings = %+v, want stalled-consumer", result.Findings)
	}
	if result.Status != report.StatusError {
		t.Errorf("status = %s, want error", result.Status)
	}
}
