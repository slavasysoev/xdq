package engine

import (
	"errors"
	"testing"
	"time"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/lock"
	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/rule"
	"github.com/slavasysoev/xdq/internal/source"
)

// consumerSnapshot is a Kafka snapshot where the group billing consumes the topic.
func consumerSnapshot(topic string) graph.Snapshot {
	topicNode := graph.NewTopic("main", topic)
	group := graph.NewGroup("main", "billing")
	snapshot := graph.NewSnapshot()
	snapshot.Nodes = []graph.Node{topicNode, group}
	snapshot.Edges = []graph.Edge{{From: topicNode.ID, To: group.ID, Kind: graph.EdgeConsumer, Evidence: []graph.Evidence{{Source: "main"}}}}
	snapshot.Observations.Topics[topicNode.ID] = graph.TopicObservation{Partitions: 1, EndOffsets: map[int32]int64{0: 5}}

	return snapshot
}

func runWithLock(t *testing.T, snapshot graph.Snapshot, discoverErr error, locked *lock.File) report.Report {
	t.Helper()
	rules, err := rule.NewSet(nil)
	if err != nil {
		t.Fatal(err)
	}
	sources := []source.Source{returning("main", config.TypeKafka, snapshot, discoverErr)}

	// Without configured chains and workloads the implicit chain is the whole graph, so the renamed
	// topic is in the chain.
	return New(sources, rules, nil, locked, time.Second).Run(t.Context(), "")
}

func TestDriftRenamedTopic(t *testing.T) {
	healthy := runWithLock(t, consumerSnapshot("orders.created"), nil, nil)
	locked := lock.New("", healthy.Chains)
	locked.Chains["retired"] = lock.Chain{}

	result := runWithLock(t, consumerSnapshot("orders.creatd"), nil, &locked)

	rules := make(map[string]report.Finding)
	for _, finding := range result.Findings {
		rules[finding.Rule] = finding
	}
	wantMessage := "locked consumer edge is no longer discovered\n" +
		"possibly renamed or mistyped: topic:main/orders.created -> topic:main/orders.creatd"
	if rules[lock.RuleEdgeRemoved].Message != wantMessage {
		t.Errorf("findings = %+v, want %s with %q", result.Findings, lock.RuleEdgeRemoved, wantMessage)
	}
	if rules[lock.RuleEdgeAdded].Rule == "" || rules[lock.RuleChainNotConfigured].Rule == "" {
		t.Errorf("findings = %+v, want also %s and %s", result.Findings, lock.RuleEdgeAdded, lock.RuleChainNotConfigured)
	}
	if len(result.Chains) != 2 || result.Chains[1].Name != "retired" || result.Chains[1].Status != report.StatusWarning {
		t.Errorf("chains = %+v, want all and the warning chain retired", result.Chains)
	}
	if result.Status != report.StatusError {
		t.Errorf("status = %s, want error", result.Status)
	}
}

func TestDriftSkippedWhenIncomplete(t *testing.T) {
	locked := lock.New("", []report.Chain{report.NewChain("all", graph.Subgraph{Snapshot: consumerSnapshot("orders.created")})})

	result := runWithLock(t, graph.Snapshot{}, errors.New("connection refused"), &locked)

	if len(result.Findings) != 0 {
		t.Errorf("findings = %+v, want none: a missing source is not a drift", result.Findings)
	}
}
