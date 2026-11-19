package engine

import (
	"reflect"
	"slices"
	"testing"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

func chainSnapshot() graph.Snapshot {
	billing := graph.NewWorkload("test", "stand-a", graph.WorkloadDeployment, "billing")
	billing.Environment = "stand-a"
	snapshot := graph.NewSnapshot()
	snapshot.Nodes = []graph.Node{
		graph.NewTopic("main", "orders"), graph.NewGroup("main", "billing"), billing,
		graph.NewTopic("main", "other"), graph.NewGroup("main", "other"),
	}
	snapshot.Edges = []graph.Edge{
		{From: graph.TopicID("main", "orders"), To: graph.GroupID("main", "billing"), Kind: graph.EdgeConsumer},
		{From: graph.GroupID("main", "billing"), To: billing.ID, Kind: graph.EdgeMembership},
		{From: graph.TopicID("main", "other"), To: graph.GroupID("main", "other"), Kind: graph.EdgeConsumer},
	}
	snapshot.Sort()

	return snapshot
}

func TestExtractChains(t *testing.T) {
	definitions := map[string]config.ChainConfig{
		"order-flow": {From: []config.SelectorConfig{{Topic: "orders"}, {Workload: "missing"}}},
	}

	t.Run("entry that matched nothing", func(t *testing.T) {
		chains, findings := extractChains(definitions, chainSnapshot(), "stand-a", true)

		if len(chains) != 1 || len(chains[0].Nodes) != 3 {
			t.Fatalf("chains = %+v, want order-flow with 3 nodes", chains)
		}
		want := []report.Finding{{
			Rule:     ruleChainEntryNotFound,
			Severity: report.SeverityError,
			Chain:    "order-flow",
			Message:  "entry workload missing matched no node in environment stand-a",
		}}
		if !reflect.DeepEqual(findings, want) {
			t.Errorf("findings = %+v, want %+v", findings, want)
		}
	})
	t.Run("incomplete run reports no missing entries", func(t *testing.T) {
		_, findings := extractChains(definitions, chainSnapshot(), "stand-a", false)

		if len(findings) != 0 {
			t.Errorf("findings = %+v, want none: the entry may be in the missing source", findings)
		}
	})
}

func TestImplicitChain(t *testing.T) {
	t.Run("connected to the workloads of the environment", func(t *testing.T) {
		chains, findings := extractChains(nil, chainSnapshot(), "stand-a", true)

		if len(chains[0].Nodes) != 3 || len(findings) != 0 {
			t.Errorf("chain = %+v, findings = %+v, want orders, billing group and workload", chains[0].Nodes, findings)
		}
	})
	t.Run("unknown environment", func(t *testing.T) {
		chains, findings := extractChains(nil, chainSnapshot(), "stand-x", true)

		if len(chains[0].Nodes) != 0 || len(findings) != 1 || findings[0].Message != "no workload in environment stand-x" {
			t.Errorf("chain = %+v, findings = %+v, want an empty chain and a finding", chains[0].Nodes, findings)
		}
	})
	t.Run("no workloads at all", func(t *testing.T) {
		kafkaOnly := chainSnapshot()
		kafkaOnly.Nodes = slices.DeleteFunc(kafkaOnly.Nodes, func(node graph.Node) bool { return node.Kind == graph.NodeWorkload })

		chains, _ := extractChains(nil, kafkaOnly, "stand-a", true)

		if len(chains[0].Nodes) != len(kafkaOnly.Nodes) {
			t.Errorf("chain has %d nodes, want the whole graph of %d", len(chains[0].Nodes), len(kafkaOnly.Nodes))
		}
	})
}

func TestFindingsInChains(t *testing.T) {
	chains, _ := extractChains(nil, chainSnapshot(), "stand-a", true)
	inChain := report.Finding{Rule: "stalled-consumer", Subjects: []graph.NodeID{graph.GroupID("main", "billing")}}
	elsewhere := report.Finding{Rule: "stalled-consumer", Subjects: []graph.NodeID{graph.GroupID("main", "other")}}
	aboutChain := report.Finding{Rule: ruleChainEntryNotFound, Chain: "all"}

	got := findingsInChains([]report.Finding{inChain, elsewhere, aboutChain}, chains)

	want := []report.Finding{inChain, aboutChain}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("findings = %+v, want %+v", got, want)
	}
}
