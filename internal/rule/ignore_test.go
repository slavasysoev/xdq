package rule

import (
	"testing"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

func TestSetIgnoresFindings(t *testing.T) {
	member := scenario{
		extraNodes: []graph.Node{graph.NewEndpoint("192.168.1.20")},
		extraEdges: []graph.Edge{{From: billing, To: laptop, Kind: graph.EdgeMembership}},
	}
	sources := []report.SourceResult{{Name: "test", Type: config.TypeKubernetes, Status: report.StatusOK}}
	ignoreGroup := func(name string) config.IgnoreConfig {
		return config.IgnoreConfig{SelectorConfig: config.SelectorConfig{Group: name}}
	}
	tests := []struct {
		name        string
		ignore      []config.IgnoreConfig
		wantIgnored bool
	}{
		{name: "no exceptions"},
		{name: "group matches a subject", ignore: []config.IgnoreConfig{ignoreGroup("billing")}, wantIgnored: true},
		{name: "other group", ignore: []config.IgnoreConfig{ignoreGroup("shipping")}},
		{name: "group of another source", ignore: []config.IgnoreConfig{{SelectorConfig: config.SelectorConfig{Group: "billing", Source: "other"}}}},
		{name: "address", ignore: []config.IgnoreConfig{{Endpoint: "192.168.1.20"}}, wantIgnored: true},
		{name: "other address", ignore: []config.IgnoreConfig{{Endpoint: "192.168.1.21"}}},
		{name: "range containing the address", ignore: []config.IgnoreConfig{{Endpoint: "192.168.0.0/16"}}, wantIgnored: true},
		{name: "range without the address", ignore: []config.IgnoreConfig{{Endpoint: "10.0.0.0/8"}}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			set, err := NewSet(map[string]config.RuleConfig{"unresolved-client": {Ignore: test.ignore}})
			if err != nil {
				t.Fatal(err)
			}

			var got []report.Finding
			for _, finding := range set.Check(Input{Graph: member.snapshot(), Sources: sources, Now: now}) {
				if finding.Rule == "unresolved-client" {
					got = append(got, finding)
				}
			}

			if len(got) != 1 || got[0].Ignored != test.wantIgnored {
				t.Errorf("findings = %+v, want one with ignored = %t", got, test.wantIgnored)
			}
		})
	}
}

func TestIgnoreWorkloadInEveryEnvironment(t *testing.T) {
	workload := graph.NewWorkload("test", "stand-b", graph.WorkloadDeployment, "billing")
	nodes := map[graph.NodeID]graph.Node{workload.ID: workload}
	findings := []report.Finding{{Subjects: []graph.NodeID{workload.ID}}}

	markIgnored(findings, []exception{newException(config.IgnoreConfig{SelectorConfig: config.SelectorConfig{Workload: "billing"}})}, nodes)

	if !findings[0].Ignored {
		t.Error("a workload selector must match in every environment")
	}
}
