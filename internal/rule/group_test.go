package rule

import (
	"reflect"
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

func workloadIn(environment, name string) graph.Node {
	workload := graph.NewWorkload("test", environment, graph.WorkloadDeployment, name)
	workload.Environment = environment

	return workload
}

// groupWith is the group billing with one membership edge and one member per workload.
func groupWith(workloads ...graph.Node) graph.Snapshot {
	snapshot := graph.NewSnapshot()
	snapshot.Nodes = []graph.Node{graph.NewGroup("main", "billing"), graph.NewEndpoint("192.168.1.20")}
	var members []graph.MemberObservation
	for _, workload := range workloads {
		snapshot.Nodes = append(snapshot.Nodes, workload)
		snapshot.Edges = append(snapshot.Edges, graph.Edge{From: billing, To: workload.ID, Kind: graph.EdgeMembership})
		members = append(members, graph.MemberObservation{Endpoint: workload.ID, ClientID: workload.Name})
	}
	// An unresolved member is no environment and no workload: unresolved-client reports it.
	snapshot.Edges = append(snapshot.Edges, graph.Edge{From: billing, To: laptop, Kind: graph.EdgeMembership})
	snapshot.Observations.Groups[billing] = graph.GroupObservation{State: "Stable", Members: members}
	snapshot.Sort()

	return snapshot
}

func TestGroupSpansEnvironments(t *testing.T) {
	billingA := workloadIn("stand-a", "billing")
	billingB := workloadIn("stand-b", "billing")
	tests := []struct {
		name         string
		snapshot     graph.Snapshot
		wantSubjects [][]graph.NodeID
	}{
		{name: "one environment", snapshot: groupWith(billingA, workloadIn("stand-a", "billing-canary"))},
		{name: "only unresolved members", snapshot: groupWith()},
		{
			name:         "two environments",
			snapshot:     groupWith(billingA, billingB),
			wantSubjects: [][]graph.NodeID{{billing, billingA.ID, billingB.ID}},
		},
		{
			name:         "workload without environment",
			snapshot:     groupWith(billingA, workloadIn("", "debug")),
			wantSubjects: [][]graph.NodeID{{billing, graph.WorkloadID("test", "", graph.WorkloadDeployment, "debug"), billingA.ID}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			findings := checkGroupSpansEnvironments(Input{Graph: test.snapshot}, settings{name: "group-spans-environments"})

			assertSubjects(t, findings, report.SeverityError, test.wantSubjects)
		})
	}
}

func TestGroupMultipleWorkloads(t *testing.T) {
	billingA := workloadIn("stand-a", "billing")
	canaryA := workloadIn("stand-a", "billing-canary")
	tests := []struct {
		name         string
		snapshot     graph.Snapshot
		wantSubjects [][]graph.NodeID
	}{
		{name: "one workload", snapshot: groupWith(billingA)},
		{name: "one workload per environment", snapshot: groupWith(billingA, workloadIn("stand-b", "billing"))},
		{
			name:         "two workloads in one environment",
			snapshot:     groupWith(billingA, canaryA, workloadIn("stand-b", "billing")),
			wantSubjects: [][]graph.NodeID{{billing, billingA.ID, canaryA.ID}},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			findings := checkGroupMultipleWorkloads(Input{Graph: test.snapshot}, settings{name: "group-multiple-workloads"})

			assertSubjects(t, findings, report.SeverityWarning, test.wantSubjects)
		})
	}
}

func assertSubjects(t *testing.T, findings []report.Finding, severity report.Severity, want [][]graph.NodeID) {
	t.Helper()
	var got [][]graph.NodeID
	for _, finding := range findings {
		got = append(got, finding.Subjects)
		if finding.Severity != severity {
			t.Errorf("severity = %s, want %s", finding.Severity, severity)
		}
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("subjects = %v, want %v", got, want)
	}
}
