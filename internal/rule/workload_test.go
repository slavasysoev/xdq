package rule

import (
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

func TestWorkloadNotReady(t *testing.T) {
	deployment := graph.NewWorkload("test", "stand-a", graph.WorkloadDeployment, "billing")
	crashing := graph.WorkloadObservation{
		Pods: 2,
		NotReady: []graph.PodProblem{
			{Pod: "billing-a", Reason: "CrashLoopBackOff", Restarts: 7},
			{Pod: "billing-b", Reason: "NotReady"},
		},
	}
	rollout := graph.WorkloadObservation{
		Pods: 2, ReadyPods: 1,
		NotReady: []graph.PodProblem{{Pod: "billing-b", Reason: "NotReady"}},
	}

	tests := []struct {
		name        string
		observation graph.WorkloadObservation
		want        int
	}{
		{name: "no ready pod", observation: crashing, want: 1},
		{name: "some pods ready", observation: rollout},
		{name: "all pods ready", observation: graph.WorkloadObservation{Pods: 2, ReadyPods: 2}},
		{name: "no pods", observation: graph.WorkloadObservation{}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := graph.NewSnapshot()
			snapshot.Nodes = []graph.Node{deployment}
			snapshot.Observations.Workloads[deployment.ID] = test.observation

			findings := checkWorkloadNotReady(Input{Graph: snapshot, Now: now}, settings{name: "workload-not-ready"})

			if len(findings) != test.want {
				t.Fatalf("findings = %+v, want %d", findings, test.want)
			}
			if test.want == 0 {
				return
			}
			finding := findings[0]
			if finding.Severity != report.SeverityError || finding.Subjects[0] != deployment.ID {
				t.Errorf("finding = %s %v, want error on %s", finding.Severity, finding.Subjects, deployment.ID)
			}
			if finding.Message != "no ready pod out of 2" {
				t.Errorf("message = %q", finding.Message)
			}
			if len(finding.Evidence) != 2 || finding.Evidence[0].Detail != "pod billing-a: CrashLoopBackOff, 7 restarts" {
				t.Errorf("evidence = %+v", finding.Evidence)
			}
		})
	}
}
