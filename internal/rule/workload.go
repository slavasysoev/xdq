package rule

import (
	"fmt"
	"maps"
	"slices"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// checkWorkloadNotReady reports workloads that have pods but none of them is ready. Fewer ready
// pods than pods is normal during a rollout, and the ready pods keep working.
func checkWorkloadNotReady(input Input, settings settings) []report.Finding {
	nodes := nodesByID(input.Graph)
	workloads := input.Graph.Observations.Workloads
	var findings []report.Finding
	for _, workload := range slices.Sorted(maps.Keys(workloads)) {
		observation := workloads[workload]
		if observation.Pods == 0 || observation.ReadyPods > 0 {
			continue
		}
		var evidence []graph.Evidence
		for _, problem := range observation.NotReady {
			evidence = append(evidence, graph.Evidence{
				Source: nodes[workload].Source,
				Detail: fmt.Sprintf("pod %s: %s, %d restarts", problem.Pod, problem.Reason, problem.Restarts),
			})
		}
		message := fmt.Sprintf("no ready pod out of %d", observation.Pods)
		findings = append(findings, settings.finding(report.SeverityError, []graph.NodeID{workload}, message, evidence))
	}

	return findings
}
