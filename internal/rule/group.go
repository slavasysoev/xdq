package rule

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// noEnvironment is shown for workloads whose environment could not be derived.
const noEnvironment = "(none)"

// groupMembers is a group with its member workloads, by environment. Unresolved endpoints are left
// out: unresolved-client reports them.
type groupMembers struct {
	group         graph.NodeID
	byEnvironment map[string][]graph.Node
}

// checkGroupSpansEnvironments reports groups with members from more than one environment: another
// environment takes part of the partitions, and its messages "disappear". This is the motivating
// bug of xdq.
func checkGroupSpansEnvironments(input Input, settings settings) []report.Finding {
	nodes := nodesByID(input.Graph)
	var findings []report.Finding
	for _, members := range membersOfGroups(input.Graph, nodes) {
		if len(members.byEnvironment) < 2 {
			continue
		}
		environments := slices.Sorted(maps.Keys(members.byEnvironment))
		var workloads []graph.Node
		for _, environment := range environments {
			workloads = append(workloads, members.byEnvironment[environment]...)
		}
		message := fmt.Sprintf("members from %d environments: %s", len(environments), strings.Join(displayEnvironments(environments), ", "))
		findings = append(findings, settings.finding(report.SeverityError, subjects(members.group, workloads), message,
			memberEvidence(input.Graph, nodes, members.group, workloads)))
	}

	return findings
}

// checkGroupMultipleWorkloads reports groups with members from more than one workload of the same
// environment. It is a warning: a canary deployment is a legitimate case.
func checkGroupMultipleWorkloads(input Input, settings settings) []report.Finding {
	nodes := nodesByID(input.Graph)
	var findings []report.Finding
	for _, members := range membersOfGroups(input.Graph, nodes) {
		for _, environment := range slices.Sorted(maps.Keys(members.byEnvironment)) {
			workloads := members.byEnvironment[environment]
			if len(workloads) < 2 {
				continue
			}
			message := fmt.Sprintf("members from %d workloads in environment %s", len(workloads), displayEnvironment(environment))
			findings = append(findings, settings.finding(report.SeverityWarning, subjects(members.group, workloads), message,
				memberEvidence(input.Graph, nodes, members.group, workloads)))
		}
	}

	return findings
}

// membersOfGroups returns every group that has member workloads, sorted by group. The workloads
// of each environment are sorted by ID.
func membersOfGroups(snapshot graph.Snapshot, nodes map[graph.NodeID]graph.Node) []groupMembers {
	byGroup := make(map[graph.NodeID]map[string][]graph.Node)
	for _, edge := range snapshot.Edges {
		member := nodes[edge.To]
		if edge.Kind != graph.EdgeMembership || member.Kind != graph.NodeWorkload {
			continue
		}
		if byGroup[edge.From] == nil {
			byGroup[edge.From] = make(map[string][]graph.Node)
		}
		byGroup[edge.From][member.Environment] = append(byGroup[edge.From][member.Environment], member)
	}
	var result []groupMembers
	for _, group := range slices.Sorted(maps.Keys(byGroup)) {
		// Edges are sorted by From, Kind, To, so the workloads of one group come sorted by ID.
		result = append(result, groupMembers{group: group, byEnvironment: byGroup[group]})
	}

	return result
}

func subjects(group graph.NodeID, workloads []graph.Node) []graph.NodeID {
	result := []graph.NodeID{group}
	for _, workload := range workloads {
		result = append(result, workload.ID)
	}

	return result
}

// memberEvidence tells, for every workload, its environment and which partitions its members
// are assigned, e.g. "stand-a  deployment/billing  orders.created 0,1,2".
func memberEvidence(snapshot graph.Snapshot, nodes map[graph.NodeID]graph.Node, group graph.NodeID, workloads []graph.Node) []graph.Evidence {
	members := snapshot.Observations.Groups[group].Members
	evidence := make([]graph.Evidence, 0, len(workloads))
	for _, workload := range workloads {
		assigned := make(map[graph.NodeID][]int32)
		for _, member := range members {
			if member.Endpoint != workload.ID {
				continue
			}
			for topic, partitions := range member.Assignment {
				assigned[topic] = append(assigned[topic], partitions...)
			}
		}
		detail := fmt.Sprintf("%s  %s/%s  %s", displayEnvironment(workload.Environment), workload.WorkloadKind, workload.Name,
			formatAssignment(assigned, nodes))
		evidence = append(evidence, graph.Evidence{Source: workload.Source, Detail: detail})
	}

	return evidence
}

// formatAssignment prints the partitions of every topic as "orders.created 0,1,2; payments 3".
func formatAssignment(assigned map[graph.NodeID][]int32, nodes map[graph.NodeID]graph.Node) string {
	if len(assigned) == 0 {
		return "no partitions assigned"
	}
	var topics []string
	for _, topic := range slices.Sorted(maps.Keys(assigned)) {
		partitions := slices.Sorted(slices.Values(assigned[topic]))
		texts := make([]string, len(partitions))
		for index, partition := range partitions {
			texts[index] = strconv.Itoa(int(partition))
		}
		topics = append(topics, nodes[topic].Name+" "+strings.Join(texts, ","))
	}

	return strings.Join(topics, "; ")
}

func displayEnvironment(environment string) string {
	if environment == "" {
		return noEnvironment
	}

	return environment
}

func displayEnvironments(environments []string) []string {
	result := make([]string, len(environments))
	for index, environment := range environments {
		result[index] = displayEnvironment(environment)
	}

	return result
}
