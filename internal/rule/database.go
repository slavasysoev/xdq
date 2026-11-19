package rule

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// checkDatabaseSpansEnvironments reports databases with client workloads from more than one
// environment: a service of one environment points to the database of another one, e.g. after
// a config change. Unresolved clients are left out: unresolved-client reports them.
func checkDatabaseSpansEnvironments(input Input, settings settings) []report.Finding {
	nodes := nodesByID(input.Graph)
	clients := make(map[graph.NodeID]map[string][]graph.Edge)
	for _, edge := range input.Graph.Edges {
		if edge.Kind != graph.EdgeConnection || nodes[edge.From].Kind != graph.NodeWorkload {
			continue
		}
		if clients[edge.To] == nil {
			clients[edge.To] = make(map[string][]graph.Edge)
		}
		environment := nodes[edge.From].Environment
		clients[edge.To][environment] = append(clients[edge.To][environment], edge)
	}

	var findings []report.Finding
	for _, database := range slices.Sorted(maps.Keys(clients)) {
		byEnvironment := clients[database]
		if len(byEnvironment) < 2 {
			continue
		}
		environments := slices.Sorted(maps.Keys(byEnvironment))
		findingSubjects := []graph.NodeID{database}
		var evidence []graph.Evidence
		for _, environment := range environments {
			// Edges are sorted by From, so the clients of one environment come sorted by ID.
			for _, edge := range byEnvironment[environment] {
				findingSubjects = append(findingSubjects, edge.From)
				evidence = append(evidence, clientEvidence(nodes, edge))
			}
		}
		message := fmt.Sprintf("clients from %d environments: %s", len(environments), strings.Join(displayEnvironments(environments), ", "))
		findings = append(findings, settings.finding(report.SeverityError, findingSubjects, message, evidence))
	}

	return findings
}

// clientEvidence tells the environment of a client workload and what the database source said
// about its sessions, e.g. "stand-b  deployment/billing  2 sessions as user billing".
func clientEvidence(nodes map[graph.NodeID]graph.Node, connection graph.Edge) graph.Evidence {
	client := nodes[connection.From]
	var sessions []string
	for _, evidence := range connection.Evidence {
		if evidence.Source == nodes[connection.To].Source {
			sessions = append(sessions, evidence.Detail)
		}
	}
	detail := fmt.Sprintf("%s  %s/%s  %s", displayEnvironment(client.Environment), client.WorkloadKind, client.Name,
		strings.Join(sessions, "; "))

	return graph.Evidence{Source: client.Source, Detail: detail}
}
