package rule

import (
	"fmt"
	"slices"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// checkUnresolvedClient reports clients known only by address: resolution found no workload for
// them, e.g. a developer laptop consuming from a test cluster. A group member is an error, because
// it takes partitions away from the real consumers; a database client is a warning.
func checkUnresolvedClient(input Input, settings settings) []report.Finding {
	if !identitiesDiscovered(input.Sources) {
		return nil
	}
	nodes := nodesByID(input.Graph)
	var findings []report.Finding
	for _, edge := range input.Graph.Edges {
		switch {
		case edge.Kind == graph.EdgeMembership && nodes[edge.To].Kind == graph.NodeEndpoint:
			message := fmt.Sprintf("member at %s is not a known workload", nodes[edge.To].Name)
			findings = append(findings, settings.finding(report.SeverityError, []graph.NodeID{edge.From, edge.To}, message, edge.Evidence))
		case edge.Kind == graph.EdgeConnection && nodes[edge.From].Kind == graph.NodeEndpoint:
			message := fmt.Sprintf("client at %s is not a known workload", nodes[edge.From].Name)
			findings = append(findings, settings.finding(report.SeverityWarning, []graph.NodeID{edge.To, edge.From}, message, edge.Evidence))
		}
	}

	return findings
}

// identitiesDiscovered tells whether endpoints could be resolved at all: at least one Kubernetes
// source is configured and every one answered. Otherwise every endpoint is unresolved, and
// reporting each of them would only repeat that identities are missing.
func identitiesDiscovered(sources []report.SourceResult) bool {
	isKubernetes := func(result report.SourceResult) bool { return result.Type == config.TypeKubernetes }
	anyKubernetes := slices.ContainsFunc(sources, isKubernetes)
	failedKubernetes := slices.ContainsFunc(sources, func(result report.SourceResult) bool {
		return isKubernetes(result) && result.Status != report.StatusOK
	})

	return anyKubernetes && !failedKubernetes
}
