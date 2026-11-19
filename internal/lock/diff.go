package lock

import (
	"fmt"
	"maps"
	"slices"

	"github.com/slavasysoev/xdq/internal/distance"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// Drift findings. They are not rules of §9.1 and cannot be configured: the lockfile is the
// configuration of what is expected.
const (
	RuleEdgeRemoved        = "drift-edge-removed"
	RuleEdgeAdded          = "drift-edge-added"
	RuleChainNotConfigured = "drift-chain-not-configured"
	RuleChainNotLocked     = "drift-chain-not-locked"
)

// renameDistance is the largest edit distance between the IDs of a removed and an added node
// that is reported as a possible rename.
const renameDistance = 2

// Diff compares the chains with the locked ones (§9.2). Every finding names its chain, because
// the nodes of a removed edge may no longer be in the chain. Locked chains that are not among the
// chains are reported as no longer configured: the caller leaves out the chains it does not check.
func Diff(locked File, chains []report.Chain) []report.Finding {
	current := New(locked.Environment, chains)
	var findings []report.Finding
	for _, chain := range chains {
		lockedChain, found := locked.Chains[chain.Name]
		if !found {
			findings = append(findings, chainFinding(RuleChainNotLocked, chain.Name, "chain is not in the lockfile; run xdq lock to add it"))

			continue
		}
		findings = append(findings, diffChain(chain.Name, lockedChain.Edges, current.Chains[chain.Name].Edges)...)
	}
	for _, name := range slices.Sorted(maps.Keys(locked.Chains)) {
		if _, found := current.Chains[name]; !found {
			findings = append(findings, chainFinding(RuleChainNotConfigured, name, "chain is in the lockfile but not in the config; run xdq lock to remove it"))
		}
	}

	return findings
}

func diffChain(chainName string, locked, current []Edge) []report.Finding {
	renames := possibleRenames(locked, current)
	var findings []report.Finding
	for _, edge := range difference(locked, current) {
		severity := report.SeverityError
		message := fmt.Sprintf("locked %s edge is no longer discovered", edge.Kind)
		// A connection is seen only while the client holds a session. Its absence proves nothing:
		// the pool may have closed idle sessions, or a CronJob is not running now.
		if edge.Kind == graph.EdgeConnection {
			severity = report.SeverityWarning
			message += "; the client may be idle or not running now"
		}
		for _, node := range []graph.NodeID{edge.From, edge.To} {
			for _, renamed := range renames[node] {
				message += fmt.Sprintf("\npossibly renamed or mistyped: %s -> %s", node, renamed)
			}
		}
		findings = append(findings, edgeFinding(severity, RuleEdgeRemoved, chainName, edge, message))
	}
	for _, edge := range difference(current, locked) {
		message := fmt.Sprintf("%s edge is not in the lockfile", edge.Kind)
		findings = append(findings, edgeFinding(report.SeverityWarning, RuleEdgeAdded, chainName, edge, message))
	}

	return findings
}

// difference returns the sorted edges that are not in other.
func difference(edges, other []Edge) []Edge {
	var result []Edge
	for _, edge := range sortedEdges(edges) {
		if !slices.Contains(other, edge) {
			result = append(result, edge)
		}
	}

	return result
}

// possibleRenames maps every node that is only in the locked edges to the nodes of the same kind
// that are only in the current edges and within renameDistance.
func possibleRenames(locked, current []Edge) map[graph.NodeID][]graph.NodeID {
	lockedNodes := nodesOf(locked)
	currentNodes := nodesOf(current)
	renames := make(map[graph.NodeID][]graph.NodeID)
	for _, removed := range lockedNodes {
		if slices.Contains(currentNodes, removed) {
			continue
		}
		for _, added := range currentNodes {
			if slices.Contains(lockedNodes, added) || added.Kind() != removed.Kind() {
				continue
			}
			if distance.Levenshtein(string(removed), string(added), renameDistance) <= renameDistance {
				renames[removed] = append(renames[removed], added)
			}
		}
	}

	return renames
}

// nodesOf returns the sorted, distinct nodes of the edges.
func nodesOf(edges []Edge) []graph.NodeID {
	var nodes []graph.NodeID
	for _, edge := range edges {
		nodes = append(nodes, edge.From, edge.To)
	}
	slices.Sort(nodes)

	return slices.Compact(nodes)
}

func edgeFinding(severity report.Severity, rule, chainName string, edge Edge, message string) report.Finding {
	return report.Finding{
		Rule:     rule,
		Severity: severity,
		Subjects: []graph.NodeID{edge.From, edge.To},
		Chain:    chainName,
		Message:  message,
	}
}

func chainFinding(rule, chainName, message string) report.Finding {
	return report.Finding{Rule: rule, Severity: report.SeverityWarning, Chain: chainName, Message: message}
}
