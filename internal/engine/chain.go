package engine

import (
	"fmt"
	"maps"
	"slices"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/rule"
)

// chainAll is the implicit chain when the config names no chains (§8.1).
const chainAll = "all"

// ruleChainEntryNotFound is the finding of a chain entry that matched no node. It is not a rule
// of §9.1 and cannot be configured: a chain that cannot be found cannot be green.
const ruleChainEntryNotFound = "chain-entry-not-found"

// extractChains builds every configured chain, or the implicit one. It also returns a finding for
// every entry that matched nothing. Those are only reported when every source answered: otherwise
// the node may exist in the source that is missing, and the chain is unknown anyway.
func extractChains(
	definitions map[string]config.ChainConfig,
	snapshot graph.Snapshot,
	environment string,
	complete bool,
) ([]report.Chain, []report.Finding) {
	if len(definitions) == 0 {
		return implicitChain(snapshot, environment, complete)
	}
	var chains []report.Chain
	var findings []report.Finding
	for _, name := range slices.Sorted(maps.Keys(definitions)) {
		var entries []graph.NodeID
		for _, entry := range definitions[name].From {
			selector := rule.SelectorOf(entry)
			matched := matchingNodes(snapshot, selector, environment)
			if len(matched) == 0 && complete {
				message := fmt.Sprintf("entry %s matched no node", selector)
				if selector.Kind == graph.NodeWorkload && environment != "" {
					message += " in environment " + environment
				}
				findings = append(findings, entryNotFound(name, message))
			}
			entries = append(entries, matched...)
		}
		chains = append(chains, report.NewChain(name, graph.ExtractChain(snapshot, entries, environment)))
	}

	return chains, findings
}

// implicitChain is everything connected to the workloads of the environment. Without any workloads
// (no Kubernetes source) it is the whole graph, so a Kafka-only check still shows everything.
func implicitChain(snapshot graph.Snapshot, environment string, complete bool) ([]report.Chain, []report.Finding) {
	var workloads []graph.NodeID
	anyWorkload := false
	for _, node := range snapshot.Nodes {
		anyWorkload = anyWorkload || node.Kind == graph.NodeWorkload
		if graph.InEnvironment(node, environment) {
			workloads = append(workloads, node.ID)
		}
	}
	if !anyWorkload {
		return []report.Chain{report.NewChain(chainAll, graph.Subgraph{Snapshot: snapshot})}, nil
	}
	chain := report.NewChain(chainAll, graph.ExtractConnected(snapshot, workloads, environment))
	if len(workloads) > 0 || !complete {
		return []report.Chain{chain}, nil
	}

	return []report.Chain{chain}, []report.Finding{entryNotFound(chainAll, "no workload in environment "+environment)}
}

func entryNotFound(chain, message string) report.Finding {
	return report.Finding{Rule: ruleChainEntryNotFound, Severity: report.SeverityError, Chain: chain, Message: message}
}

func matchingNodes(snapshot graph.Snapshot, selector graph.Selector, environment string) []graph.NodeID {
	var matched []graph.NodeID
	for _, node := range snapshot.Nodes {
		if selector.Matches(node, environment) {
			matched = append(matched, node.ID)
		}
	}

	return matched
}

// chainsOnlyInFindings returns an empty chain for every chain a finding is about that is not among
// the chains, e.g. a chain that is only in the lockfile. The report then shows the finding under
// its chain, and the finding counts in the status.
func chainsOnlyInFindings(findings []report.Finding, chains []report.Chain) []report.Chain {
	var missing []report.Chain
	for _, finding := range findings {
		known := func(chain report.Chain) bool { return chain.Name == finding.Chain }
		if finding.Chain == "" || slices.ContainsFunc(chains, known) || slices.ContainsFunc(missing, known) {
			continue
		}
		missing = append(missing, report.NewChain(finding.Chain, graph.Subgraph{Snapshot: graph.NewSnapshot()}))
	}

	return missing
}

// findingsInChains keeps the findings about the chains: a finding with a subject in at least one
// chain, or about a chain itself. Problems elsewhere in shared infrastructure, e.g. a stalled group
// of another environment, must not fail the check of this one.
func findingsInChains(findings []report.Finding, chains []report.Chain) []report.Finding {
	inChains := make(map[graph.NodeID]bool)
	for _, chain := range chains {
		for _, node := range chain.Nodes {
			inChains[node.ID] = true
		}
	}
	var result []report.Finding
	for _, finding := range findings {
		if finding.Chain != "" || slices.ContainsFunc(finding.Subjects, func(subject graph.NodeID) bool { return inChains[subject] }) {
			result = append(result, finding)
		}
	}

	return result
}
