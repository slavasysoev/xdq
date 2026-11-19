package report

import (
	"slices"

	"github.com/slavasysoev/xdq/internal/graph"
)

// SchemaVersion is the version of the Report JSON schema, the contract for CI and the UIs.
const SchemaVersion = 1

// Report is the result of one check run.
type Report struct {
	SchemaVersion int            `json:"schemaVersion"`
	Environment   string         `json:"environment,omitempty"`
	Status        Status         `json:"status"`
	Sources       []SourceResult `json:"sources"`
	Chains        []Chain        `json:"chains"`
	Findings      []Finding      `json:"findings"`
}

// SourceResult tells whether a source was discovered and how long it took.
type SourceResult struct {
	Name       string `json:"name"`
	Type       string `json:"type"`
	Status     Status `json:"status"`
	DurationMs int64  `json:"durationMs"`
	Error      string `json:"error,omitempty"`
}

// Chain is a business process: the nodes and edges reachable from its entry points.
type Chain struct {
	Name         string             `json:"name"`
	Status       Status             `json:"status"`
	Nodes        []ChainNode        `json:"nodes"`
	Edges        []ChainEdge        `json:"edges"`
	Observations graph.Observations `json:"observations"`
}

// ChainNode is a node with its status. The embedded graph.Node fields are flattened into
// the same JSON object.
type ChainNode struct {
	graph.Node

	Status Status `json:"status"`
	// Gap marks a workload where the walk stopped: nothing was discovered downstream. It is
	// information and does not change the status, because most chains end at a workload.
	Gap bool `json:"gap,omitempty"`
}

// ChainEdge is an edge with its status.
type ChainEdge struct {
	graph.Edge

	Status Status `json:"status"`
}

// Finding is a violated rule or a drift from the lockfile.
type Finding struct {
	Rule     string         `json:"rule"`
	Severity Severity       `json:"severity"`
	Subjects []graph.NodeID `json:"subjects"`
	// Chain is set when the finding is about a chain itself rather than its nodes, e.g. an entry
	// that matched no node. The finding then sets the status of that chain.
	Chain    string           `json:"chain,omitempty"`
	Message  string           `json:"message"`
	Evidence []graph.Evidence `json:"evidence,omitempty"`
}

// NewChain makes a chain of the nodes and edges of the subgraph, every status ok.
func NewChain(name string, subgraph graph.Subgraph) Chain {
	snapshot := subgraph.Snapshot
	chain := Chain{
		Name:         name,
		Status:       StatusOK,
		Nodes:        make([]ChainNode, 0, len(snapshot.Nodes)),
		Edges:        make([]ChainEdge, 0, len(snapshot.Edges)),
		Observations: snapshot.Observations,
	}
	for _, node := range snapshot.Nodes {
		chain.Nodes = append(chain.Nodes, ChainNode{Node: node, Status: StatusOK, Gap: slices.Contains(subgraph.Gaps, node.ID)})
	}
	for _, edge := range snapshot.Edges {
		chain.Edges = append(chain.Edges, ChainEdge{Edge: edge, Status: StatusOK})
	}

	return chain
}

// New builds a report and sets the statuses of nodes, edges, chains and the report itself from
// the findings and the source results (§9.3). Chains are taken over and updated in place.
//
// An unavailable source makes every chain at least unknown: part of the graph is not visible,
// so no chain can be confirmed green.
func New(environment string, sources []SourceResult, chains []Chain, findings []Finding) Report {
	if findings == nil {
		// JSON shows an empty list, not null.
		findings = []Finding{}
	}
	report := Report{
		SchemaVersion: SchemaVersion,
		Environment:   environment,
		Sources:       sources,
		Chains:        chains,
		Findings:      findings,
	}
	report.assignStatuses()

	return report
}

func (report *Report) assignStatuses() {
	unavailable := make(map[string]bool)
	for _, result := range report.Sources {
		if result.Status != StatusOK {
			unavailable[result.Name] = true
		}
	}
	findingStatus := make(map[graph.NodeID]Status)
	chainFindingStatus := make(map[string]Status)
	for _, finding := range report.Findings {
		for _, subject := range finding.Subjects {
			findingStatus[subject] = Worst(findingStatus[subject], finding.Severity.Status())
		}
		if finding.Chain != "" {
			chainFindingStatus[finding.Chain] = Worst(chainFindingStatus[finding.Chain], finding.Severity.Status())
		}
	}

	var chainStatuses []Status
	for chainIndex := range report.Chains {
		chain := &report.Chains[chainIndex]
		statuses := []Status{chainFindingStatus[chain.Name]}
		if len(unavailable) > 0 {
			statuses = append(statuses, StatusUnknown)
		}
		for nodeIndex := range chain.Nodes {
			node := &chain.Nodes[nodeIndex]
			node.Status = Worst(StatusOK, findingStatus[node.ID])
			if unavailable[node.Source] {
				node.Status = Worst(node.Status, StatusUnknown)
			}
			statuses = append(statuses, node.Status)
		}
		for edgeIndex := range chain.Edges {
			edge := &chain.Edges[edgeIndex]
			edge.Status = StatusOK
			if slices.ContainsFunc(edge.Evidence, func(evidence graph.Evidence) bool { return unavailable[evidence.Source] }) {
				edge.Status = StatusUnknown
			}
			statuses = append(statuses, edge.Status)
		}
		chain.Status = Worst(statuses...)
		chainStatuses = append(chainStatuses, chain.Status)
	}
	report.Status = Worst(chainStatuses...)
}

// IsComplete tells whether every source was discovered. An incomplete report cannot be trusted,
// even when it has no error findings.
func (report *Report) IsComplete() bool {
	return !slices.ContainsFunc(report.Sources, func(result SourceResult) bool { return result.Status != StatusOK })
}

// HasErrors tells whether at least one finding has error severity.
func (report *Report) HasErrors() bool {
	return slices.ContainsFunc(report.Findings, func(finding Finding) bool { return finding.Severity == SeverityError })
}
