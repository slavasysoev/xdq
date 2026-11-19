// Package lock reads and writes the lockfile, the structure of the chains in a known-good state,
// and compares the current chains with it (§9.2, §10).
package lock

import (
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// Version is the lockfile format version.
const Version = 1

// DefaultName is the file name of the lockfile next to the config.
const DefaultName = "xdq.lock"

// File is a lockfile. It holds only stable identities: no evidence, confidence or observations.
type File struct {
	Version     int              `yaml:"version"`
	Environment string           `yaml:"environment"`
	Chains      map[string]Chain `yaml:"chains"`
}

// Chain is the locked structure of one chain.
type Chain struct {
	Edges []Edge `yaml:"edges"`
}

// Edge is a locked edge: which nodes are connected and how.
type Edge struct {
	From graph.NodeID   `yaml:"from"`
	Kind graph.EdgeKind `yaml:"kind"`
	To   graph.NodeID   `yaml:"to"`
}

// New captures the chains. Edges to and from endpoints are left out: an endpoint ID is an
// address, which changes with every restart of a pod. Unresolved clients are a rule's business.
func New(environment string, chains []report.Chain) File {
	file := File{Version: Version, Environment: environment, Chains: make(map[string]Chain, len(chains))}
	for _, chain := range chains {
		var edges []Edge
		for _, edge := range chain.Edges {
			if edge.From.Kind() == graph.NodeEndpoint || edge.To.Kind() == graph.NodeEndpoint {
				continue
			}
			edges = append(edges, Edge{From: edge.From, Kind: edge.Kind, To: edge.To})
		}
		file.Chains[chain.Name] = Chain{Edges: edges}
	}

	return file
}
