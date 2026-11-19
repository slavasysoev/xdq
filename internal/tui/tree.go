package tui

import (
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// treeLine is one node of a chain drawn as a tree, in the direction data flows.
type treeLine struct {
	// prefix draws the branches before the node, e.g. "│ └ ".
	prefix string
	// edge is the kind of the edge from the parent; empty for a root.
	edge graph.EdgeKind
	node report.ChainNode
	// repeated marks a node that is already shown above: a second parent or a cycle. Its children
	// are not repeated.
	repeated bool
}

// treeBuilder walks the chain depth-first and collects the lines.
type treeBuilder struct {
	nodes    map[graph.NodeID]report.ChainNode
	outgoing map[graph.NodeID][]report.ChainEdge
	visited  map[graph.NodeID]bool
	lines    []treeLine
}

// buildTree draws the chain as a tree from its roots: the nodes without incoming edges. A graph
// is not a tree, so a node reachable twice is shown once in full and then as repeated.
func buildTree(chain report.Chain) []treeLine {
	builder := treeBuilder{
		nodes:    make(map[graph.NodeID]report.ChainNode, len(chain.Nodes)),
		outgoing: make(map[graph.NodeID][]report.ChainEdge),
		visited:  make(map[graph.NodeID]bool, len(chain.Nodes)),
	}
	incoming := make(map[graph.NodeID]bool)
	for _, node := range chain.Nodes {
		builder.nodes[node.ID] = node
	}
	// Edges are sorted by From, Kind, To, so the children come in a stable order.
	for _, edge := range chain.Edges {
		builder.outgoing[edge.From] = append(builder.outgoing[edge.From], edge)
		incoming[edge.To] = true
	}
	for _, node := range chain.Nodes {
		if !incoming[node.ID] {
			builder.walk(node.ID, "", "", "")
		}
	}
	// A cycle that no root leads to has no node without incoming edges.
	for _, node := range chain.Nodes {
		if !builder.visited[node.ID] {
			builder.walk(node.ID, "", "", "")
		}
	}

	return builder.lines
}

// walk adds the node and, the first time it is seen, its children. linePrefix is drawn before
// the node itself, childPrefix before the lines of its children.
func (builder *treeBuilder) walk(nodeID graph.NodeID, edge graph.EdgeKind, linePrefix, childPrefix string) {
	repeated := builder.visited[nodeID]
	builder.visited[nodeID] = true
	builder.lines = append(builder.lines, treeLine{prefix: linePrefix, edge: edge, node: builder.nodes[nodeID], repeated: repeated})
	if repeated {
		return
	}
	children := builder.outgoing[nodeID]
	for index, child := range children {
		if index == len(children)-1 {
			builder.walk(child.To, child.Kind, childPrefix+"└ ", childPrefix+"  ")
		} else {
			builder.walk(child.To, child.Kind, childPrefix+"├ ", childPrefix+"│ ")
		}
	}
}
