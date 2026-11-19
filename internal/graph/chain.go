package graph

// Selector matches the entry nodes of a chain by kind and name (§8.1). It names no source-specific
// IDs, so one chain definition works in every environment.
type Selector struct {
	Kind NodeKind
	Name string
	// Source restricts the selector to one source when not empty.
	Source string
}

// String describes the selector for messages, e.g. "topic orders.created".
func (selector Selector) String() string {
	text := string(selector.Kind) + " " + selector.Name
	if selector.Source != "" {
		text += " of source " + selector.Source
	}

	return text
}

// Matches tells whether the selector selects the node. A workload is selected only in the
// environment; an empty environment means every environment.
func (selector Selector) Matches(node Node, environment string) bool {
	return node.Kind == selector.Kind &&
		node.Name == selector.Name &&
		(selector.Source == "" || node.Source == selector.Source) &&
		(node.Kind != NodeWorkload || InEnvironment(node, environment))
}

// InEnvironment tells whether the node is a workload of the environment. An empty environment
// means every environment.
func InEnvironment(node Node, environment string) bool {
	return node.Kind == NodeWorkload && (environment == "" || node.Environment == environment)
}

// Subgraph is the part of a graph that belongs to one chain.
type Subgraph struct {
	// Snapshot holds the nodes and edges of the chain and the observations of its nodes.
	Snapshot Snapshot
	// Gaps are workloads of the environment with no outgoing edges: the walk stopped there.
	// It may be the real end of the chain or a producer edge that was not discovered.
	Gaps []NodeID
}

// ExtractChain walks from the entries in the direction data flows (§8.2).
func ExtractChain(snapshot Snapshot, entries []NodeID, environment string) Subgraph {
	return walk(snapshot, entries, environment, false)
}

// ExtractConnected walks from the entries both along and against the edges: everything connected
// to them. It builds the implicit chain of an environment, whose workloads are in the middle of
// the flow, not at its entry.
func ExtractConnected(snapshot Snapshot, entries []NodeID, environment string) Subgraph {
	return walk(snapshot, entries, environment, true)
}

// walk is a breadth-first walk with a visited set, so cycles end. Nodes that are not expanded
// (workloads of other environments, unresolved endpoints, databases) are included, but the walk
// does not continue from them: a member from another environment is visible in the chain.
func walk(snapshot Snapshot, entries []NodeID, environment string, againstEdges bool) Subgraph {
	nodes := make(map[NodeID]Node, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodes[node.ID] = node
	}
	// Edges are referenced by index: an Edge holds a slice, so it cannot be a map key.
	outgoing := make(map[NodeID][]int)
	incoming := make(map[NodeID][]int)
	for index, edge := range snapshot.Edges {
		outgoing[edge.From] = append(outgoing[edge.From], index)
		incoming[edge.To] = append(incoming[edge.To], index)
	}

	visited := make(map[NodeID]bool)
	includedEdges := make(map[int]bool)
	var queue []NodeID
	visit := func(id NodeID) {
		_, exists := nodes[id]
		if exists && !visited[id] {
			visited[id] = true
			queue = append(queue, id)
		}
	}
	for _, entry := range entries {
		visit(entry)
	}
	for len(queue) > 0 {
		current := queue[0]
		queue = queue[1:]
		if !expands(nodes[current], environment) {
			continue
		}
		for _, index := range outgoing[current] {
			includedEdges[index] = true
			visit(snapshot.Edges[index].To)
		}
		if !againstEdges {
			continue
		}
		for _, index := range incoming[current] {
			includedEdges[index] = true
			visit(snapshot.Edges[index].From)
		}
	}

	return subgraph(snapshot, visited, includedEdges, outgoing, environment)
}

// expands tells whether the walk continues from the node.
func expands(node Node, environment string) bool {
	switch node.Kind {
	case NodeWorkload:
		return InEnvironment(node, environment)
	case NodeEndpoint, NodeDatabase:
		return false
	default:
		return true
	}
}

// subgraph copies the visited part of the snapshot, keeping its order.
func subgraph(snapshot Snapshot, visited map[NodeID]bool, includedEdges map[int]bool, outgoing map[NodeID][]int, environment string) Subgraph {
	result := Subgraph{Snapshot: NewSnapshot()}
	for _, node := range snapshot.Nodes {
		if !visited[node.ID] {
			continue
		}
		result.Snapshot.Nodes = append(result.Snapshot.Nodes, node)
		if InEnvironment(node, environment) && len(outgoing[node.ID]) == 0 {
			result.Gaps = append(result.Gaps, node.ID)
		}
		topic, isTopic := snapshot.Observations.Topics[node.ID]
		if isTopic {
			result.Snapshot.Observations.Topics[node.ID] = topic
		}
		group, isGroup := snapshot.Observations.Groups[node.ID]
		if isGroup {
			result.Snapshot.Observations.Groups[node.ID] = group
		}
	}
	for index, edge := range snapshot.Edges {
		if includedEdges[index] {
			result.Snapshot.Edges = append(result.Snapshot.Edges, edge)
		}
	}

	return result
}
