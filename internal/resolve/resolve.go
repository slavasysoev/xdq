// Package resolve joins the facts of different sources into one graph (§7). Sources never know
// about each other: all cross-system joining happens here.
package resolve

import (
	"maps"
	"net/netip"
	"slices"

	"github.com/slavasysoev/xdq/internal/graph"
)

// Resolve replaces every endpoint whose address belongs to exactly one workload with that
// workload. Endpoints without a match stay as they are. Endpoints with several matches stay
// too, and their edges list the candidates in the evidence. Then it adds the producer edges
// inferred from the workloads' configuration. The input is not modified.
func Resolve(snapshot graph.Snapshot) graph.Snapshot {
	nodes := make(map[graph.NodeID]graph.Node, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodes[node.ID] = node
	}
	matches := endpointMatches(snapshot, nodes)

	resolved := graph.NewSnapshot()
	resolved.Identities = snapshot.Identities
	resolved.ConfigValues = snapshot.ConfigValues
	resolved.Observations.Topics = snapshot.Observations.Topics
	for _, node := range snapshot.Nodes {
		if len(matches[node.ID]) != 1 {
			resolved.Nodes = append(resolved.Nodes, node)
		}
	}
	resolved.Edges = resolveEdges(snapshot.Edges, matches, nodes)
	for id, observation := range snapshot.Observations.Groups {
		resolved.Observations.Groups[id] = resolveMembers(observation, matches)
	}
	// After the endpoints: whether a workload consumes a topic is known only once its group
	// members are resolved to it.
	resolved.Edges = append(resolved.Edges, inferProducers(resolved)...)
	resolved.Sort()

	return resolved
}

// endpointMatches returns the workloads whose address is the address of the endpoint, for every
// endpoint that has at least one match.
func endpointMatches(snapshot graph.Snapshot, nodes map[graph.NodeID]graph.Node) map[graph.NodeID][]graph.NodeID {
	workloads := make(map[string][]graph.NodeID)
	for _, identity := range snapshot.Identities {
		address := normalize(identity.Address)
		if !slices.Contains(workloads[address], identity.Workload) {
			workloads[address] = append(workloads[address], identity.Workload)
		}
	}
	matches := make(map[graph.NodeID][]graph.NodeID)
	for _, node := range nodes {
		if node.Kind != graph.NodeEndpoint {
			continue
		}
		candidates := workloads[normalize(node.Name)]
		if len(candidates) > 0 {
			matches[node.ID] = slices.Sorted(slices.Values(candidates))
		}
	}

	return matches
}

// edgeKey identifies an edge. An Edge itself cannot be a map key: it holds a slice.
type edgeKey struct {
	from, to graph.NodeID
	kind     graph.EdgeKind
}

// resolveEdges points the edges of resolved endpoints to their workloads. Edges that become
// equal, e.g. two pods of one workload in the same group, are merged into one with the evidence
// of both. Each edge gets the evidence of the identity source that resolved it.
func resolveEdges(edges []graph.Edge, matches map[graph.NodeID][]graph.NodeID, nodes map[graph.NodeID]graph.Node) []graph.Edge {
	merged := make(map[edgeKey]graph.Edge)
	for _, edge := range edges {
		evidence := slices.Clone(edge.Evidence)
		edge.From, evidence = resolveEnd(edge.From, evidence, matches, nodes)
		edge.To, evidence = resolveEnd(edge.To, evidence, matches, nodes)
		key := edgeKey{from: edge.From, to: edge.To, kind: edge.Kind}
		existing, seen := merged[key]
		if seen {
			evidence = append(existing.Evidence, evidence...)
		}
		edge.Evidence = evidence
		merged[key] = edge
	}

	// The caller sorts the edges; the evidence keeps the order of the input edges.
	return slices.Collect(maps.Values(merged))
}

// resolveEnd returns the node an edge end points to after resolution, and the evidence with
// what the identity sources said about the address.
func resolveEnd(
	end graph.NodeID,
	evidence []graph.Evidence,
	matches map[graph.NodeID][]graph.NodeID,
	nodes map[graph.NodeID]graph.Node,
) (graph.NodeID, []graph.Evidence) {
	candidates := matches[end]
	for _, candidate := range candidates {
		detail := "address " + nodes[end].Name + " belongs to " + string(candidate)
		if len(candidates) > 1 {
			detail += " (ambiguous: the address belongs to several workloads)"
		}
		evidence = append(evidence, graph.Evidence{Source: nodes[candidate].Source, Detail: detail})
	}
	if len(candidates) == 1 {
		return candidates[0], evidence
	}

	return end, evidence
}

// resolveMembers replaces the endpoint of every member that was resolved. The input
// observation is not modified.
func resolveMembers(observation graph.GroupObservation, matches map[graph.NodeID][]graph.NodeID) graph.GroupObservation {
	observation.Members = slices.Clone(observation.Members)
	for index, member := range observation.Members {
		candidates := matches[member.Endpoint]
		if len(candidates) == 1 {
			observation.Members[index].Endpoint = candidates[0]
		}
	}

	return observation
}

// normalize returns the canonical form of an IP address, so "0:0:0:0:0:0:0:1" as Java reports it
// equals "::1" as Kubernetes reports it, and an IPv4-mapped IPv6 address equals the IPv4 address.
// Anything that is not an IP address is compared as it is.
func normalize(address string) string {
	parsed, err := netip.ParseAddr(address)
	if err != nil {
		return address
	}

	return parsed.Unmap().String()
}
