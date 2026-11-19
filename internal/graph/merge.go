package graph

import (
	"cmp"
	"maps"
	"slices"
)

// Merge combines the snapshots of several sources into one. Nodes with the same ID are kept once:
// an endpoint is reported by every source the address connects to. The output is sorted.
func Merge(snapshots ...Snapshot) Snapshot {
	merged := NewSnapshot()
	seen := make(map[NodeID]bool)
	for _, snapshot := range snapshots {
		for _, node := range snapshot.Nodes {
			if seen[node.ID] {
				continue
			}
			seen[node.ID] = true
			merged.Nodes = append(merged.Nodes, node)
		}
		merged.Edges = append(merged.Edges, snapshot.Edges...)
		merged.Identities = append(merged.Identities, snapshot.Identities...)
		merged.ConfigValues = append(merged.ConfigValues, snapshot.ConfigValues...)
		maps.Copy(merged.Observations.Topics, snapshot.Observations.Topics)
		maps.Copy(merged.Observations.Groups, snapshot.Observations.Groups)
	}

	merged.Sort()

	return merged
}

// Sort orders nodes, edges, identities and config values, so equal content gives equal output.
func (snapshot *Snapshot) Sort() {
	slices.SortFunc(snapshot.Nodes, func(left, right Node) int {
		return cmp.Compare(left.ID, right.ID)
	})
	slices.SortFunc(snapshot.Edges, func(left, right Edge) int {
		return cmp.Or(cmp.Compare(left.From, right.From), cmp.Compare(left.Kind, right.Kind), cmp.Compare(left.To, right.To))
	})
	slices.SortFunc(snapshot.Identities, func(left, right Identity) int {
		return cmp.Or(cmp.Compare(left.Address, right.Address), cmp.Compare(left.Workload, right.Workload))
	})
	slices.SortFunc(snapshot.ConfigValues, func(left, right ConfigValue) int {
		return cmp.Or(cmp.Compare(left.Workload, right.Workload), cmp.Compare(left.Origin, right.Origin))
	})
}
