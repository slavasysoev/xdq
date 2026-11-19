package resolve

import (
	"reflect"
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
)

var (
	group   = graph.NewGroup("main", "billing")
	billing = graph.NewWorkload("test", "stand-a", graph.WorkloadDeployment, "billing")
	other   = graph.NewWorkload("other", "stand-a", graph.WorkloadDeployment, "billing")
)

func membership(endpoint graph.NodeID, clientID string) graph.Edge {
	return graph.Edge{
		From:       group.ID,
		To:         endpoint,
		Kind:       graph.EdgeMembership,
		Confidence: graph.ConfidenceObserved,
		Evidence:   []graph.Evidence{{Source: "main", Detail: "member client.id=" + clientID}},
	}
}

// snapshotWithMembers is a group whose members connect from the given addresses, with
// identities from the given address to workload mapping.
func snapshotWithMembers(identities []graph.Identity, addresses ...string) graph.Snapshot {
	snapshot := graph.NewSnapshot()
	snapshot.Nodes = []graph.Node{group, billing, other}
	snapshot.Identities = identities
	var members []graph.MemberObservation
	for _, address := range addresses {
		endpoint := graph.NewEndpoint(address)
		snapshot.Nodes = append(snapshot.Nodes, endpoint)
		snapshot.Edges = append(snapshot.Edges, membership(endpoint.ID, address))
		members = append(members, graph.MemberObservation{Endpoint: endpoint.ID, ClientID: address})
	}
	snapshot.Observations.Groups[group.ID] = graph.GroupObservation{State: "Stable", Members: members}
	snapshot.Sort()

	return snapshot
}

func TestResolveSingleMatch(t *testing.T) {
	input := snapshotWithMembers([]graph.Identity{
		{Address: "10.0.0.1", Workload: billing.ID},
		{Address: "10.0.0.2", Workload: billing.ID},
	}, "10.0.0.1", "10.0.0.2")

	resolved := Resolve(input)

	t.Run("endpoints are replaced", func(t *testing.T) {
		want := []graph.NodeID{group.ID, other.ID, billing.ID}
		if got := nodeIDs(resolved); !reflect.DeepEqual(got, want) {
			t.Errorf("nodes = %v, want %v", got, want)
		}
	})
	t.Run("edges of two pods are merged", func(t *testing.T) {
		want := []graph.Edge{{
			From:       group.ID,
			To:         billing.ID,
			Kind:       graph.EdgeMembership,
			Confidence: graph.ConfidenceObserved,
			Evidence: []graph.Evidence{
				{Source: "main", Detail: "member client.id=10.0.0.1"},
				{Source: "test", Detail: "address 10.0.0.1 belongs to " + string(billing.ID)},
				{Source: "main", Detail: "member client.id=10.0.0.2"},
				{Source: "test", Detail: "address 10.0.0.2 belongs to " + string(billing.ID)},
			},
		}}
		if !reflect.DeepEqual(resolved.Edges, want) {
			t.Errorf("edges =\n%+v\nwant\n%+v", resolved.Edges, want)
		}
	})
	t.Run("members point to the workload", func(t *testing.T) {
		for _, member := range resolved.Observations.Groups[group.ID].Members {
			if member.Endpoint != billing.ID {
				t.Errorf("member %s endpoint = %s, want %s", member.ClientID, member.Endpoint, billing.ID)
			}
		}
	})
	t.Run("input is not modified", func(t *testing.T) {
		if input.Observations.Groups[group.ID].Members[0].Endpoint != graph.EndpointID("10.0.0.1") {
			t.Error("Resolve changed the members of its input")
		}
	})
}

func TestResolveUnresolved(t *testing.T) {
	input := snapshotWithMembers([]graph.Identity{{Address: "10.0.0.1", Workload: billing.ID}}, "192.168.7.7")

	resolved := Resolve(input)

	if !reflect.DeepEqual(resolved, input) {
		t.Errorf("an endpoint without identity changed the snapshot\n got: %+v\nwant: %+v", resolved, input)
	}
}

func TestResolveAmbiguous(t *testing.T) {
	input := snapshotWithMembers([]graph.Identity{
		{Address: "10.0.0.1", Workload: billing.ID},
		{Address: "10.0.0.1", Workload: other.ID},
	}, "10.0.0.1")

	resolved := Resolve(input)

	endpoint := graph.EndpointID("10.0.0.1")
	if len(resolved.Edges) != 1 || resolved.Edges[0].To != endpoint {
		t.Fatalf("edges = %+v, want one edge to the unresolved %s", resolved.Edges, endpoint)
	}
	wantEvidence := []graph.Evidence{
		{Source: "main", Detail: "member client.id=10.0.0.1"},
		{Source: "other", Detail: "address 10.0.0.1 belongs to " + string(other.ID) + " (ambiguous: the address belongs to several workloads)"},
		{Source: "test", Detail: "address 10.0.0.1 belongs to " + string(billing.ID) + " (ambiguous: the address belongs to several workloads)"},
	}
	if !reflect.DeepEqual(resolved.Edges[0].Evidence, wantEvidence) {
		t.Errorf("evidence =\n%+v\nwant\n%+v", resolved.Edges[0].Evidence, wantEvidence)
	}
	if resolved.Observations.Groups[group.ID].Members[0].Endpoint != endpoint {
		t.Error("an ambiguous member was resolved")
	}
}

func TestResolveNormalizesAddresses(t *testing.T) {
	tests := []struct {
		name     string
		reported string
		identity string
	}{
		{name: "java long ipv6", reported: "fd00:0:0:0:0:0:0:17", identity: "fd00::17"},
		{name: "ipv4-mapped ipv6", reported: "::ffff:10.0.0.1", identity: "10.0.0.1"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			input := snapshotWithMembers([]graph.Identity{{Address: test.identity, Workload: billing.ID}}, test.reported)

			resolved := Resolve(input)

			if len(resolved.Edges) != 1 || resolved.Edges[0].To != billing.ID {
				t.Errorf("edges = %+v, want one edge to %s", resolved.Edges, billing.ID)
			}
		})
	}
}

func nodeIDs(snapshot graph.Snapshot) []graph.NodeID {
	var ids []graph.NodeID
	for _, node := range snapshot.Nodes {
		ids = append(ids, node.ID)
	}

	return ids
}
