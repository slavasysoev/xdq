package kafka

import (
	"cmp"
	"maps"
	"slices"
	"strconv"
	"strings"

	"github.com/slavasysoev/xdq/internal/graph"
)

// buildSnapshot turns the raw cluster state into nodes, edges and observations.
// The output is sorted, so the same cluster state always gives the same snapshot.
func buildSnapshot(sourceName string, cluster clusterState) graph.Snapshot {
	snapshot := graph.NewSnapshot()
	addTopics(&snapshot, sourceName, cluster.topics)

	// An endpoint node is added once, however many groups it is a member of.
	endpoints := make(map[graph.NodeID]bool)
	for _, name := range slices.Sorted(maps.Keys(cluster.groups)) {
		addGroup(&snapshot, sourceName, name, cluster, endpoints)
	}
	snapshot.Sort()

	return snapshot
}

func addTopics(snapshot *graph.Snapshot, sourceName string, topics map[string]topicState) {
	for _, name := range slices.Sorted(maps.Keys(topics)) {
		topic := topics[name]
		node := graph.NewTopic(sourceName, name)
		snapshot.Nodes = append(snapshot.Nodes, node)
		snapshot.Observations.Topics[node.ID] = graph.TopicObservation{
			Partitions:   topic.partitions,
			EndOffsets:   topic.endOffsets,
			LatestRecord: topic.latestRecord,
		}
	}
}

// addGroup adds the group node, its subscriptions, its members and the group observation.
func addGroup(snapshot *graph.Snapshot, sourceName, name string, cluster clusterState, endpoints map[graph.NodeID]bool) {
	group := cluster.groups[name]
	groupNode := graph.NewGroup(sourceName, name)
	snapshot.Nodes = append(snapshot.Nodes, groupNode)

	observation := graph.GroupObservation{State: group.phase}
	for _, topic := range subscribedTopics(group, cluster.topics) {
		topicID := graph.TopicID(sourceName, topic)
		snapshot.Edges = append(snapshot.Edges, graph.Edge{
			From:       topicID,
			To:         groupNode.ID,
			Kind:       graph.EdgeConsumer,
			Confidence: graph.ConfidenceObserved,
			Evidence:   []graph.Evidence{{Source: sourceName, Detail: subscriptionDetail(group, topic)}},
		})
		lag, hasLag := topicLag(group.committed[topic], cluster.topics[topic].endOffsets)
		if hasLag {
			if observation.Lag == nil {
				observation.Lag = make(map[graph.NodeID]int64)
			}
			observation.Lag[topicID] = lag
		}
	}
	observation.Members = addMembers(snapshot, sourceName, groupNode.ID, group, endpoints)
	snapshot.Observations.Groups[groupNode.ID] = observation
}

// addMembers adds the endpoint node of every member and one membership edge per endpoint.
// It returns the member observations, sorted.
func addMembers(
	snapshot *graph.Snapshot,
	sourceName string,
	groupID graph.NodeID,
	group groupState,
	endpoints map[graph.NodeID]bool,
) []graph.MemberObservation {
	var members []graph.MemberObservation
	membershipEvidence := make(map[graph.NodeID][]graph.Evidence)
	for _, member := range group.members {
		endpoint := graph.NewEndpoint(member.host)
		if !endpoints[endpoint.ID] {
			endpoints[endpoint.ID] = true
			snapshot.Nodes = append(snapshot.Nodes, endpoint)
		}
		membershipEvidence[endpoint.ID] = append(membershipEvidence[endpoint.ID],
			graph.Evidence{Source: sourceName, Detail: "member client.id=" + member.clientID})
		members = append(members, graph.MemberObservation{
			Endpoint:   endpoint.ID,
			ClientID:   member.clientID,
			Assignment: memberAssignment(sourceName, member.assignment),
		})
	}
	for _, endpoint := range slices.Sorted(maps.Keys(membershipEvidence)) {
		snapshot.Edges = append(snapshot.Edges, graph.Edge{
			From:       groupID,
			To:         endpoint,
			Kind:       graph.EdgeMembership,
			Confidence: graph.ConfidenceObserved,
			Evidence:   membershipEvidence[endpoint],
		})
	}
	slices.SortFunc(members, func(left, right graph.MemberObservation) int {
		return cmp.Or(cmp.Compare(left.Endpoint, right.Endpoint), cmp.Compare(left.ClientID, right.ClientID))
	})

	return members
}

// subscribedTopics returns the topics the group is assigned or has committed offsets for.
// Committed offsets matter: a stalled group has no members, but still has offsets.
// Topics that no longer exist are skipped.
func subscribedTopics(group groupState, topics map[string]topicState) []string {
	subscribed := make(map[string]bool)
	for _, member := range group.members {
		for topic := range member.assignment {
			subscribed[topic] = true
		}
	}
	for topic := range group.committed {
		subscribed[topic] = true
	}
	var result []string
	for _, topic := range slices.Sorted(maps.Keys(subscribed)) {
		_, exists := topics[topic]
		if exists {
			result = append(result, topic)
		}
	}

	return result
}

func subscriptionDetail(group groupState, topic string) string {
	var assigned []int32
	for _, member := range group.members {
		assigned = append(assigned, member.assignment[topic]...)
	}
	var parts []string
	if len(assigned) > 0 {
		parts = append(parts, "assigned partitions "+formatPartitions(assigned))
	}
	if len(group.committed[topic]) > 0 {
		parts = append(parts, "committed offsets on partitions "+formatPartitions(slices.Collect(maps.Keys(group.committed[topic]))))
	}

	return strings.Join(parts, "; ")
}

// topicLag sums the lag over the partitions the group has committed. Partitions without
// a commit are skipped: their lag is unknown.
func topicLag(committed map[int32]int64, endOffsets map[int32]int64) (int64, bool) {
	if len(committed) == 0 {
		return 0, false
	}
	var lag int64
	for partition, offset := range committed {
		lag += max(endOffsets[partition]-offset, 0)
	}

	return lag, true
}

func memberAssignment(sourceName string, assignment map[string][]int32) map[graph.NodeID][]int32 {
	if len(assignment) == 0 {
		return nil
	}
	result := make(map[graph.NodeID][]int32, len(assignment))
	for topic, partitions := range assignment {
		result[graph.TopicID(sourceName, topic)] = slices.Sorted(slices.Values(partitions))
	}

	return result
}

// formatPartitions returns sorted partitions as "0,1,2".
func formatPartitions(partitions []int32) string {
	sorted := slices.Sorted(slices.Values(partitions))
	texts := make([]string, len(sorted))
	for index, partition := range sorted {
		texts[index] = strconv.Itoa(int(partition))
	}

	return strings.Join(texts, ",")
}
