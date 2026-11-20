package kafka

import (
	"maps"
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
)

func TestBuildSnapshotKeepsStartAndCommittedOffsets(t *testing.T) {
	cluster := clusterState{
		topics: map[string]topicState{
			"orders": {partitions: 2, startOffsets: map[int32]int64{0: 130, 1: 0}, endOffsets: map[int32]int64{0: 200, 1: 5}},
		},
		groups: map[string]groupState{
			"billing": {phase: "Empty", committed: map[string]map[int32]int64{"orders": {0: 10, 1: 5}}},
			"fresh":   {phase: "Empty"},
		},
	}
	orders := graph.TopicID("main", "orders")

	snapshot := buildSnapshot("main", cluster)

	if got, want := snapshot.Observations.Topics[orders].StartOffsets, cluster.topics["orders"].startOffsets; !maps.Equal(got, want) {
		t.Errorf("start offsets = %v, want %v", got, want)
	}
	if got, want := snapshot.Observations.Groups[graph.GroupID("main", "billing")].Committed[orders], (map[int32]int64{0: 10, 1: 5}); !maps.Equal(got, want) {
		t.Errorf("committed offsets = %v, want %v", got, want)
	}
	if committed := snapshot.Observations.Groups[graph.GroupID("main", "fresh")].Committed; committed != nil {
		t.Errorf("a group without commits has committed offsets %v, want none", committed)
	}
}
