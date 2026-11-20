package graph

import "time"

// Snapshot is everything one source discovered. It is plain data that round-trips through JSON,
// so a source may later run as an external process.
type Snapshot struct {
	Nodes        []Node        `json:"nodes"`
	Edges        []Edge        `json:"edges"`
	Identities   []Identity    `json:"identities"`
	ConfigValues []ConfigValue `json:"configValues"`
	Observations Observations  `json:"observations"`
}

// Observations are volatile measurements. They feed rules and the UI and are never locked.
type Observations struct {
	Topics    map[NodeID]TopicObservation    `json:"topics,omitempty"`
	Groups    map[NodeID]GroupObservation    `json:"groups,omitempty"`
	Workloads map[NodeID]WorkloadObservation `json:"workloads,omitempty"`
}

// WorkloadObservation describes the pods of a workload. Finished pods are not counted.
type WorkloadObservation struct {
	Pods      int `json:"pods"`
	ReadyPods int `json:"readyPods"`
	// NotReady lists every pod that is not ready, sorted by pod name.
	NotReady []PodProblem `json:"notReady,omitempty"`
}

// PodProblem is a pod that is not ready. Pod names appear only here and are never locked.
type PodProblem struct {
	Pod string `json:"pod"`
	// Reason is the waiting reason of a container (CrashLoopBackOff, ImagePullBackOff, ...),
	// Unschedulable for a pod that is not scheduled, otherwise NotReady.
	Reason string `json:"reason"`
	// Restarts is the number of container restarts of the pod.
	Restarts int32 `json:"restarts"`
}

// TopicObservation describes the current state of a topic.
type TopicObservation struct {
	Partitions int `json:"partitions"`
	// StartOffsets maps a partition to its start offset (the offset of the oldest record that is still kept).
	StartOffsets map[int32]int64 `json:"startOffsets,omitempty"`
	// EndOffsets maps a partition to its end offset (the offset of the next record).
	EndOffsets map[int32]int64 `json:"endOffsets"`
	// LatestRecord is the timestamp of the latest record, nil when the topic holds no records.
	LatestRecord *time.Time `json:"latestRecord,omitempty"`
}

// GroupObservation describes the current state of a consumer group.
type GroupObservation struct {
	// State is reported by Kafka: Stable, Empty, PreparingRebalance, Dead, etc.
	State string `json:"state"`
	// Lag maps a topic to the group's lag summed over the partitions it has committed.
	Lag map[NodeID]int64 `json:"lag,omitempty"`
	// Committed maps a topic to the group's committed offset on each partition it has committed.
	Committed map[NodeID]map[int32]int64 `json:"committed,omitempty"`
	Members   []MemberObservation        `json:"members,omitempty"`
}

// MemberObservation is one member of a consumer group.
type MemberObservation struct {
	// Endpoint is the node the member connects from; resolution replaces it with a workload.
	Endpoint NodeID `json:"endpoint"`
	ClientID string `json:"clientId"`
	// Assignment maps a topic to the partitions assigned to this member.
	Assignment map[NodeID][]int32 `json:"assignment,omitempty"`
}

// Identity states that a network address belongs to a workload.
type Identity struct {
	Address  string `json:"address"`
	Workload NodeID `json:"workload"`
}

// ConfigValue is one piece of a workload's configuration, scanned for topic names.
type ConfigValue struct {
	Workload NodeID `json:"workload"`
	// Origin tells where the value comes from, e.g. "env KAFKA_OUT_TOPIC" or
	// "configmap app-config/application.yml".
	Origin string `json:"origin"`
	Value  string `json:"value"`
}

// NewSnapshot returns an empty snapshot whose observation maps are ready to be written to.
func NewSnapshot() Snapshot {
	return Snapshot{
		Observations: Observations{
			Topics:    make(map[NodeID]TopicObservation),
			Groups:    make(map[NodeID]GroupObservation),
			Workloads: make(map[NodeID]WorkloadObservation),
		},
	}
}
