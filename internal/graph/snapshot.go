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
	Topics map[NodeID]TopicObservation `json:"topics,omitempty"`
	Groups map[NodeID]GroupObservation `json:"groups,omitempty"`
}

// TopicObservation describes the current state of a topic.
type TopicObservation struct {
	Partitions int `json:"partitions"`
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
	Lag     map[NodeID]int64    `json:"lag,omitempty"`
	Members []MemberObservation `json:"members,omitempty"`
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
			Topics: make(map[NodeID]TopicObservation),
			Groups: make(map[NodeID]GroupObservation),
		},
	}
}
