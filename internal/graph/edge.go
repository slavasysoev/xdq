package graph

// EdgeKind describes a relation between nodes. Edges always point in the direction data flows.
type EdgeKind string

// Edge kinds.
const (
	// EdgeConsumer points from a topic to a group consuming it.
	EdgeConsumer EdgeKind = "consumer"
	// EdgeMembership points from a group to a workload or endpoint that is its member.
	EdgeMembership EdgeKind = "membership"
	// EdgeProducer points from a workload to a topic it writes to.
	EdgeProducer EdgeKind = "producer"
	// EdgeConnection points from a workload or endpoint to a database it is connected to.
	EdgeConnection EdgeKind = "connection"
)

// Confidence tells whether a system reported an edge or xdq derived it.
type Confidence string

// Confidence levels.
const (
	ConfidenceObserved Confidence = "observed"
	ConfidenceInferred Confidence = "inferred"
)

// Edge is a relation between two nodes. From, Kind and To identify the edge.
type Edge struct {
	From       NodeID     `json:"from"`
	To         NodeID     `json:"to"`
	Kind       EdgeKind   `json:"kind"`
	Confidence Confidence `json:"confidence"`
	Evidence   []Evidence `json:"evidence"`
}

// Evidence explains which source reported an edge and why.
type Evidence struct {
	Source string `json:"source"`
	// Detail is a human-readable explanation, e.g. "env KAFKA_OUT_TOPIC=invoices.created".
	Detail string `json:"detail"`
}
