package graph

// NodeKind is the kind of a discovered entity.
type NodeKind string

// Node kinds.
const (
	NodeTopic    NodeKind = "topic"
	NodeGroup    NodeKind = "group"
	NodeWorkload NodeKind = "workload"
	NodeDatabase NodeKind = "database"
	NodeEndpoint NodeKind = "endpoint"
)

// WorkloadKind is the top-level owner of a pod. A pod without an owner is a workload of kind pod.
type WorkloadKind string

// Workload kinds.
const (
	WorkloadDeployment  WorkloadKind = "deployment"
	WorkloadStatefulSet WorkloadKind = "statefulset"
	WorkloadDaemonSet   WorkloadKind = "daemonset"
	WorkloadCronJob     WorkloadKind = "cronjob"
	WorkloadJob         WorkloadKind = "job"
	WorkloadPod         WorkloadKind = "pod"
)

// Node is a discovered entity. It is one flat struct for all kinds, so it maps directly to
// JSON; fields that apply to one kind only are empty for the others.
type Node struct {
	ID   NodeID   `json:"id"`
	Kind NodeKind `json:"kind"`
	// Source is the name of the source from the config. Empty for endpoints.
	Source string `json:"source,omitempty"`
	Name   string `json:"name"`

	// Namespace, WorkloadKind and Environment are set for workloads only.
	Namespace    string       `json:"namespace,omitempty"`
	WorkloadKind WorkloadKind `json:"workloadKind,omitempty"`
	Environment  string       `json:"environment,omitempty"`
}

// NewTopic returns a topic node.
func NewTopic(sourceName, name string) Node {
	return Node{ID: TopicID(sourceName, name), Kind: NodeTopic, Source: sourceName, Name: name}
}

// NewGroup returns a consumer group node.
func NewGroup(sourceName, name string) Node {
	return Node{ID: GroupID(sourceName, name), Kind: NodeGroup, Source: sourceName, Name: name}
}

// NewWorkload returns a workload node. The caller sets Environment, because how it is
// derived depends on the source configuration.
func NewWorkload(sourceName, namespace string, kind WorkloadKind, name string) Node {
	return Node{
		ID:           WorkloadID(sourceName, namespace, kind, name),
		Kind:         NodeWorkload,
		Source:       sourceName,
		Name:         name,
		Namespace:    namespace,
		WorkloadKind: kind,
	}
}

// NewDatabase returns a database node.
func NewDatabase(sourceName, name string) Node {
	return Node{ID: DatabaseID(sourceName, name), Kind: NodeDatabase, Source: sourceName, Name: name}
}

// NewEndpoint returns a node for a network client known only by address.
func NewEndpoint(address string) Node {
	return Node{ID: EndpointID(address), Kind: NodeEndpoint, Name: address}
}
