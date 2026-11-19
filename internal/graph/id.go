package graph

// NodeID is a stable, human-readable node identifier. IDs are stored in the lockfile,
// so they never contain volatile data such as IPs of pods or replica names.
//
// The prefix is the node kind, not the source type: the graph does not depend on which
// source discovered a node. IDs are unique as long as source names contain no "/": every
// name that may contain "/" (e.g. a Kafka group) is the last segment of its ID.
type NodeID string

// TopicID returns the ID of a topic, e.g. "topic:main/orders.created".
func TopicID(sourceName, topic string) NodeID {
	return NodeID("topic:" + sourceName + "/" + topic)
}

// GroupID returns the ID of a consumer group, e.g. "group:main/billing".
func GroupID(sourceName, group string) NodeID {
	return NodeID("group:" + sourceName + "/" + group)
}

// WorkloadID returns the ID of a workload, e.g. "workload:test/stand-a/deployment/billing".
func WorkloadID(sourceName, namespace string, kind WorkloadKind, name string) NodeID {
	return NodeID("workload:" + sourceName + "/" + namespace + "/" + string(kind) + "/" + name)
}

// DatabaseID returns the ID of a database, e.g. "database:orders-db/orders".
func DatabaseID(sourceName, database string) NodeID {
	return NodeID("database:" + sourceName + "/" + database)
}

// EndpointID returns the ID of a network client known only by address, e.g. "endpoint:10.4.2.17".
func EndpointID(address string) NodeID {
	return NodeID("endpoint:" + address)
}
