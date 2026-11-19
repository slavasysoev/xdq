package resolve

import (
	"maps"
	"slices"

	"github.com/slavasysoev/xdq/internal/graph"
)

// inferProducers returns a producer edge from a workload to every topic its configuration names
// exactly, unless the workload consumes that topic (§7.2). Kafka does not know its producers, so
// this is the only way to see them. A workload that both consumes and produces a topic is seen as
// a consumer only. A name found in several Kafka sources gives an edge to each.
func inferProducers(snapshot graph.Snapshot) []graph.Edge {
	nodes := make(map[graph.NodeID]graph.Node, len(snapshot.Nodes))
	topicsByName := make(map[string][]graph.NodeID)
	for _, node := range snapshot.Nodes {
		nodes[node.ID] = node
		if node.Kind == graph.NodeTopic {
			topicsByName[node.Name] = append(topicsByName[node.Name], node.ID)
		}
	}
	consumed := consumedTopics(snapshot)

	producers := make(map[edgeKey]graph.Edge)
	for _, value := range snapshot.ConfigValues {
		for _, token := range value.Tokens() {
			for _, topic := range topicsByName[token] {
				if consumed[value.Workload][topic] {
					continue
				}
				key := edgeKey{from: value.Workload, to: topic, kind: graph.EdgeProducer}
				edge, seen := producers[key]
				if !seen {
					edge = graph.Edge{From: value.Workload, To: topic, Kind: graph.EdgeProducer, Confidence: graph.ConfidenceInferred}
				}
				edge.Evidence = append(edge.Evidence, graph.Evidence{Source: nodes[value.Workload].Source, Detail: referenceDetail(value, token)})
				producers[key] = edge
			}
		}
	}

	// The caller sorts the edges.
	return slices.Collect(maps.Values(producers))
}

// consumedTopics returns, for every workload, the topics consumed by the groups it is a member of.
func consumedTopics(snapshot graph.Snapshot) map[graph.NodeID]map[graph.NodeID]bool {
	topicsOfGroup := make(map[graph.NodeID][]graph.NodeID)
	for _, edge := range snapshot.Edges {
		if edge.Kind == graph.EdgeConsumer {
			topicsOfGroup[edge.To] = append(topicsOfGroup[edge.To], edge.From)
		}
	}
	consumed := make(map[graph.NodeID]map[graph.NodeID]bool)
	for _, edge := range snapshot.Edges {
		if edge.Kind != graph.EdgeMembership {
			continue
		}
		if consumed[edge.To] == nil {
			consumed[edge.To] = make(map[graph.NodeID]bool)
		}
		for _, topic := range topicsOfGroup[edge.From] {
			consumed[edge.To][topic] = true
		}
	}

	return consumed
}

// referenceDetail explains where the configuration names the topic: "env KAFKA_OUT_TOPIC=invoices"
// for a value that is just the name, "configmap app/application.yml mentions invoices" otherwise.
func referenceDetail(value graph.ConfigValue, token string) string {
	if value.Value == token {
		return value.Origin + "=" + token
	}

	return value.Origin + " mentions " + token
}
