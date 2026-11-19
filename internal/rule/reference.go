package rule

import (
	"fmt"
	"slices"
	"strings"

	"github.com/slavasysoev/xdq/internal/distance"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// minNearMissLength is the shortest topic name that near-miss-topic suggests. Shorter names are
// a few edits away from too many ordinary words in a configuration file.
const minNearMissLength = 5

// longNameLength is the length from which a name may be two edits away instead of one.
const longNameLength = 10

// reference is one name in a workload's configuration that looks like a mistyped topic.
type reference struct {
	workload graph.NodeID
	token    string
	// exact are the topics with exactly this name. They exist, but never received a record: a
	// consumer with a typo auto-creates its topic.
	exact       []graph.NodeID
	suggestions []graph.NodeID
	evidence    []graph.Evidence
}

// checkNearMissTopic reports names in a workload's configuration that are 1–2 edits away from a
// topic that receives records, when the name itself is not a topic in use: it does not exist, or
// it never received a record (§7.2). Names shorter than 10 characters may be one edit away only.
func checkNearMissTopic(input Input, settings settings) []report.Finding {
	nodes := nodesByID(input.Graph)
	topicsByName := make(map[string][]graph.NodeID)
	var active []graph.Node
	for _, node := range input.Graph.Nodes {
		if node.Kind != graph.NodeTopic {
			continue
		}
		topicsByName[node.Name] = append(topicsByName[node.Name], node.ID)
		if hasRecords(input.Graph, node.ID) && len(node.Name) >= minNearMissLength && !strings.HasPrefix(node.Name, internalTopicPrefix) {
			active = append(active, node)
		}
	}

	var references []*reference
	byKey := make(map[string]*reference)
	for _, value := range input.Graph.ConfigValues {
		for _, token := range value.Tokens() {
			exact := topicsByName[token]
			if slices.ContainsFunc(exact, func(topic graph.NodeID) bool { return hasRecords(input.Graph, topic) }) {
				continue
			}
			suggestions := nearTopics(token, active)
			if len(suggestions) == 0 {
				continue
			}
			key := string(value.Workload) + "\x00" + token
			found := byKey[key]
			if found == nil {
				found = &reference{workload: value.Workload, token: token, exact: exact, suggestions: suggestions}
				byKey[key] = found
				references = append(references, found)
			}
			found.evidence = append(found.evidence, graph.Evidence{Source: nodes[value.Workload].Source, Detail: value.Origin})
		}
	}

	findings := make([]report.Finding, 0, len(references))
	for _, found := range references {
		findings = append(findings, settings.finding(report.SeverityWarning, slices.Concat([]graph.NodeID{found.workload}, found.suggestions, found.exact),
			nearMissMessage(found, nodes), found.evidence))
	}

	return findings
}

func nearMissMessage(found *reference, nodes map[graph.NodeID]graph.Node) string {
	state := "which is not a topic"
	if len(found.exact) > 0 {
		state = "a topic that has never received a record"
	}
	var names []string
	for _, topic := range found.suggestions {
		name := nodes[topic].Name
		if !slices.Contains(names, name) {
			names = append(names, name)
		}
	}

	return fmt.Sprintf("config names %s, %s; did you mean %s?", found.token, state, strings.Join(names, " or "))
}

// nearTopics returns the topics within the allowed edit distance of the name, in graph order.
func nearTopics(name string, topics []graph.Node) []graph.NodeID {
	var near []graph.NodeID
	for _, topic := range topics {
		limit := 1
		if len(topic.Name) >= longNameLength {
			limit = 2
		}
		edits := distance.Levenshtein(name, topic.Name, limit)
		if edits > 0 && edits <= limit {
			near = append(near, topic.ID)
		}
	}

	return near
}

func hasRecords(snapshot graph.Snapshot, topic graph.NodeID) bool {
	observation, observed := snapshot.Observations.Topics[topic]

	return observed && !neverReceived(observation)
}
