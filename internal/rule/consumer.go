package rule

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// recentActivity is how far back a record counts as "the topic receives records". Older topics
// are idle, and idle topics are normal in test environments (§9.1).
const recentActivity = 7 * 24 * time.Hour

// internalTopicPrefix marks topics of Kafka and its tooling (__consumer_offsets, _schemas, ...).
const internalTopicPrefix = "_"

// checkStalledConsumer reports groups with lag and no active members: nobody processes the backlog.
func checkStalledConsumer(input Input, settings settings) []report.Finding {
	nodes := nodesByID(input.Graph)
	groups := input.Graph.Observations.Groups
	var findings []report.Finding
	for _, group := range slices.Sorted(maps.Keys(groups)) {
		observation := groups[group]
		if len(observation.Members) > 0 {
			continue
		}
		subjects := []graph.NodeID{group}
		var evidence []graph.Evidence
		var total int64
		for _, topic := range slices.Sorted(maps.Keys(observation.Lag)) {
			lag := observation.Lag[topic]
			if lag == 0 {
				continue
			}
			total += lag
			subjects = append(subjects, topic)
			evidence = append(evidence, graph.Evidence{
				Source: nodes[group].Source,
				Detail: fmt.Sprintf("lag %d on %s", lag, nodes[topic].Name),
			})
		}
		if total == 0 {
			continue
		}
		message := fmt.Sprintf("lag %d and no active members (state %s)", total, observation.State)
		findings = append(findings, settings.finding(report.SeverityError, subjects, message, evidence))
	}

	return findings
}

// checkLaggingConsumer reports each topic on which a group's lag exceeds the threshold.
func checkLaggingConsumer(input Input, settings settings) []report.Finding {
	nodes := nodesByID(input.Graph)
	groups := input.Graph.Observations.Groups
	var findings []report.Finding
	for _, group := range slices.Sorted(maps.Keys(groups)) {
		lags := groups[group].Lag
		for _, topic := range slices.Sorted(maps.Keys(lags)) {
			if lags[topic] <= int64(settings.threshold) {
				continue
			}
			message := fmt.Sprintf("lag %d on %s exceeds the threshold %d", lags[topic], nodes[topic].Name, settings.threshold)
			findings = append(findings, settings.finding(report.SeverityWarning, []graph.NodeID{group, topic}, message, nil))
		}
	}

	return findings
}

// checkEmptySubscription reports groups consuming a topic that has never received a record,
// the typical sign of a topic auto-created from a typo. End offsets are used rather than the
// latest record: a topic whose records expired by retention did receive records.
func checkEmptySubscription(input Input, settings settings) []report.Finding {
	nodes := nodesByID(input.Graph)
	var findings []report.Finding
	for _, edge := range input.Graph.Edges {
		if edge.Kind != graph.EdgeConsumer {
			continue
		}
		observation, observed := input.Graph.Observations.Topics[edge.From]
		if !observed || !neverReceived(observation) || isDeadLetter(nodes[edge.From].Name, settings.suffixes) {
			continue
		}
		message := fmt.Sprintf("consumes %s, which has never received a record", nodes[edge.From].Name)
		findings = append(findings, settings.finding(report.SeverityWarning, []graph.NodeID{edge.To, edge.From}, message, edge.Evidence))
	}

	return findings
}

// checkUnconsumedTopic reports topics that received records recently but have no consumer group:
// records are written and nobody reads them.
func checkUnconsumedTopic(input Input, settings settings) []report.Finding {
	consumed := make(map[graph.NodeID]bool)
	for _, edge := range input.Graph.Edges {
		if edge.Kind == graph.EdgeConsumer {
			consumed[edge.From] = true
		}
	}
	nodes := nodesByID(input.Graph)
	topics := input.Graph.Observations.Topics
	var findings []report.Finding
	for _, topic := range slices.Sorted(maps.Keys(topics)) {
		latest := topics[topic].LatestRecord
		if consumed[topic] || latest == nil || input.Now.Sub(*latest) > recentActivity ||
			strings.HasPrefix(nodes[topic].Name, internalTopicPrefix) || isDeadLetter(nodes[topic].Name, settings.suffixes) {
			continue
		}
		message := fmt.Sprintf("received a record at %s, but no group consumes it", latest.Format(time.RFC3339))
		findings = append(findings, settings.finding(report.SeverityWarning, []graph.NodeID{topic}, message, nil))
	}

	return findings
}

func neverReceived(observation graph.TopicObservation) bool {
	for _, offset := range observation.EndOffsets {
		if offset > 0 {
			return false
		}
	}

	return true
}
