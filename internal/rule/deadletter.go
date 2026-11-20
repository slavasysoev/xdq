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

// defaultDeadLetterSuffixes are the endings of dead-letter topic names. Spring Kafka writes to
// <topic>.DLT, and its retry topics end in -dlt.
var defaultDeadLetterSuffixes = []string{".dlt", "-dlt", "_dlt", ".dlq", "-dlq", "_dlq"}

// deadLetterBase returns the name without the dead-letter suffix and true, or false when the
// name is not a dead-letter topic. The suffixes are compared ignoring case.
func deadLetterBase(name string, suffixes []string) (string, bool) {
	lower := strings.ToLower(name)
	for _, suffix := range suffixes {
		suffix = strings.ToLower(suffix)
		if strings.HasSuffix(lower, suffix) {
			return name[:len(name)-len(suffix)], true
		}
	}

	return "", false
}

func isDeadLetter(name string, suffixes []string) bool {
	_, found := deadLetterBase(name, suffixes)

	return found
}

// checkDeadLetterRecords reports dead-letter topics that received a record within the window.
// Records there are the only hint, without reading payloads, that a consumer fails on its messages.
func checkDeadLetterRecords(input Input, settings settings) []report.Finding {
	nodes := nodesByID(input.Graph)
	topics := input.Graph.Observations.Topics
	var findings []report.Finding
	for _, topic := range slices.Sorted(maps.Keys(topics)) {
		latest := topics[topic].LatestRecord
		base, found := deadLetterBase(nodes[topic].Name, settings.suffixes)
		if !found || latest == nil || input.Now.Sub(*latest) > settings.window {
			continue
		}
		subjects := []graph.NodeID{topic}
		baseID := graph.TopicID(nodes[topic].Source, base)
		if _, exists := nodes[baseID]; exists && base != "" {
			subjects = append(subjects, baseID)
		}
		message := fmt.Sprintf("dead-letter topic %s received a record at %s", nodes[topic].Name, latest.Format(time.RFC3339))
		findings = append(findings, settings.finding(report.SeverityWarning, subjects, message, nil))
	}

	return findings
}
