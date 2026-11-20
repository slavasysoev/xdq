package tui

import (
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/style"
)

// nodeDetails describes a node of the chain: its findings, observations and edges with evidence.
func nodeDetails(chain report.Chain, node report.ChainNode, findings []report.Finding) []string {
	lines := []string{statusLabel(node.Status) + "  " + string(node.ID)}
	if node.Gap {
		lines = append(lines, hintStyle.Render("gap: nothing discovered downstream, the end of the chain or an undiscovered producer"))
	}
	lines = append(lines, findingLines(node.ID, findings)...)
	lines = append(lines, observationLines(chain.Observations, node.ID)...)

	return append(lines, edgeLines(chain, node.ID)...)
}

func findingLines(nodeID graph.NodeID, findings []report.Finding) []string {
	var lines []string
	for _, finding := range findings {
		if !slices.Contains(finding.Subjects, nodeID) {
			continue
		}
		header := "  " + statusLabel(finding.Severity.Status()) + "  " + finding.Rule
		if finding.Ignored {
			header += "  (ignored)"
		}
		lines = append(lines, header)
		for line := range strings.Lines(finding.Message) {
			lines = append(lines, "    "+strings.TrimSuffix(line, "\n"))
		}
		for _, evidence := range finding.Evidence {
			lines = append(lines, "    "+evidence.Source+": "+evidence.Detail)
		}
	}

	return section("Findings", lines)
}

func observationLines(observations graph.Observations, nodeID graph.NodeID) []string {
	var lines []string
	if topic, found := observations.Topics[nodeID]; found {
		lines = append(lines, fmt.Sprintf("  partitions %d", topic.Partitions))
		if topic.LatestRecord == nil {
			lines = append(lines, "  latest record: none")
		} else {
			lines = append(lines, "  latest record: "+topic.LatestRecord.Local().Format(time.DateTime))
		}
	}
	if group, found := observations.Groups[nodeID]; found {
		lines = append(lines, "  state "+group.State)
		for _, topic := range slices.Sorted(maps.Keys(group.Lag)) {
			lines = append(lines, fmt.Sprintf("  lag %d on %s", group.Lag[topic], topic))
		}
		for _, member := range group.Members {
			lines = append(lines, fmt.Sprintf("  member %s %s  %s", member.Endpoint, member.ClientID, formatAssignment(member.Assignment)))
		}
	}

	return section("Observations", lines)
}

// edgeLines lists the edges into and out of the node, each with its evidence.
func edgeLines(chain report.Chain, nodeID graph.NodeID) []string {
	var lines []string
	for _, edge := range chain.Edges {
		var line string
		switch nodeID {
		case edge.To:
			line = fmt.Sprintf("  ← %s from %s", edge.Kind, edge.From)
		case edge.From:
			line = fmt.Sprintf("  → %s to %s", edge.Kind, edge.To)
		default:
			continue
		}
		if edge.Confidence == graph.ConfidenceInferred {
			line += "  (inferred)"
		}
		lines = append(lines, line)
		for _, evidence := range edge.Evidence {
			lines = append(lines, "      "+evidence.Source+": "+evidence.Detail)
		}
	}

	return section("Edges", lines)
}

// section puts a title and an empty line before the lines, or returns nothing when there are none.
func section(title string, lines []string) []string {
	if len(lines) == 0 {
		return nil
	}

	return append([]string{"", titleStyle.Render(title)}, lines...)
}

// formatAssignment prints the partitions of every topic as "topic:main/orders 0,1,2".
func formatAssignment(assignment map[graph.NodeID][]int32) string {
	if len(assignment) == 0 {
		return "no partitions assigned"
	}
	var topics []string
	for _, topic := range slices.Sorted(maps.Keys(assignment)) {
		var partitions []string
		for _, partition := range assignment[topic] {
			partitions = append(partitions, strconv.Itoa(int(partition)))
		}
		topics = append(topics, string(topic)+" "+strings.Join(partitions, ","))
	}

	return strings.Join(topics, "; ")
}

// statusLabel is the status in upper case, padded to the longest status and colored.
func statusLabel(status report.Status) string {
	return style.Status(status, fmt.Sprintf("%-7s", strings.ToUpper(string(status))))
}
