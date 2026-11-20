package rule

import (
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// checkLagBeyondRetention reports groups whose committed offset is below the start offset of a
// partition: retention deleted records the group had not read. Restarting the consumer will not
// bring them back. The deleted count is approximate, because offsets of transactional and
// compacted topics have gaps.
func checkLagBeyondRetention(input Input, settings settings) []report.Finding {
	nodes := nodesByID(input.Graph)
	groups := input.Graph.Observations.Groups
	var findings []report.Finding
	for _, group := range slices.Sorted(maps.Keys(groups)) {
		committed := groups[group].Committed
		for _, topic := range slices.Sorted(maps.Keys(committed)) {
			start := input.Graph.Observations.Topics[topic].StartOffsets
			var partitions []int32
			var evidence []graph.Evidence
			var deleted int64
			for _, partition := range slices.Sorted(maps.Keys(committed[topic])) {
				offset := committed[topic][partition]
				startOffset, known := start[partition]
				if !known || offset >= startOffset {
					continue
				}
				partitions = append(partitions, partition)
				deleted += startOffset - offset
				evidence = append(evidence, graph.Evidence{
					Source: nodes[group].Source,
					Detail: fmt.Sprintf("partition %d: committed %d, start %d", partition, offset, startOffset),
				})
			}
			if len(partitions) == 0 {
				continue
			}
			message := fmt.Sprintf("committed offset is below the start offset of %s on partitions %s: about %d records were deleted unread; restarting the consumer will not process them",
				nodes[topic].Name, joinPartitions(partitions), deleted)
			findings = append(findings, settings.finding(report.SeverityError, []graph.NodeID{group, topic}, message, evidence))
		}
	}

	return findings
}

func joinPartitions(partitions []int32) string {
	texts := make([]string, len(partitions))
	for index, partition := range partitions {
		texts[index] = fmt.Sprint(partition)
	}

	return strings.Join(texts, ",")
}
