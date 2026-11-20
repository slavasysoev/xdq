package kafka

import (
	"context"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// clusterState is the raw answer of the cluster, before it is turned into a graph.
type clusterState struct {
	topics map[string]topicState
	groups map[string]groupState
}

type topicState struct {
	partitions   int
	startOffsets map[int32]int64
	endOffsets   map[int32]int64
	latestRecord *time.Time
}

type groupState struct {
	// phase is the group state reported by Kafka: Stable, Empty, PreparingRebalance, etc.
	phase   string
	members []memberState
	// committed maps topic → partition → committed offset.
	committed map[string]map[int32]int64
}

type memberState struct {
	clientID string
	// host is the address of the client without the leading "/" that Kafka adds: "10.4.2.17".
	host string
	// assignment maps topic → assigned partitions.
	assignment map[string][]int32
}

// Group types and protocol types reported by ListGroups.
const (
	groupTypeClassic     = "classic"
	groupTypeConsumer    = "consumer"
	protocolTypeConsumer = "consumer"
)

// responseError returns err or, when the request succeeded, the first error inside the response.
// franz-go reports a failure either for the whole request or for one part of it.
func responseError(err error, response interface{ Error() error }) error {
	if err != nil {
		return err
	}

	return response.Error()
}

// memberHost removes the leading "/" from client.host as Kafka reports it, e.g. "/10.4.2.17".
func memberHost(clientHost string) string {
	return strings.TrimPrefix(clientHost, "/")
}

func (source *Source) fetch(ctx context.Context) (clusterState, error) {
	topics, err := source.fetchTopics(ctx)
	if err != nil {
		return clusterState{}, err
	}
	groups, err := source.fetchGroups(ctx)
	if err != nil {
		return clusterState{}, err
	}

	return clusterState{topics: topics, groups: groups}, nil
}

func (source *Source) fetchTopics(ctx context.Context) (map[string]topicState, error) {
	details, err := source.admin.ListTopics(ctx)
	err = responseError(err, details)
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	topics := make(map[string]topicState, len(details))
	if len(details) == 0 {
		return topics, nil
	}

	// Topic names are passed explicitly: with no names, these requests cover internal topics too.
	names := details.Names()
	startOffsets, err := source.admin.ListStartOffsets(ctx, names...)
	err = responseError(err, startOffsets)
	if err != nil {
		return nil, fmt.Errorf("list start offsets: %w", err)
	}
	endOffsets, err := source.admin.ListEndOffsets(ctx, names...)
	err = responseError(err, endOffsets)
	if err != nil {
		return nil, fmt.Errorf("list end offsets: %w", err)
	}
	latestOffsets, err := source.admin.ListMaxTimestampOffsets(ctx, names...)
	err = responseError(err, latestOffsets)
	if err != nil {
		return nil, fmt.Errorf("list max timestamp offsets: %w", err)
	}

	for name, detail := range details {
		topic := topicState{
			partitions:   len(detail.Partitions),
			startOffsets: make(map[int32]int64),
			endOffsets:   make(map[int32]int64),
		}
		for partition, offset := range startOffsets[name] {
			topic.startOffsets[partition] = offset.Offset
		}
		for partition, offset := range endOffsets[name] {
			topic.endOffsets[partition] = offset.Offset
		}
		for _, offset := range latestOffsets[name] {
			// An empty partition reports offset and timestamp -1.
			if offset.Offset < 0 || offset.Timestamp < 0 {
				continue
			}
			timestamp := time.UnixMilli(offset.Timestamp).UTC()
			if topic.latestRecord == nil || timestamp.After(*topic.latestRecord) {
				topic.latestRecord = &timestamp
			}
		}
		topics[name] = topic
	}

	return topics, nil
}

// fetchGroups describes consumer groups of both protocols and their committed offsets.
func (source *Source) fetchGroups(ctx context.Context) (map[string]groupState, error) {
	listed, err := source.admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("list groups: %w", err)
	}
	classicGroups, consumerGroups := selectGroups(listed)

	groups, err := source.describeClassicGroups(ctx, classicGroups)
	if err != nil {
		return nil, err
	}
	consumerStates, err := source.describeConsumerGroups(ctx, consumerGroups)
	if err != nil {
		return nil, err
	}
	maps.Copy(groups, consumerStates)

	committed, err := source.fetchCommittedOffsets(ctx, slices.Sorted(maps.Keys(groups)))
	if err != nil {
		return nil, err
	}
	for name, offsets := range committed {
		// A map holds struct values, not references: modify a copy and store it back.
		group := groups[name]
		group.committed = offsets
		groups[name] = group
	}

	return groups, nil
}

// selectGroups returns the names of the classic and the KIP-848 consumer groups, sorted.
// Kafka Connect, share and streams groups are skipped.
func selectGroups(listed kadm.ListedGroups) (classicGroups, consumerGroups []string) {
	for _, group := range listed.Sorted() {
		switch {
		case group.GroupType == groupTypeConsumer:
			consumerGroups = append(consumerGroups, group.Group)
		// GroupType is empty on brokers older than 3.8. An empty protocol type is a group that
		// only commits offsets (manual assignment, e.g. Spark or Flink).
		case (group.GroupType == groupTypeClassic || group.GroupType == "") &&
			(group.ProtocolType == protocolTypeConsumer || group.ProtocolType == ""):
			classicGroups = append(classicGroups, group.Group)
		}
	}

	return classicGroups, consumerGroups
}

func (source *Source) describeClassicGroups(ctx context.Context, names []string) (map[string]groupState, error) {
	groups := make(map[string]groupState, len(names))
	// With no names, DescribeGroups describes every group in the cluster.
	if len(names) == 0 {
		return groups, nil
	}
	described, err := source.admin.DescribeGroups(ctx, names...)
	err = responseError(err, described)
	if err != nil {
		return nil, fmt.Errorf("describe groups: %w", err)
	}
	for name, group := range described {
		state := groupState{phase: group.State}
		for _, member := range group.Members {
			assignment := make(map[string][]int32)
			consumerAssignment, isConsumer := member.Assigned.AsConsumer()
			if isConsumer {
				for _, topic := range consumerAssignment.Topics {
					assignment[topic.Topic] = topic.Partitions
				}
			}
			state.members = append(state.members, memberState{
				clientID:   member.ClientID,
				host:       memberHost(member.ClientHost),
				assignment: assignment,
			})
		}
		groups[name] = state
	}

	return groups, nil
}

func (source *Source) describeConsumerGroups(ctx context.Context, names []string) (map[string]groupState, error) {
	groups := make(map[string]groupState, len(names))
	if len(names) == 0 {
		return groups, nil
	}
	described, err := source.admin.DescribeConsumerGroups(ctx, names...)
	err = responseError(err, described)
	if err != nil {
		return nil, fmt.Errorf("describe consumer groups: %w", err)
	}
	for name, group := range described {
		state := groupState{phase: group.State}
		for _, member := range group.Members {
			assignment := make(map[string][]int32)
			for _, topic := range member.Assignment.Sorted() {
				assignment[topic.Topic] = topic.Partitions
			}
			state.members = append(state.members, memberState{
				clientID:   member.ClientID,
				host:       memberHost(member.ClientHost),
				assignment: assignment,
			})
		}
		groups[name] = state
	}

	return groups, nil
}

// fetchCommittedOffsets returns, for every group, topic → partition → committed offset.
func (source *Source) fetchCommittedOffsets(
	ctx context.Context,
	groupNames []string,
) (map[string]map[string]map[int32]int64, error) {
	result := make(map[string]map[string]map[int32]int64, len(groupNames))
	if len(groupNames) == 0 {
		return result, nil
	}
	fetched := source.admin.FetchManyOffsets(ctx, groupNames...)
	err := fetched.Error()
	if err != nil {
		return nil, fmt.Errorf("fetch committed offsets: %w", err)
	}
	for name, response := range fetched {
		committed := make(map[string]map[int32]int64)
		for topic, partitions := range response.Fetched {
			for partition, offset := range partitions {
				if offset.At < 0 {
					continue
				}
				if committed[topic] == nil {
					committed[topic] = make(map[int32]int64)
				}
				committed[topic][partition] = offset.At
			}
		}
		result[name] = committed
	}

	return result, nil
}
