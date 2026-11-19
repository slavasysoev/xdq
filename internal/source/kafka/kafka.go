// Package kafka discovers topics, consumer groups, members and offsets of a Kafka cluster.
// It uses the Admin API only: it never joins a group, commits offsets, produces or reads records.
package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
)

// Source is a Kafka cluster.
type Source struct {
	name  string
	admin *kadm.Client
}

// New creates a source. It does not connect: franz-go connects lazily on the first request.
func New(name string, settings config.KafkaConfig) (*Source, error) {
	options, err := clientOptions(settings)
	if err != nil {
		return nil, fmt.Errorf("kafka source %s: %w", name, err)
	}
	client, err := kgo.NewClient(options...)
	if err != nil {
		return nil, fmt.Errorf("kafka source %s: %w", name, err)
	}

	return &Source{name: name, admin: kadm.NewClient(client)}, nil
}

// Name returns the name of the source in the config.
func (source *Source) Name() string {
	return source.name
}

// Type returns config.TypeKafka.
func (source *Source) Type() string {
	return config.TypeKafka
}

// Discover reads the current state of the cluster. Any failed request fails the whole discovery.
func (source *Source) Discover(ctx context.Context) (graph.Snapshot, error) {
	state, err := source.fetch(ctx)
	if err != nil {
		return graph.Snapshot{}, fmt.Errorf("kafka source %s: %w", source.name, err)
	}

	return buildSnapshot(source.name, state), nil
}

// Close closes the connections to the brokers.
func (source *Source) Close() error {
	source.admin.Close()

	return nil
}
