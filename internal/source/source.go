// Package source defines the contract between the engine and the adapters to external systems.
package source

import (
	"context"

	"github.com/slavasysoev/xdq/internal/graph"
)

// Source discovers facts from one system. It is opened once, and Discover may be called many
// times, reusing connections. Implementations are read-only and never cause side effects
// in the system they observe.
//
// Discover must return soon after ctx is done: the engine relies on it to enforce timeouts.
type Source interface {
	// Name is the name of the source in the config.
	Name() string
	// Type is the kind of system, e.g. "kafka".
	Type() string
	Discover(ctx context.Context) (graph.Snapshot, error)
	Close() error
}
