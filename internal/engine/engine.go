// Package engine runs the check pipeline and returns a Report. It never prints, so the CLI
// and the future service mode share it.
package engine

import (
	"context"
	"slices"
	"sync"
	"time"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/resolve"
	"github.com/slavasysoev/xdq/internal/rule"
	"github.com/slavasysoev/xdq/internal/source"
)

// Engine has no global state and can run many times, reusing the open sources.
// It does not own the sources: the caller opens and closes them.
type Engine struct {
	sources []source.Source
	rules   *rule.Set
	chains  map[string]config.ChainConfig
	timeout time.Duration
}

// New creates an engine. Chains are the chains to check; none means the implicit chain.
// Timeout limits each source's Discover separately.
func New(sources []source.Source, rules *rule.Set, chains map[string]config.ChainConfig, timeout time.Duration) *Engine {
	return &Engine{sources: sources, rules: rules, chains: chains, timeout: timeout}
}

// discovery is the outcome of discovering one source.
type discovery struct {
	result   report.SourceResult
	snapshot graph.Snapshot
}

// Run discovers all sources in parallel and builds the report. A failing source does not stop
// the run: it is reported as unknown, and the other sources still contribute.
func (engine *Engine) Run(ctx context.Context, environment string) report.Report {
	discoveries := make([]discovery, len(engine.sources))
	var running sync.WaitGroup
	for index, discovered := range engine.sources {
		// Each goroutine writes only its own index, so the slice needs no lock.
		running.Go(func() {
			discoveries[index] = engine.discover(ctx, discovered)
		})
	}
	running.Wait()

	results := make([]report.SourceResult, len(discoveries))
	snapshots := make([]graph.Snapshot, len(discoveries))
	for index, discovered := range discoveries {
		results[index] = discovered.result
		snapshots[index] = discovered.snapshot
	}
	merged := resolve.Resolve(graph.Merge(snapshots...))
	findings := engine.rules.Check(rule.Input{Graph: merged, Sources: results, Now: time.Now()})
	complete := !slices.ContainsFunc(results, func(result report.SourceResult) bool { return result.Status != report.StatusOK })
	chains, chainFindings := extractChains(engine.chains, merged, environment, complete)
	findings = append(findingsInChains(findings, chains), chainFindings...)

	return report.New(environment, results, chains, findings)
}

func (engine *Engine) discover(ctx context.Context, discovered source.Source) discovery {
	ctx, cancel := context.WithTimeout(ctx, engine.timeout)
	defer cancel()

	started := time.Now()
	snapshot, err := discovered.Discover(ctx)
	result := report.SourceResult{
		Name:       discovered.Name(),
		Type:       discovered.Type(),
		Status:     report.StatusOK,
		DurationMs: time.Since(started).Milliseconds(),
	}
	if err != nil {
		result.Status = report.StatusUnknown
		result.Error = err.Error()

		return discovery{result: result}
	}

	return discovery{result: result, snapshot: snapshot}
}
