package cli

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"path/filepath"
	"slices"
	"strings"
	"time"

	"github.com/spf13/cobra"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/engine"
	"github.com/slavasysoev/xdq/internal/lock"
	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/rule"
)

// runOptions are the flags that check and lock share.
type runOptions struct {
	configPath  string
	environment string
	lockPath    string
	timeout     time.Duration
}

func addRunFlags(command *cobra.Command, options *runOptions) {
	flags := command.Flags()
	flags.StringVar(&options.configPath, "config", "xdq.yaml", "path to the config file")
	flags.StringVar(&options.environment, "env", "", "environment to check; empty means every environment")
	flags.StringVar(&options.lockPath, "lock", "", "path to the lockfile (default "+lock.DefaultName+" next to the config)")
	flags.DurationVar(&options.timeout, "timeout", 10*time.Second, "timeout of each source")
}

// plan is everything a run needs before any source is contacted. Building it first makes a typo
// in the config or the flags fail fast.
type plan struct {
	options       runOptions
	configuration *config.Config
	rules         *rule.Set
	chains        map[string]config.ChainConfig
}

// newPlan loads the config and selects the chains named by chainNames, all when there are none.
func newPlan(options runOptions, chainNames []string) (plan, error) {
	if options.timeout <= 0 {
		return plan{}, fmt.Errorf("invalid --timeout %s: must be positive", options.timeout)
	}
	configuration, err := config.Load(options.configPath)
	if err != nil {
		return plan{}, err
	}
	rules, err := rule.NewSet(configuration.Rules)
	if err != nil {
		return plan{}, fmt.Errorf("config %s:\n%w", options.configPath, err)
	}
	chains, err := selectChains(configuration.Chains, chainNames)
	if err != nil {
		return plan{}, err
	}

	return plan{options: options, configuration: configuration, rules: rules, chains: chains}, nil
}

// lockPath returns the --lock path, or the default one next to the config.
func (plan plan) lockPath() string {
	if plan.options.lockPath != "" {
		return plan.options.lockPath
	}

	return filepath.Join(filepath.Dir(plan.options.configPath), lock.DefaultName)
}

// readLock returns the lockfile to compare with, or nil when the default one does not exist. A
// lockfile named by --lock must exist. Configured chains that are not checked are left out, so
// only the chains that are gone from the config are reported as such.
func (plan plan) readLock() (*lock.File, error) {
	locked, err := lock.Read(plan.lockPath())
	if errors.Is(err, fs.ErrNotExist) && plan.options.lockPath == "" {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if locked.Environment != plan.options.environment {
		return nil, fmt.Errorf("lockfile %s is for environment %q, not %q", plan.lockPath(), locked.Environment, plan.options.environment)
	}
	maps.DeleteFunc(locked.Chains, func(name string, _ lock.Chain) bool {
		_, configured := plan.configuration.Chains[name]
		_, checked := plan.chains[name]

		return configured && !checked
	})

	return &locked, nil
}

// run opens the sources, runs the engine and closes the sources. It has a named result, so the
// deferred Close can add its error to the one returned.
func (plan plan) run(ctx context.Context, locked *lock.File) (result report.Report, err error) {
	sources, err := openSources(plan.configuration)
	if err != nil {
		return report.Report{}, err
	}
	defer func() {
		err = errors.Join(err, closeSources(sources))
	}()

	return engine.New(sources, plan.rules, plan.chains, locked, plan.options.timeout).Run(ctx, plan.options.environment), nil
}

// selectChains returns the configured chains named by --chain, or all of them when none is named.
func selectChains(configured map[string]config.ChainConfig, names []string) (map[string]config.ChainConfig, error) {
	if len(names) == 0 {
		return configured, nil
	}
	selected := make(map[string]config.ChainConfig, len(names))
	for _, name := range names {
		chain, found := configured[name]
		if !found {
			return nil, fmt.Errorf("invalid --chain %q: configured chains: %s", name, strings.Join(slices.Sorted(maps.Keys(configured)), ", "))
		}
		selected[name] = chain
	}

	return selected, nil
}
