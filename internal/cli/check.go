package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"slices"
	"strings"
	"time"

	"github.com/charmbracelet/colorprofile"
	"github.com/spf13/cobra"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/engine"
	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/rule"
)

type checkOptions struct {
	configPath  string
	environment string
	chains      []string
	output      string
	timeout     time.Duration
}

func newCheckCommand() *cobra.Command {
	var options checkOptions
	command := &cobra.Command{
		Use:   "check",
		Short: "Discover the sources and print the status of every chain",
		Long: "Discover the sources and print the status of every chain.\n\n" +
			"Exit codes: 0 no error findings, 1 at least one error finding, " +
			"2 xdq could not complete (invalid config or an unavailable source).",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCheck(cmd.Context(), cmd.OutOrStdout(), options)
		},
	}
	flags := command.Flags()
	flags.StringVar(&options.configPath, "config", "xdq.yaml", "path to the config file")
	flags.StringVar(&options.environment, "env", "", "environment to check; empty means every environment")
	flags.StringArrayVar(&options.chains, "chain", nil, "chain to check, repeatable; default all configured chains")
	flags.StringVarP(&options.output, "output", "o", outputText, "output format: text or json")
	flags.DurationVar(&options.timeout, "timeout", 10*time.Second, "timeout of each source")

	return command
}

// runCheck has a named result, so the deferred Close can add its error to the one returned.
func runCheck(ctx context.Context, stdout io.Writer, options checkOptions) (err error) {
	if options.output != outputText && options.output != outputJSON {
		return fmt.Errorf("invalid --output %q: want %s or %s", options.output, outputText, outputJSON)
	}
	if options.timeout <= 0 {
		return fmt.Errorf("invalid --timeout %s: must be positive", options.timeout)
	}
	configuration, err := config.Load(options.configPath)
	if err != nil {
		return err
	}
	// Rules are validated before any source is contacted: a typo in a rule name fails fast.
	rules, err := rule.NewSet(configuration.Rules)
	if err != nil {
		return fmt.Errorf("config %s:\n%w", options.configPath, err)
	}
	chains, err := selectChains(configuration.Chains, options.chains)
	if err != nil {
		return err
	}
	sources, err := openSources(configuration)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, closeSources(sources))
	}()

	result := engine.New(sources, rules, chains, options.timeout).Run(ctx, options.environment)

	if options.output == outputJSON {
		err = writeJSON(stdout, &result)
	} else {
		err = writeText(colorprofile.NewWriter(stdout, os.Environ()), &result)
	}
	if err != nil {
		return err
	}
	code := checkExitCode(&result)
	if code != exitCodeOK {
		return &ExitError{Code: code}
	}

	return nil
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

// checkExitCode returns the exit code of a finished check. An incomplete run wins over error
// findings: access to the sources has to be fixed before the result can be trusted.
func checkExitCode(result *report.Report) int {
	if !result.IsComplete() {
		return exitCodeIncomplete
	}
	if result.HasErrors() {
		return exitCodeErrors
	}

	return exitCodeOK
}
