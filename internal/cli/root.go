// Package cli defines the xdq command tree.
package cli

import (
	"context"
	"errors"
	"io"

	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"

	"github.com/slavasysoev/xdq/internal/engine"
	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/tui"
)

// NewRootCommand builds the xdq command tree. Running it without a subcommand starts the TUI.
func NewRootCommand(version string) *cobra.Command {
	var options runOptions
	root := &cobra.Command{
		Use:   "xdq",
		Short: "Check that event-driven chains are wired as when they last worked",
		Long: "Check that event-driven chains are wired as when they last worked.\n\n" +
			"Without a command, xdq checks every chain and shows the result in an interactive UI.",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runTUI(cmd.Context(), cmd.InOrStdin(), cmd.OutOrStdout(), version, options)
		},
	}
	addRunFlags(root, &options)
	root.AddCommand(newCheckCommand(), newLockCommand(), newVersionCommand(version))

	return root
}

// runTUI loads the config and opens the sources once; every refresh in the UI discovers the same
// sources again. A broken config fails before the UI starts.
func runTUI(ctx context.Context, stdin io.Reader, stdout io.Writer, version string, options runOptions) (err error) {
	tuiPlan, err := newPlan(options, nil)
	if err != nil {
		return err
	}
	locked, err := tuiPlan.readLock()
	if err != nil {
		return err
	}
	sources, err := openSources(tuiPlan.configuration)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, closeSources(sources))
	}()

	checker := engine.New(sources, tuiPlan.rules, tuiPlan.chains, locked, options.timeout)
	check := func() report.Report {
		return checker.Run(ctx, options.environment)
	}
	program := tea.NewProgram(tui.New(version, check), tea.WithContext(ctx), tea.WithInput(stdin), tea.WithOutput(stdout))
	_, err = program.Run()

	return err
}
