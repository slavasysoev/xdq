package cli

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/charmbracelet/colorprofile"
	"github.com/spf13/cobra"

	"github.com/slavasysoev/xdq/internal/lock"
	"github.com/slavasysoev/xdq/internal/report"
)

type lockOptions struct {
	runOptions

	force bool
}

func newLockCommand() *cobra.Command {
	var options lockOptions
	command := &cobra.Command{
		Use:   "lock",
		Short: "Discover the sources and write the chains to the lockfile",
		Long: "Discover the sources and write the structure of every chain to the lockfile, the known-good\n" +
			"state that xdq check compares with.\n\n" +
			"The lockfile is not written while there are error findings, unless --force, and never when a\n" +
			"source is unavailable. Exit codes: 0 written, 1 not written because of error findings, " +
			"2 xdq could not complete (invalid config or an unavailable source).",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runLock(cmd.Context(), cmd.OutOrStdout(), options)
		},
	}
	addRunFlags(command, &options.runOptions)
	command.Flags().BoolVar(&options.force, "force", false, "write the lockfile despite error findings")

	return command
}

func runLock(ctx context.Context, stdout io.Writer, options lockOptions) error {
	lockPlan, err := newPlan(options.runOptions, nil)
	if err != nil {
		return err
	}
	// The old lockfile is not read: it is being replaced, not compared with.
	result, err := lockPlan.run(ctx, nil)
	if err != nil {
		return err
	}
	path := lockPlan.lockPath()

	code, reason := lockRefusal(&result, options.force)
	if code != exitCodeOK {
		err = writeText(colorprofile.NewWriter(stdout, os.Environ()), &result)
		if err != nil {
			return err
		}
		_, err = fmt.Fprintf(stdout, "\n%s not written: %s\n", path, reason)
		if err != nil {
			return err
		}

		return &ExitError{Code: code}
	}

	locked := lock.New(options.environment, result.Chains)
	err = lock.Write(path, locked)
	if err != nil {
		return err
	}
	edges := 0
	for _, chain := range locked.Chains {
		edges += len(chain.Edges)
	}
	_, err = fmt.Fprintf(stdout, "wrote %s: %d chains, %d edges\n", path, len(locked.Chains), edges)

	return err
}

// lockRefusal returns the exit code and the reason when the run must not be locked. A lockfile is
// a known-good state: an incomplete graph never is, a graph with error findings only when forced.
func lockRefusal(result *report.Report, force bool) (int, string) {
	if !result.IsComplete() {
		return exitCodeIncomplete, "a source is unavailable, so the chains are incomplete"
	}
	if result.HasErrors() && !force {
		return exitCodeErrors, "there are error findings; fix them, or use --force to lock this state anyway"
	}

	return exitCodeOK, ""
}
