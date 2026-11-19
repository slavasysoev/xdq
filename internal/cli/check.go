package cli

import (
	"context"
	"fmt"
	"io"
	"os"

	"github.com/charmbracelet/colorprofile"
	"github.com/spf13/cobra"

	"github.com/slavasysoev/xdq/internal/report"
)

type checkOptions struct {
	runOptions

	chains []string
	output string
}

func newCheckCommand() *cobra.Command {
	var options checkOptions
	command := &cobra.Command{
		Use:   "check",
		Short: "Discover the sources and print the status of every chain",
		Long: "Discover the sources and print the status of every chain. When a lockfile exists, the chains\n" +
			"are also compared with it.\n\n" +
			"Exit codes: 0 no error findings, 1 at least one error finding, " +
			"2 xdq could not complete (invalid config or an unavailable source).",
		Args: cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runCheck(cmd.Context(), cmd.OutOrStdout(), options)
		},
	}
	addRunFlags(command, &options.runOptions)
	flags := command.Flags()
	flags.StringArrayVar(&options.chains, "chain", nil, "chain to check, repeatable; default all configured chains")
	flags.StringVarP(&options.output, "output", "o", outputText, "output format: text or json")

	return command
}

func runCheck(ctx context.Context, stdout io.Writer, options checkOptions) error {
	if options.output != outputText && options.output != outputJSON {
		return fmt.Errorf("invalid --output %q: want %s or %s", options.output, outputText, outputJSON)
	}
	checkPlan, err := newPlan(options.runOptions, options.chains)
	if err != nil {
		return err
	}
	locked, err := checkPlan.readLock()
	if err != nil {
		return err
	}
	result, err := checkPlan.run(ctx, locked)
	if err != nil {
		return err
	}

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
