package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/charmbracelet/colorprofile"
	"github.com/spf13/cobra"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/engine"
	"github.com/slavasysoev/xdq/internal/report"
)

type checkOptions struct {
	configPath string
	output     string
	timeout    time.Duration
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
	sources, err := openSources(configuration)
	if err != nil {
		return err
	}
	defer func() {
		err = errors.Join(err, closeSources(sources))
	}()

	result := engine.New(sources, options.timeout).Run(ctx, "")

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
