package cli

import (
	"errors"
	"fmt"
	"io"

	"github.com/charmbracelet/fang"
)

// Exit codes of xdq check (§12). exitCodeIncomplete is also the code of any failure before
// a report exists, e.g. an invalid config.
const (
	exitCodeOK         = 0
	exitCodeErrors     = 1
	exitCodeIncomplete = 2
)

// ExitError ends a command with a specific exit code. The command has already printed
// everything the user needs, so the error itself is not printed.
type ExitError struct {
	Code int
}

func (exitError *ExitError) Error() string {
	return fmt.Sprintf("exit code %d", exitError.Code)
}

// ExitCode returns the process exit code for an error returned by the command tree.
func ExitCode(err error) int {
	var exitError *ExitError
	if errors.As(err, &exitError) {
		return exitError.Code
	}

	return exitCodeIncomplete
}

// HandleError prints errors in the fang style, except ExitError, which carries only a code.
func HandleError(writer io.Writer, styles fang.Styles, err error) {
	var exitError *ExitError
	if errors.As(err, &exitError) {
		return
	}
	fang.DefaultErrorHandler(writer, styles, err)
}
