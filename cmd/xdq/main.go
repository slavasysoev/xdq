package main

import (
	"context"
	"os"

	"github.com/charmbracelet/fang"

	"github.com/slavasysoev/xdq/internal/cli"
)

// version is overwritten at build time:
// go build -ldflags "-X main.version=v0.1.0" ./cmd/xdq
var version = "dev"

func main() {
	err := fang.Execute(
		context.Background(),
		cli.NewRootCommand(version),
		fang.WithVersion(version),
		fang.WithErrorHandler(cli.HandleError),
		// Ctrl+C cancels the context: sources stop, and the report shows them as unknown.
		fang.WithNotifySignal(os.Interrupt),
	)
	if err != nil {
		os.Exit(cli.ExitCode(err))
	}
}
