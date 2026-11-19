// Package cli defines the xdq command tree.
package cli

import (
	tea "charm.land/bubbletea/v2"
	"github.com/spf13/cobra"

	"github.com/slavasysoev/xdq/internal/tui"
)

// NewRootCommand builds the xdq command tree. Running it without a subcommand starts the TUI.
func NewRootCommand(version string) *cobra.Command {
	root := &cobra.Command{
		Use:   "xdq",
		Short: "Check that event-driven chains are wired as when they last worked",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			program := tea.NewProgram(
				tui.New(version),
				tea.WithInput(cmd.InOrStdin()),
				tea.WithOutput(cmd.OutOrStdout()),
			)
			_, err := program.Run()

			return err
		},
	}
	root.AddCommand(newCheckCommand(), newVersionCommand(version))

	return root
}
