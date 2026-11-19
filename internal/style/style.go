// Package style holds the terminal styles that the text output and the TUI share.
package style

import (
	"charm.land/lipgloss/v2"

	"github.com/slavasysoev/xdq/internal/report"
)

var statusStyles = map[report.Status]lipgloss.Style{
	report.StatusOK:      lipgloss.NewStyle().Foreground(lipgloss.Color("10")),
	report.StatusUnknown: lipgloss.NewStyle().Foreground(lipgloss.Color("8")),
	report.StatusWarning: lipgloss.NewStyle().Foreground(lipgloss.Color("11")),
	report.StatusError:   lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true),
}

// Status renders the text in the color of the status.
func Status(status report.Status, text string) string {
	return statusStyles[status].Render(text)
}
