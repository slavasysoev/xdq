package cli

import (
	"cmp"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strings"

	"charm.land/lipgloss/v2"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// Output formats of xdq check.
const (
	outputText = "text"
	outputJSON = "json"
)

// statusWidth is the width of the status column: the longest status is "warning".
const statusWidth = 7

// findingIndent aligns the lines under a finding with its rule name: two spaces, the status
// column and two more spaces come first.
var findingIndent = strings.Repeat(" ", 2+statusWidth+2)

var statusStyles = map[report.Status]lipgloss.Style{
	report.StatusOK:      lipgloss.NewStyle().Foreground(lipgloss.Color("10")),
	report.StatusUnknown: lipgloss.NewStyle().Foreground(lipgloss.Color("8")),
	report.StatusWarning: lipgloss.NewStyle().Foreground(lipgloss.Color("11")),
	report.StatusError:   lipgloss.NewStyle().Foreground(lipgloss.Color("9")).Bold(true),
}

func writeJSON(writer io.Writer, result *report.Report) error {
	encoder := json.NewEncoder(writer)
	encoder.SetIndent("", "  ")

	return encoder.Encode(result)
}

// writeText prints the sources, then every chain with its findings. It always writes colors;
// the caller's writer strips them when the output is not a terminal.
func writeText(writer io.Writer, result *report.Report) error {
	var text strings.Builder
	writeSources(&text, result.Sources)

	findings := slices.Clone(result.Findings)
	slices.SortStableFunc(findings, func(left, right report.Finding) int {
		return cmp.Or(right.Severity.Compare(left.Severity), cmp.Compare(left.Rule, right.Rule))
	})
	for _, chain := range result.Chains {
		text.WriteString("\n")
		_, err := fmt.Fprintf(&text, "%s  %s\n\n", chain.Name, styleStatus(chain.Status, strings.ToUpper(string(chain.Status))))
		if err != nil {
			return err
		}
		chainFindings := findingsInChain(chain, findings)
		if len(chainFindings) == 0 {
			text.WriteString("  no findings\n")
		}
		for index, finding := range chainFindings {
			if index > 0 {
				text.WriteString("\n")
			}
			writeFinding(&text, finding)
		}
		writeGaps(&text, chain)
	}

	_, err := io.WriteString(writer, text.String())

	return err
}

func writeSources(text *strings.Builder, sources []report.SourceResult) {
	nameWidth, typeWidth := 0, 0
	for _, result := range sources {
		nameWidth = max(nameWidth, len(result.Name))
		typeWidth = max(typeWidth, len(result.Type))
	}
	for _, result := range sources {
		// Pad before styling: escape codes would break the column width.
		status := styleStatus(result.Status, fmt.Sprintf("%-*s", statusWidth, result.Status))
		line := fmt.Sprintf("%-*s  %-*s  %s  %6s", nameWidth, result.Name, typeWidth, result.Type, status, formatDuration(result.DurationMs))
		if result.Error != "" {
			line += "  " + result.Error
		}
		text.WriteString(line + "\n")
	}
}

func writeFinding(text *strings.Builder, finding report.Finding) {
	severity := styleStatus(finding.Severity.Status(), fmt.Sprintf("%-*s", statusWidth, strings.ToUpper(string(finding.Severity))))
	subjects := make([]string, len(finding.Subjects))
	for index, subject := range finding.Subjects {
		subjects[index] = string(subject)
	}
	header := "  " + severity + "  " + finding.Rule
	if len(subjects) > 0 {
		header += "  " + strings.Join(subjects, ", ")
	}
	text.WriteString(header + "\n")
	for line := range strings.Lines(finding.Message) {
		text.WriteString(findingIndent + strings.TrimSuffix(line, "\n") + "\n")
	}
	for _, evidence := range finding.Evidence {
		text.WriteString(findingIndent + evidence.Source + ": " + evidence.Detail + "\n")
	}
}

// writeGaps lists the workloads where the chain stops. They are information, not findings.
func writeGaps(text *strings.Builder, chain report.Chain) {
	var gaps []string
	for _, node := range chain.Nodes {
		if node.Gap {
			gaps = append(gaps, string(node.ID))
		}
	}
	if len(gaps) == 0 {
		return
	}
	text.WriteString("\n")
	for _, gap := range gaps {
		_, err := fmt.Fprintf(text, "  %-*s  %s\n", statusWidth, "gap", gap)
		if err != nil {
			return
		}
		text.WriteString(findingIndent + "nothing discovered downstream: the end of the chain or an undiscovered producer\n")
	}
}

// findingsInChain returns the findings about the chain itself or with a subject in it.
func findingsInChain(chain report.Chain, findings []report.Finding) []report.Finding {
	inChain := make(map[graph.NodeID]bool, len(chain.Nodes))
	for _, node := range chain.Nodes {
		inChain[node.ID] = true
	}
	var result []report.Finding
	for _, finding := range findings {
		if finding.Chain == chain.Name || slices.ContainsFunc(finding.Subjects, func(subject graph.NodeID) bool { return inChain[subject] }) {
			result = append(result, finding)
		}
	}

	return result
}

func styleStatus(status report.Status, text string) string {
	return statusStyles[status].Render(text)
}

// formatDuration prints milliseconds as "120ms" below a second and "10.0s" above.
func formatDuration(milliseconds int64) string {
	if milliseconds < 1000 {
		return fmt.Sprintf("%dms", milliseconds)
	}

	return fmt.Sprintf("%.1fs", float64(milliseconds)/1000)
}
