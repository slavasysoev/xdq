package tui

import (
	"fmt"
	"math"
	"strings"

	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"

	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/style"
)

var (
	titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("212"))
	hintStyle  = lipgloss.NewStyle().Faint(true)
)

// frameLines is the number of lines around the body: two header lines, an empty line, and an
// empty line with the key hints below the body.
const frameLines = 5

var keyHints = map[screen]string{
	screenChains: "↑↓ move · enter open · r refresh · q quit",
	screenChain:  "↑↓ move · enter details · esc back · r refresh · q quit",
	screenNode:   "↑↓ scroll · esc back · r refresh · q quit",
}

// View implements tea.Model.
func (model Model) View() tea.View {
	lines := model.header()
	lines = append(lines, "")
	switch {
	case model.result == nil:
	case model.screen == screenChains:
		lines = append(lines, model.chainList()...)
	case model.screen == screenChain:
		lines = append(lines, model.chainView()...)
	case model.screen == screenNode:
		lines = append(lines, model.nodeView()...)
	}
	lines = append(lines, "", hintStyle.Render(keyHints[model.screen]))

	content := strings.Join(lines, "\n")
	if model.width > 0 {
		content = lipgloss.NewStyle().MaxWidth(model.width).Render(content)
	}
	view := tea.NewView(content)
	view.AltScreen = true

	return view
}

// header is the title line with the overall status and the line with the sources.
func (model Model) header() []string {
	title := titleStyle.Render("xdq " + model.version)
	if model.result == nil {
		return []string{title + "  checking…", ""}
	}
	environment := model.result.Environment
	if environment == "" {
		environment = "all environments"
	}
	title += "  env " + environment + "  " + statusLabel(model.result.Status)
	if model.checking {
		title += "  checking…"
	} else {
		title += "  checked " + model.checkedAt.Format("15:04:05")
	}

	var sources []string
	for _, result := range model.result.Sources {
		source := result.Name + " " + style.Status(result.Status, string(result.Status))
		if result.Error != "" {
			source += " " + result.Error
		}
		sources = append(sources, source)
	}

	return []string{title, "sources: " + strings.Join(sources, " · ")}
}

func (model Model) chainList() []string {
	if len(model.chains) == 0 {
		return []string{"no chains"}
	}
	nameWidth := 0
	for _, chain := range model.chains {
		nameWidth = max(nameWidth, len(chain.Name))
	}
	lines := make([]string, 0, len(model.chains))
	for index, chain := range model.chains {
		summary := findingSummary(report.FindingsInChain(chain, model.result.Findings))
		line := fmt.Sprintf("%s  %-*s  %s", statusLabel(chain.Status), nameWidth, chain.Name, summary)
		lines = append(lines, cursorMark(index == model.chainCursor)+line)
	}

	return visible(lines, model.chainCursor, model.bodyHeight())
}

// chainView is the chain title, the findings about the chain itself and the tree of its nodes.
func (model Model) chainView() []string {
	chain := model.chains[model.chainCursor]
	lines := []string{statusLabel(chain.Status) + "  " + titleStyle.Render(chain.Name)}
	for _, finding := range model.result.Findings {
		if finding.Chain == chain.Name {
			message, _, _ := strings.Cut(finding.Message, "\n")
			lines = append(lines, "  "+statusLabel(finding.Severity.Status())+"  "+finding.Rule+"  "+message)
		}
	}
	lines = append(lines, "")
	if len(model.tree) == 0 {
		return append(lines, "no nodes")
	}

	tree := make([]string, len(model.tree))
	for index, line := range model.tree {
		tree[index] = cursorMark(index == model.nodeCursor) + renderTreeLine(line)
	}

	return append(lines, visible(tree, model.nodeCursor, max(model.bodyHeight()-len(lines), 1))...)
}

func (model Model) nodeView() []string {
	lines := model.nodeDetails()
	height := model.bodyHeight()
	start := min(model.detailOffset, len(lines))
	end := min(start+height, len(lines))

	return lines[start:end]
}

func renderTreeLine(line treeLine) string {
	text := ""
	if line.prefix != "" {
		text = hintStyle.Render(line.prefix)
	}
	if line.edge != "" {
		text += hintStyle.Render(string(line.edge)) + " "
	}
	text += style.Status(line.node.Status, "●") + " " + string(line.node.ID)
	if line.node.Status != report.StatusOK {
		text += "  " + style.Status(line.node.Status, strings.ToUpper(string(line.node.Status)))
	}
	if line.node.Gap {
		text += hintStyle.Render("  gap")
	}
	if line.repeated {
		text += hintStyle.Render("  ↺ shown above")
	}

	return text
}

// findingSummary counts the findings by severity, e.g. "2 errors, 1 warning".
func findingSummary(findings []report.Finding) string {
	errorCount, warningCount, ignoredCount := 0, 0, 0
	for _, finding := range findings {
		if finding.Ignored {
			ignoredCount++
		} else if finding.Severity == report.SeverityError {
			errorCount++
		} else {
			warningCount++
		}
	}
	var parts []string
	if errorCount > 0 {
		parts = append(parts, plural(errorCount, "error"))
	}
	if warningCount > 0 {
		parts = append(parts, plural(warningCount, "warning"))
	}
	if len(parts) == 0 {
		parts = append(parts, "no findings")
	}
	if ignoredCount > 0 {
		parts = append(parts, fmt.Sprintf("%d ignored", ignoredCount))
	}

	return strings.Join(parts, ", ")
}

func plural(count int, noun string) string {
	if count == 1 {
		return "1 " + noun
	}

	return fmt.Sprintf("%d %ss", count, noun)
}

func cursorMark(selected bool) string {
	if selected {
		return "> "
	}

	return "  "
}

// bodyHeight is the number of lines left for the body. Before the first WindowSizeMsg the size is
// unknown, and nothing is cut.
func (model Model) bodyHeight() int {
	if model.height == 0 {
		return math.MaxInt32
	}

	return max(model.height-frameLines, 1)
}

// visible returns the lines that fit in height, scrolled so that the focused line is in the middle
// when the lines do not fit.
func visible(lines []string, focus, height int) []string {
	if height <= 0 || len(lines) <= height {
		return lines
	}
	start := clamp(focus-height/2, len(lines)-height+1)

	return lines[start : start+height]
}
