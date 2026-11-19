// Package tui is the interactive terminal UI. It only renders data, it never discovers anything itself.
package tui

import (
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
)

var (
	titleStyle = lipgloss.NewStyle().Bold(true).Foreground(lipgloss.Color("212"))
	hintStyle  = lipgloss.NewStyle().Faint(true)
)

// Model is the root Bubble Tea model.
type Model struct {
	version string
}

// New creates the root model.
func New(version string) Model {
	return Model{version: version}
}

// Init implements tea.Model.
func (model Model) Init() tea.Cmd {
	return nil
}

// Update implements tea.Model.
func (model Model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	key, isKey := message.(tea.KeyPressMsg)
	if !isKey {
		return model, nil
	}

	switch key.String() {
	case "q", "esc", "ctrl+c":
		return model, tea.Quit
	}

	return model, nil
}

// View implements tea.Model.
func (model Model) View() tea.View {
	content := titleStyle.Render("xdq "+model.version) + "\n\n" + hintStyle.Render("q: quit") + "\n"

	return tea.NewView(content)
}
