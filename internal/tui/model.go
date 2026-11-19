// Package tui is the interactive terminal UI. It only renders reports, it never discovers anything
// itself: the check that produces them is given to it.
package tui

import (
	"slices"
	"time"

	tea "charm.land/bubbletea/v2"

	"github.com/slavasysoev/xdq/internal/report"
)

// CheckFunc runs one check and blocks until the report is ready. The TUI calls it in the
// background, so the screen stays responsive.
type CheckFunc func() report.Report

// screen is one level of the navigation: Enter goes one level deeper, Esc one level back.
type screen int

const (
	screenChains screen = iota
	screenChain
	screenNode
)

// reportMsg delivers the report of a finished check.
type reportMsg struct {
	result    report.Report
	checkedAt time.Time
}

// Model is the root Bubble Tea model. Bubble Tea passes it by value: Update returns the changed
// copy.
type Model struct {
	version string
	check   CheckFunc

	// result is nil until the first check finished.
	result *report.Report
	// chains are the chains of the result, the worst first.
	chains    []report.Chain
	checkedAt time.Time
	checking  bool

	screen       screen
	chainCursor  int
	tree         []treeLine
	nodeCursor   int
	detailOffset int

	width, height int
}

// New creates the root model. The first check starts with Init.
func New(version string, check CheckFunc) Model {
	return Model{version: version, check: check, checking: true}
}

// Init implements tea.Model.
func (model Model) Init() tea.Cmd {
	return model.checkCommand()
}

// Update implements tea.Model.
func (model Model) Update(message tea.Msg) (tea.Model, tea.Cmd) {
	switch message := message.(type) {
	case tea.WindowSizeMsg:
		model.width, model.height = message.Width, message.Height
	case reportMsg:
		model = model.showReport(message)
	case tea.KeyPressMsg:
		return model.handleKey(message.String())
	}

	return model, nil
}

func (model Model) checkCommand() tea.Cmd {
	check := model.check

	return func() tea.Msg {
		return reportMsg{result: check(), checkedAt: time.Now()}
	}
}

func (model Model) handleKey(key string) (tea.Model, tea.Cmd) {
	switch key {
	case "q", "ctrl+c":
		return model, tea.Quit
	case "r":
		if model.checking {
			return model, nil
		}
		model.checking = true

		return model, model.checkCommand()
	case "esc", "backspace", "left", "h":
		if model.screen == screenChains {
			if key == "esc" {
				return model, tea.Quit
			}

			return model, nil
		}
		model.screen--
	case "up", "k":
		model = model.move(-1)
	case "down", "j":
		model = model.move(1)
	case "enter", "right", "l":
		model = model.open()
	}

	return model, nil
}

func (model Model) move(step int) Model {
	switch model.screen {
	case screenChains:
		model.chainCursor = clamp(model.chainCursor+step, len(model.chains))
	case screenChain:
		model.nodeCursor = clamp(model.nodeCursor+step, len(model.tree))
	case screenNode:
		lines := len(model.nodeDetails())
		model.detailOffset = clamp(model.detailOffset+step, lines-model.bodyHeight()+1)
	}

	return model
}

func (model Model) open() Model {
	switch model.screen {
	case screenChains:
		if len(model.chains) == 0 {
			return model
		}
		model.tree = buildTree(model.chains[model.chainCursor])
		model.nodeCursor = 0
		model.screen = screenChain
	case screenChain:
		if len(model.tree) == 0 {
			return model
		}
		model.detailOffset = 0
		model.screen = screenNode
	case screenNode:
	}

	return model
}

// showReport replaces the report and keeps the selection: the same chain, the same position in
// the tree. When the chain is gone, the chain list is shown.
func (model Model) showReport(message reportMsg) Model {
	var selected string
	if model.chainCursor < len(model.chains) {
		selected = model.chains[model.chainCursor].Name
	}
	model.result = &message.result
	model.checkedAt = message.checkedAt
	model.checking = false
	model.chains = slices.Clone(message.result.Chains)
	slices.SortStableFunc(model.chains, func(left, right report.Chain) int {
		return right.Status.Compare(left.Status)
	})

	index := slices.IndexFunc(model.chains, func(chain report.Chain) bool { return chain.Name == selected })
	if index < 0 {
		model.chainCursor = 0
		model.screen = screenChains

		return model
	}
	model.chainCursor = index
	model.tree = buildTree(model.chains[index])
	model.nodeCursor = clamp(model.nodeCursor, len(model.tree))
	if len(model.tree) == 0 && model.screen == screenNode {
		model.screen = screenChain
	}

	return model
}

// nodeDetails describes the node under the cursor.
func (model Model) nodeDetails() []string {
	if model.nodeCursor >= len(model.tree) {
		return nil
	}

	return nodeDetails(model.chains[model.chainCursor], model.tree[model.nodeCursor].node, model.result.Findings)
}

// clamp keeps an index within [0, count).
func clamp(index, count int) int {
	return max(min(index, count-1), 0)
}
