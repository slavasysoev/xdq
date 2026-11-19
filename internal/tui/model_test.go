package tui

import (
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
)

func TestQuitKeys(t *testing.T) {
	tests := []struct {
		name string
		key  tea.KeyPressMsg
	}{
		{name: "q", key: tea.KeyPressMsg{Code: 'q', Text: "q"}},
		{name: "esc", key: tea.KeyPressMsg{Code: tea.KeyEscape}},
		{name: "ctrl+c", key: tea.KeyPressMsg{Code: 'c', Mod: tea.ModCtrl}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, command := New("v1.2.3").Update(test.key)
			if command == nil {
				t.Fatal("expected a quit command, got nil")
			}
			if _, isQuit := command().(tea.QuitMsg); !isQuit {
				t.Errorf("expected tea.QuitMsg, got %T", command())
			}
		})
	}
}

func TestOtherKeyDoesNotQuit(t *testing.T) {
	_, command := New("v1.2.3").Update(tea.KeyPressMsg{Code: 'x', Text: "x"})
	if command != nil {
		t.Errorf("expected no command, got %T", command())
	}
}

func TestViewShowsVersion(t *testing.T) {
	view := New("v1.2.3").View()
	if !strings.Contains(view.Content, "v1.2.3") {
		t.Errorf("view does not contain version: %q", view.Content)
	}
}
