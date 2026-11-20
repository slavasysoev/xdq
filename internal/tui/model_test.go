package tui

import (
	"reflect"
	"strings"
	"testing"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

var (
	orders   = graph.NewTopic("main", "orders.created")
	billing  = graph.NewGroup("main", "billing")
	workload = graph.NewWorkload("test", "stand-a", graph.WorkloadDeployment, "billing")
	audit    = graph.NewGroup("main", "audit")
)

// chainOf builds a chain of the nodes, with an edge between every two neighbors.
func chainOf(name string, nodes ...graph.Node) report.Chain {
	snapshot := graph.NewSnapshot()
	snapshot.Nodes = nodes
	kinds := map[graph.NodeKind]graph.EdgeKind{graph.NodeGroup: graph.EdgeConsumer, graph.NodeWorkload: graph.EdgeMembership}
	for index := 1; index < len(nodes); index++ {
		snapshot.Edges = append(snapshot.Edges, graph.Edge{
			From: nodes[index-1].ID, To: nodes[index].ID, Kind: kinds[nodes[index].Kind],
			Evidence: []graph.Evidence{{Source: "main", Detail: "assigned partitions 0,1"}},
		})
	}
	snapshot.Observations.Groups[billing.ID] = graph.GroupObservation{State: "Stable"}
	snapshot.Sort()

	return report.NewChain(name, graph.Subgraph{Snapshot: snapshot})
}

// sampleReport has a green chain first and a red one second: the TUI shows the red one first.
func sampleReport(chainNames ...string) report.Report {
	all := map[string]report.Chain{
		"audit":      chainOf("audit", orders, audit),
		"order-flow": chainOf("order-flow", orders, billing, workload),
	}
	var chains []report.Chain
	for _, name := range chainNames {
		chains = append(chains, all[name])
	}
	findings := []report.Finding{{
		Rule: "group-spans-environments", Severity: report.SeverityError, Subjects: []graph.NodeID{billing.ID},
		Message: "members from 2 environments", Evidence: []graph.Evidence{{Source: "test", Detail: "stand-b  deployment/billing"}},
	}}
	sources := []report.SourceResult{{Name: "main", Type: "kafka", Status: report.StatusOK}}

	return report.New("stand-a", sources, chains, findings)
}

func checking(result report.Report) CheckFunc {
	return func() report.Report { return result }
}

// started is a model that received the report of its first check.
func started(t *testing.T, result report.Report) Model {
	t.Helper()
	model := New("v1.2.3", checking(result))
	message := model.Init()()

	return update(t, model, message)
}

func update(t *testing.T, model Model, message tea.Msg) Model {
	t.Helper()
	next, _ := model.Update(message)

	return next.(Model)
}

var keys = map[string]tea.KeyPressMsg{
	"enter": {Code: tea.KeyEnter},
	"esc":   {Code: tea.KeyEscape},
	"down":  {Code: tea.KeyDown},
	"up":    {Code: tea.KeyUp},
	"r":     {Code: 'r', Text: "r"},
	"q":     {Code: 'q', Text: "q"},
}

func press(t *testing.T, model Model, names ...string) Model {
	t.Helper()
	for _, name := range names {
		model = update(t, model, keys[name])
	}

	return model
}

func TestQuitKeys(t *testing.T) {
	for _, key := range []tea.KeyPressMsg{keys["q"], keys["esc"], {Code: 'c', Mod: tea.ModCtrl}} {
		t.Run(key.String(), func(t *testing.T) {
			_, command := started(t, sampleReport("audit")).Update(key)
			if command == nil {
				t.Fatal("expected a quit command, got nil")
			}
			if _, isQuit := command().(tea.QuitMsg); !isQuit {
				t.Errorf("expected tea.QuitMsg, got %T", command())
			}
		})
	}
}

func TestBeforeFirstReport(t *testing.T) {
	view := plain(New("v1.2.3", checking(sampleReport())).View())
	if !strings.Contains(view, "xdq v1.2.3") || !strings.Contains(view, "checking…") {
		t.Errorf("view =\n%s", view)
	}
}

func TestChainListWorstFirst(t *testing.T) {
	view := plain(started(t, sampleReport("audit", "order-flow")).View())

	orderFlow := strings.Index(view, "order-flow")
	auditChain := strings.Index(view, "audit ")
	if orderFlow < 0 || auditChain < 0 || orderFlow > auditChain {
		t.Errorf("order-flow (error) must be listed before audit (ok):\n%s", view)
	}
	for _, want := range []string{"env stand-a", "sources: main ok", "1 error", "no findings", "> "} {
		if !strings.Contains(view, want) {
			t.Errorf("view does not contain %q:\n%s", want, view)
		}
	}
}

func TestNavigation(t *testing.T) {
	model := started(t, sampleReport("audit", "order-flow"))

	model = press(t, model, "enter")
	if model.screen != screenChain || !strings.Contains(plain(model.View()), "consumer ● group:main/billing  ERROR") {
		t.Fatalf("chain view =\n%s", plain(model.View()))
	}

	model = press(t, model, "down", "enter")
	details := plain(model.View())
	for _, want := range []string{"group-spans-environments", "test: stand-b  deployment/billing", "state Stable", "← consumer from topic:main/orders.created", "main: assigned partitions 0,1"} {
		if !strings.Contains(details, want) {
			t.Errorf("details do not contain %q:\n%s", want, details)
		}
	}

	model = press(t, model, "esc", "esc")
	if model.screen != screenChains {
		t.Errorf("screen = %d after two esc, want the chain list", model.screen)
	}
}

func TestRefresh(t *testing.T) {
	model := press(t, started(t, sampleReport("audit", "order-flow")), "enter", "down", "enter")

	next, command := model.Update(keys["r"])
	model = next.(Model)
	if command == nil || !model.checking || !strings.Contains(plain(model.View()), "checking…") {
		t.Fatal("r must start a check and show it")
	}
	if _, second := model.Update(keys["r"]); second != nil {
		t.Error("r during a check must not start another one")
	}

	model = update(t, model, command())
	if model.screen != screenNode || model.chains[model.chainCursor].Name != "order-flow" || model.nodeCursor != 1 {
		t.Errorf("selection lost after refresh: screen %d, chain %d, node %d", model.screen, model.chainCursor, model.nodeCursor)
	}

	model = update(t, model, reportMsg{result: sampleReport("audit")})
	if model.screen != screenChains {
		t.Errorf("screen = %d, want the chain list when the open chain is gone", model.screen)
	}
}

func TestScrolling(t *testing.T) {
	model := started(t, sampleReport("audit", "order-flow"))
	model = update(t, model, tea.WindowSizeMsg{Width: 80, Height: frameLines + 1})

	view := plain(model.View())
	if strings.Contains(view, "audit") {
		t.Errorf("only the selected chain fits:\n%s", view)
	}
	model = press(t, model, "down")
	if view = plain(model.View()); !strings.Contains(view, "audit") || strings.Contains(view, "order-flow") {
		t.Errorf("the list must scroll to the selected chain:\n%s", view)
	}
}

func TestVisible(t *testing.T) {
	lines := []string{"0", "1", "2", "3", "4", "5"}
	tests := []struct {
		focus, height int
		want          []string
	}{
		{focus: 0, height: 10, want: lines},
		{focus: 0, height: 3, want: []string{"0", "1", "2"}},
		{focus: 3, height: 3, want: []string{"2", "3", "4"}},
		{focus: 5, height: 3, want: []string{"3", "4", "5"}},
	}
	for _, test := range tests {
		got := visible(lines, test.focus, test.height)
		if !reflect.DeepEqual(got, test.want) {
			t.Errorf("visible(focus %d, height %d) = %v, want %v", test.focus, test.height, got, test.want)
		}
	}
}

// plain is the content of the view without colors.
func plain(view tea.View) string {
	return ansi.Strip(view.Content)
}

func TestFindingSummaryCountsIgnoredApart(t *testing.T) {
	findings := []report.Finding{
		{Severity: report.SeverityError},
		{Severity: report.SeverityError, Ignored: true},
	}

	if got, want := findingSummary(findings), "1 error, 1 ignored"; got != want {
		t.Errorf("summary = %q, want %q", got, want)
	}
	if got, want := findingSummary(findings[1:]), "no findings, 1 ignored"; got != want {
		t.Errorf("summary = %q, want %q", got, want)
	}
}
