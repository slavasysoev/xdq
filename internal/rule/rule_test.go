package rule

import (
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

var (
	now     = time.Date(2026, 11, 19, 12, 0, 0, 0, time.UTC)
	orders  = graph.TopicID("main", "orders")
	billing = graph.GroupID("main", "billing")
	laptop  = graph.EndpointID("192.168.1.20")
)

// scenario is a small cluster: the topic orders and the group billing, changed by each test case.
type scenario struct {
	endOffsets   map[int32]int64
	latestRecord *time.Time
	consumed     bool
	members      int
	lag          map[graph.NodeID]int64
	extraNodes   []graph.Node
	extraEdges   []graph.Edge
}

func (scenario scenario) snapshot() graph.Snapshot {
	snapshot := graph.NewSnapshot()
	snapshot.Nodes = append([]graph.Node{graph.NewTopic("main", "orders"), graph.NewGroup("main", "billing")}, scenario.extraNodes...)
	snapshot.Observations.Topics[orders] = graph.TopicObservation{
		Partitions:   1,
		EndOffsets:   scenario.endOffsets,
		LatestRecord: scenario.latestRecord,
	}
	group := graph.GroupObservation{State: "Stable", Lag: scenario.lag}
	for range scenario.members {
		group.Members = append(group.Members, graph.MemberObservation{Endpoint: graph.EndpointID("10.0.0.1"), ClientID: "billing-1"})
	}
	if scenario.members == 0 {
		group.State = "Empty"
	}
	snapshot.Observations.Groups[billing] = group
	if scenario.consumed {
		snapshot.Edges = append(snapshot.Edges, graph.Edge{
			From: orders, To: billing, Kind: graph.EdgeConsumer,
			Evidence: []graph.Evidence{{Source: "main", Detail: "assigned partitions 0"}},
		})
	}
	snapshot.Edges = append(snapshot.Edges, scenario.extraEdges...)
	snapshot.Sort()

	return snapshot
}

func ago(duration time.Duration) *time.Time {
	timestamp := now.Add(-duration)

	return &timestamp
}

type ruleTest struct {
	name     string
	scenario scenario
	sources  []report.SourceResult
	// want lists the subjects of every expected finding with its severity; nil means no findings.
	want []wantFinding
}

type wantFinding struct {
	severity report.Severity
	subjects []graph.NodeID
	message  string
}

func runRuleTests(t *testing.T, check checkFunc, ruleSettings settings, tests []ruleTest) {
	t.Helper()
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			findings := check(Input{Graph: test.scenario.snapshot(), Sources: test.sources, Now: now}, ruleSettings)
			if len(findings) != len(test.want) {
				t.Fatalf("findings = %+v, want %d", findings, len(test.want))
			}
			for index, want := range test.want {
				got := findings[index]
				if got.Rule != ruleSettings.name || got.Severity != want.severity || !reflect.DeepEqual(got.Subjects, want.subjects) {
					t.Errorf("finding = %s %s %v, want %s %s %v",
						got.Rule, got.Severity, got.Subjects, ruleSettings.name, want.severity, want.subjects)
				}
				if !strings.Contains(got.Message, want.message) {
					t.Errorf("message = %q, want it to contain %q", got.Message, want.message)
				}
			}
		})
	}
}

func TestStalledConsumer(t *testing.T) {
	runRuleTests(t, checkStalledConsumer, settings{name: "stalled-consumer"}, []ruleTest{
		{
			name:     "lag and no members",
			scenario: scenario{lag: map[graph.NodeID]int64{orders: 4}},
			want:     []wantFinding{{report.SeverityError, []graph.NodeID{billing, orders}, "lag 4 and no active members (state Empty)"}},
		},
		{name: "lag with active members", scenario: scenario{members: 1, lag: map[graph.NodeID]int64{orders: 4}}},
		{name: "no members and no lag", scenario: scenario{lag: map[graph.NodeID]int64{orders: 0}}},
		{name: "no committed offsets", scenario: scenario{}},
	})
}

func TestLaggingConsumer(t *testing.T) {
	runRuleTests(t, checkLaggingConsumer, settings{name: "lagging-consumer", threshold: 1000}, []ruleTest{
		{
			name:     "lag over the threshold",
			scenario: scenario{members: 1, lag: map[graph.NodeID]int64{orders: 1001}},
			want:     []wantFinding{{report.SeverityWarning, []graph.NodeID{billing, orders}, "lag 1001 on orders exceeds the threshold 1000"}},
		},
		{name: "lag at the threshold", scenario: scenario{members: 1, lag: map[graph.NodeID]int64{orders: 1000}}},
		{name: "no lag", scenario: scenario{members: 1}},
	})
}

func TestEmptySubscription(t *testing.T) {
	runRuleTests(t, checkEmptySubscription, settings{name: "empty-subscription"}, []ruleTest{
		{
			name:     "consumed topic never received a record",
			scenario: scenario{consumed: true, endOffsets: map[int32]int64{0: 0}},
			want:     []wantFinding{{report.SeverityWarning, []graph.NodeID{billing, orders}, "consumes orders, which has never received a record"}},
		},
		{name: "consumed topic has records", scenario: scenario{consumed: true, endOffsets: map[int32]int64{0: 6}, latestRecord: ago(time.Hour)}},
		{name: "records expired by retention", scenario: scenario{consumed: true, endOffsets: map[int32]int64{0: 6}}},
		{name: "empty topic without consumer", scenario: scenario{endOffsets: map[int32]int64{0: 0}}},
	})
}

func TestUnconsumedTopic(t *testing.T) {
	runRuleTests(t, checkUnconsumedTopic, settings{name: "unconsumed-topic"}, []ruleTest{
		{
			name:     "recent records and no consumer",
			scenario: scenario{endOffsets: map[int32]int64{0: 6}, latestRecord: ago(time.Hour)},
			want:     []wantFinding{{report.SeverityWarning, []graph.NodeID{orders}, "received a record at 2026-11-19T11:00:00Z"}},
		},
		{name: "recent records and a consumer", scenario: scenario{consumed: true, endOffsets: map[int32]int64{0: 6}, latestRecord: ago(time.Hour)}},
		{name: "idle topic", scenario: scenario{endOffsets: map[int32]int64{0: 6}, latestRecord: ago(8 * 24 * time.Hour)}},
		{name: "empty topic", scenario: scenario{endOffsets: map[int32]int64{0: 0}}},
	})
}

func TestUnconsumedTopicSkipsInternalTopics(t *testing.T) {
	snapshot := graph.NewSnapshot()
	snapshot.Nodes = []graph.Node{graph.NewTopic("main", "_schemas")}
	snapshot.Observations.Topics[graph.TopicID("main", "_schemas")] = graph.TopicObservation{
		EndOffsets: map[int32]int64{0: 3}, LatestRecord: ago(time.Hour),
	}

	findings := checkUnconsumedTopic(Input{Graph: snapshot, Now: now}, settings{name: "unconsumed-topic"})
	if len(findings) != 0 {
		t.Errorf("findings = %+v, want none for an internal topic", findings)
	}
}

func TestUnresolvedClient(t *testing.T) {
	database := graph.NewDatabase("db", "orders")
	member := scenario{
		extraNodes: []graph.Node{graph.NewEndpoint("192.168.1.20")},
		extraEdges: []graph.Edge{{From: billing, To: laptop, Kind: graph.EdgeMembership}},
	}
	resolved := scenario{
		extraNodes: []graph.Node{graph.NewWorkload("test", "stand-a", graph.WorkloadDeployment, "billing")},
		extraEdges: []graph.Edge{{From: billing, To: graph.WorkloadID("test", "stand-a", graph.WorkloadDeployment, "billing"), Kind: graph.EdgeMembership}},
	}
	databaseClient := scenario{
		extraNodes: []graph.Node{graph.NewEndpoint("192.168.1.20"), database},
		extraEdges: []graph.Edge{{From: laptop, To: database.ID, Kind: graph.EdgeConnection}},
	}
	kubernetes := []report.SourceResult{{Name: "test", Type: config.TypeKubernetes, Status: report.StatusOK}}
	kubernetesFailed := []report.SourceResult{{Name: "test", Type: config.TypeKubernetes, Status: report.StatusUnknown}}

	runRuleTests(t, checkUnresolvedClient, settings{name: "unresolved-client"}, []ruleTest{
		{
			name: "group member at an unknown address", scenario: member, sources: kubernetes,
			want: []wantFinding{{report.SeverityError, []graph.NodeID{billing, laptop}, "member at 192.168.1.20 is not a known workload"}},
		},
		{
			name: "database client at an unknown address", scenario: databaseClient, sources: kubernetes,
			want: []wantFinding{{report.SeverityWarning, []graph.NodeID{database.ID, laptop}, "client at 192.168.1.20 is not a known workload"}},
		},
		{name: "member resolved to a workload", scenario: resolved, sources: kubernetes},
		{name: "no kubernetes source", scenario: member},
		{name: "kubernetes source unavailable", scenario: member, sources: kubernetesFailed},
	})
}

func threshold(value int) *int {
	return new(value)
}

func TestNewSet(t *testing.T) {
	tests := []struct {
		name      string
		configs   map[string]config.RuleConfig
		wantError string
	}{
		{name: "defaults", configs: nil},
		{name: "rule turned off", configs: map[string]config.RuleConfig{"group-multiple-workloads": {Severity: config.SeverityOff}}},
		{name: "threshold override", configs: map[string]config.RuleConfig{"lagging-consumer": {Threshold: new(5000)}}},
		{name: "unknown rule", configs: map[string]config.RuleConfig{"stalled-consumers": {}}, wantError: "rules.stalled-consumers: unknown rule; known rules: empty-subscription"},
		{name: "threshold on a rule without one", configs: map[string]config.RuleConfig{"stalled-consumer": {Threshold: new(1)}}, wantError: "rules.stalled-consumer.threshold: rule stalled-consumer has no threshold"},
		{name: "negative threshold", configs: map[string]config.RuleConfig{"lagging-consumer": {Threshold: new(-1)}}, wantError: "rules.lagging-consumer.threshold: must not be negative"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := NewSet(test.configs)
			if test.wantError == "" && err != nil {
				t.Errorf("NewSet: %v", err)
			}
			if test.wantError != "" && (err == nil || !strings.Contains(err.Error(), test.wantError)) {
				t.Errorf("error = %v, want it to contain %q", err, test.wantError)
			}
		})
	}
}

func TestSetAppliesOverrides(t *testing.T) {
	input := Input{
		Graph: scenario{members: 1, consumed: true, endOffsets: map[int32]int64{0: 0}, lag: map[graph.NodeID]int64{orders: 2000}}.snapshot(),
		Now:   now,
	}
	set, err := NewSet(map[string]config.RuleConfig{
		"lagging-consumer":   {Severity: config.SeverityError, Threshold: threshold(5000)},
		"empty-subscription": {Severity: config.SeverityOff},
	})
	if err != nil {
		t.Fatal(err)
	}
	if findings := set.Check(input); len(findings) != 0 {
		t.Errorf("findings = %+v, want none: threshold raised and empty-subscription off", findings)
	}

	set, err = NewSet(map[string]config.RuleConfig{"lagging-consumer": {Severity: config.SeverityError}})
	if err != nil {
		t.Fatal(err)
	}
	findings := set.Check(input)
	want := []string{"lagging-consumer error", "empty-subscription warning"}
	var got []string
	for _, finding := range findings {
		got = append(got, finding.Rule+" "+string(finding.Severity))
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("findings = %v, want %v", got, want)
	}
}
