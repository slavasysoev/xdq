package rule

import (
	"strings"
	"testing"
	"time"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// topicsSnapshot has the named topics of source main, each with its latest record.
func topicsSnapshot(latest map[string]*time.Time) graph.Snapshot {
	snapshot := graph.NewSnapshot()
	for name, record := range latest {
		topic := graph.NewTopic("main", name)
		snapshot.Nodes = append(snapshot.Nodes, topic)
		snapshot.Observations.Topics[topic.ID] = graph.TopicObservation{
			Partitions: 1, EndOffsets: map[int32]int64{0: 5}, LatestRecord: record,
		}
	}
	snapshot.Sort()

	return snapshot
}

func TestDeadLetterRecords(t *testing.T) {
	deadLetter := graph.TopicID("main", "orders.DLT")
	tests := []struct {
		name     string
		latest   map[string]*time.Time
		settings settings
		want     []graph.NodeID
	}{
		{
			name:     "recent record and a base topic",
			latest:   map[string]*time.Time{"orders": ago(time.Hour), "orders.DLT": ago(10 * time.Minute)},
			settings: settings{window: time.Hour, suffixes: defaultDeadLetterSuffixes},
			want:     []graph.NodeID{deadLetter, orders},
		},
		{
			name:     "no base topic",
			latest:   map[string]*time.Time{"orders.DLT": ago(10 * time.Minute)},
			settings: settings{window: time.Hour, suffixes: defaultDeadLetterSuffixes},
			want:     []graph.NodeID{deadLetter},
		},
		{
			name:     "record older than the window",
			latest:   map[string]*time.Time{"orders.DLT": ago(2 * time.Hour)},
			settings: settings{window: time.Hour, suffixes: defaultDeadLetterSuffixes},
		},
		{
			name:     "wider window",
			latest:   map[string]*time.Time{"orders.DLT": ago(2 * time.Hour)},
			settings: settings{window: 3 * time.Hour, suffixes: defaultDeadLetterSuffixes},
			want:     []graph.NodeID{deadLetter},
		},
		{
			name:     "no record",
			latest:   map[string]*time.Time{"orders.DLT": nil},
			settings: settings{window: time.Hour, suffixes: defaultDeadLetterSuffixes},
		},
		{
			name:     "not a dead-letter topic",
			latest:   map[string]*time.Time{"orders": ago(time.Minute)},
			settings: settings{window: time.Hour, suffixes: defaultDeadLetterSuffixes},
		},
		{
			name:     "custom suffixes replace the default ones",
			latest:   map[string]*time.Time{"orders.DLT": ago(time.Minute), "orders.failed": ago(time.Minute)},
			settings: settings{window: time.Hour, suffixes: []string{".failed"}},
			want:     []graph.NodeID{graph.TopicID("main", "orders.failed")},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			test.settings.name = ruleDeadLetterRecords

			findings := checkDeadLetterRecords(Input{Graph: topicsSnapshot(test.latest), Now: now}, test.settings)

			if test.want == nil {
				if len(findings) != 0 {
					t.Errorf("findings = %+v, want none", findings)
				}

				return
			}
			if len(findings) != 1 || findings[0].Severity != report.SeverityWarning || !equalSubjects(findings[0].Subjects, test.want) {
				t.Fatalf("findings = %+v, want one warning with subjects %v", findings, test.want)
			}
			if !strings.Contains(findings[0].Message, "dead-letter topic") {
				t.Errorf("message = %q", findings[0].Message)
			}
		})
	}
}

func equalSubjects(got, want []graph.NodeID) bool {
	return strings.Join(idsToStrings(got), ",") == strings.Join(idsToStrings(want), ",")
}

func idsToStrings(ids []graph.NodeID) []string {
	texts := make([]string, len(ids))
	for index, id := range ids {
		texts[index] = string(id)
	}

	return texts
}

func TestDeadLetterTopicsAreSkippedByOtherRules(t *testing.T) {
	snapshot := topicsSnapshot(map[string]*time.Time{"orders.dlq": ago(time.Hour)})
	deadLetter := graph.TopicID("main", "orders.dlq")
	snapshot.Observations.Topics[deadLetter] = graph.TopicObservation{
		EndOffsets: map[int32]int64{0: 0}, LatestRecord: ago(time.Hour),
	}
	snapshot.Nodes = append(snapshot.Nodes, graph.NewGroup("main", "dlq-reader"))
	snapshot.Edges = []graph.Edge{{From: deadLetter, To: graph.GroupID("main", "dlq-reader"), Kind: graph.EdgeConsumer}}
	input := Input{Graph: snapshot, Now: now}

	empty := checkEmptySubscription(input, settings{name: "empty-subscription", suffixes: defaultDeadLetterSuffixes})
	snapshot.Edges = nil
	unconsumed := checkUnconsumedTopic(Input{Graph: snapshot, Now: now}, settings{name: "unconsumed-topic", suffixes: defaultDeadLetterSuffixes})

	if len(empty) != 0 || len(unconsumed) != 0 {
		t.Errorf("empty-subscription = %+v, unconsumed-topic = %+v, want none for a dead-letter topic", empty, unconsumed)
	}
}

func TestSetSkipsDeadLettersWithCustomSuffixesAlsoWhenRuleIsOff(t *testing.T) {
	snapshot := topicsSnapshot(map[string]*time.Time{"orders.failed": ago(time.Hour)})
	set, err := NewSet(map[string]config.RuleConfig{
		ruleDeadLetterRecords: {Severity: config.SeverityOff, Suffixes: []string{".failed"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	findings := set.Check(Input{Graph: snapshot, Now: now})

	if len(findings) != 0 {
		t.Errorf("findings = %+v, want none: the topic is dead-letter by the configured suffix", findings)
	}
}

func TestNewSetDeadLetterOptions(t *testing.T) {
	tests := []struct {
		name      string
		configs   map[string]config.RuleConfig
		wantError string
	}{
		{name: "window", configs: map[string]config.RuleConfig{ruleDeadLetterRecords: {Window: 30 * time.Minute}}},
		{name: "suffixes", configs: map[string]config.RuleConfig{ruleDeadLetterRecords: {Suffixes: []string{".bad"}}}},
		{
			name:      "negative window",
			configs:   map[string]config.RuleConfig{ruleDeadLetterRecords: {Window: -time.Minute}},
			wantError: "rules.dead-letter-records.window: must not be negative",
		},
		{
			name:      "window on another rule",
			configs:   map[string]config.RuleConfig{"stalled-consumer": {Window: time.Hour}},
			wantError: "rules.stalled-consumer.window: rule stalled-consumer has no window",
		},
		{
			name:      "suffixes on another rule",
			configs:   map[string]config.RuleConfig{"stalled-consumer": {Suffixes: []string{".dlt"}}},
			wantError: "rules.stalled-consumer.suffixes: rule stalled-consumer has no suffixes",
		},
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
