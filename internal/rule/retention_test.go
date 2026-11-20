package rule

import (
	"strings"
	"testing"

	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

func TestLagBeyondRetention(t *testing.T) {
	tests := []struct {
		name      string
		start     map[int32]int64
		committed map[int32]int64
		want      []string
		wantEvery string
	}{
		{name: "committed below start", start: map[int32]int64{0: 130}, committed: map[int32]int64{0: 10}, want: []string{"partitions 0", "about 120 records"}, wantEvery: "partition 0: committed 10, start 130"},
		{name: "committed at start", start: map[int32]int64{0: 130}, committed: map[int32]int64{0: 130}},
		{name: "committed above start", start: map[int32]int64{0: 130}, committed: map[int32]int64{0: 150}},
		{
			name:      "several partitions add up",
			start:     map[int32]int64{0: 100, 1: 50, 2: 0},
			committed: map[int32]int64{0: 90, 1: 45, 2: 0},
			want:      []string{"partitions 0,1", "about 15 records"},
		},
		{name: "no start offset known", committed: map[int32]int64{0: 10}},
		{name: "no commits", start: map[int32]int64{0: 130}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			snapshot := graph.NewSnapshot()
			snapshot.Nodes = []graph.Node{graph.NewTopic("main", "orders"), graph.NewGroup("main", "billing")}
			snapshot.Observations.Topics[orders] = graph.TopicObservation{StartOffsets: test.start}
			group := graph.GroupObservation{State: "Empty"}
			if test.committed != nil {
				group.Committed = map[graph.NodeID]map[int32]int64{orders: test.committed}
			}
			snapshot.Observations.Groups[billing] = group

			findings := checkLagBeyondRetention(Input{Graph: snapshot, Now: now}, settings{name: "lag-beyond-retention"})

			if test.want == nil {
				if len(findings) != 0 {
					t.Errorf("findings = %+v, want none", findings)
				}

				return
			}
			if len(findings) != 1 || findings[0].Severity != report.SeverityError || !equalSubjects(findings[0].Subjects, []graph.NodeID{billing, orders}) {
				t.Fatalf("findings = %+v, want one error about billing and orders", findings)
			}
			for _, want := range test.want {
				if !strings.Contains(findings[0].Message, want) {
					t.Errorf("message = %q, want it to contain %q", findings[0].Message, want)
				}
			}
			if test.wantEvery != "" && findings[0].Evidence[0].Detail != test.wantEvery {
				t.Errorf("evidence = %q, want %q", findings[0].Evidence[0].Detail, test.wantEvery)
			}
		})
	}
}
