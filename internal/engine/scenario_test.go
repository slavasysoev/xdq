package engine

import (
	"reflect"
	"testing"
	"time"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
	"github.com/slavasysoev/xdq/internal/rule"
	"github.com/slavasysoev/xdq/internal/source"
)

// TestScenarioStolenMessages reproduces the motivating bug: a pod of stand-b joins the consumer
// group of stand-a and silently takes half of the partitions. The group is Stable, lag is zero.
func TestScenarioStolenMessages(t *testing.T) {
	orders := graph.NewTopic("main", "orders.created")
	group := graph.NewGroup("main", "billing")
	podA := graph.NewEndpoint("10.0.0.1")
	podB := graph.NewEndpoint("10.0.1.1")

	kafka := graph.NewSnapshot()
	kafka.Nodes = []graph.Node{orders, group, podA, podB}
	kafka.Edges = []graph.Edge{
		{From: orders.ID, To: group.ID, Kind: graph.EdgeConsumer, Evidence: []graph.Evidence{{Source: "main"}}},
		{From: group.ID, To: podA.ID, Kind: graph.EdgeMembership, Evidence: []graph.Evidence{{Source: "main"}}},
		{From: group.ID, To: podB.ID, Kind: graph.EdgeMembership, Evidence: []graph.Evidence{{Source: "main"}}},
	}
	kafka.Observations.Topics[orders.ID] = graph.TopicObservation{
		Partitions: 6,
		EndOffsets: map[int32]int64{0: 10, 1: 10, 2: 10, 3: 10, 4: 10, 5: 10},
	}
	kafka.Observations.Groups[group.ID] = graph.GroupObservation{
		State: "Stable",
		Lag:   map[graph.NodeID]int64{orders.ID: 0},
		Members: []graph.MemberObservation{
			{Endpoint: podA.ID, ClientID: "billing-a", Assignment: map[graph.NodeID][]int32{orders.ID: {0, 1, 2}}},
			{Endpoint: podB.ID, ClientID: "billing-b", Assignment: map[graph.NodeID][]int32{orders.ID: {3, 4, 5}}},
		},
	}

	billingA := graph.NewWorkload("test", "stand-a", graph.WorkloadDeployment, "billing")
	billingA.Environment = "stand-a"
	billingB := graph.NewWorkload("test", "stand-b", graph.WorkloadDeployment, "billing")
	billingB.Environment = "stand-b"
	kubernetes := graph.NewSnapshot()
	kubernetes.Nodes = []graph.Node{billingA, billingB}
	kubernetes.Identities = []graph.Identity{
		{Address: "10.0.0.1", Workload: billingA.ID},
		{Address: "10.0.1.1", Workload: billingB.ID},
	}

	rules, err := rule.NewSet(nil)
	if err != nil {
		t.Fatal(err)
	}
	engine := New([]source.Source{
		returning("main", config.TypeKafka, kafka, nil),
		returning("test", config.TypeKubernetes, kubernetes, nil),
	}, rules, nil, time.Second)

	result := engine.Run(t.Context(), "stand-a")

	if len(result.Findings) != 1 {
		t.Fatalf("findings = %+v, want exactly group-spans-environments", result.Findings)
	}
	finding := result.Findings[0]
	if finding.Rule != "group-spans-environments" || finding.Severity != report.SeverityError {
		t.Errorf("finding = %s %s, want group-spans-environments error", finding.Rule, finding.Severity)
	}
	wantSubjects := []graph.NodeID{group.ID, billingA.ID, billingB.ID}
	if !reflect.DeepEqual(finding.Subjects, wantSubjects) {
		t.Errorf("subjects = %v, want %v", finding.Subjects, wantSubjects)
	}
	wantEvidence := []graph.Evidence{
		{Source: "test", Detail: "stand-a  deployment/billing  orders.created 0,1,2"},
		{Source: "test", Detail: "stand-b  deployment/billing  orders.created 3,4,5"},
	}
	if !reflect.DeepEqual(finding.Evidence, wantEvidence) {
		t.Errorf("evidence =\n%+v\nwant\n%+v", finding.Evidence, wantEvidence)
	}
	if result.Status != report.StatusError {
		t.Errorf("status = %s, want error", result.Status)
	}
}

// TestScenarioTypo reproduces the typo bug: the consumer's config names orders.creatd. With topic
// auto-creation the consumer happily consumes an empty topic, while the records go to
// orders.created. Nothing is red: the group is Stable and has no lag.
func TestScenarioTypo(t *testing.T) {
	orders := graph.NewTopic("main", "orders.created")
	typo := graph.NewTopic("main", "orders.creatd")
	group := graph.NewGroup("main", "billing")
	pod := graph.NewEndpoint("10.0.0.1")
	latestRecord := time.Now().Add(-time.Hour)

	kafka := graph.NewSnapshot()
	kafka.Nodes = []graph.Node{orders, typo, group, pod}
	kafka.Edges = []graph.Edge{
		{From: typo.ID, To: group.ID, Kind: graph.EdgeConsumer, Evidence: []graph.Evidence{{Source: "main"}}},
		{From: group.ID, To: pod.ID, Kind: graph.EdgeMembership, Evidence: []graph.Evidence{{Source: "main"}}},
	}
	kafka.Observations.Topics[orders.ID] = graph.TopicObservation{Partitions: 1, EndOffsets: map[int32]int64{0: 42}, LatestRecord: &latestRecord}
	kafka.Observations.Topics[typo.ID] = graph.TopicObservation{Partitions: 1, EndOffsets: map[int32]int64{0: 0}}
	kafka.Observations.Groups[group.ID] = graph.GroupObservation{
		State:   "Stable",
		Lag:     map[graph.NodeID]int64{typo.ID: 0},
		Members: []graph.MemberObservation{{Endpoint: pod.ID, ClientID: "billing-a", Assignment: map[graph.NodeID][]int32{typo.ID: {0}}}},
	}

	billing := graph.NewWorkload("test", "stand-a", graph.WorkloadDeployment, "billing")
	billing.Environment = "stand-a"
	kubernetes := graph.NewSnapshot()
	kubernetes.Nodes = []graph.Node{billing}
	kubernetes.Identities = []graph.Identity{{Address: "10.0.0.1", Workload: billing.ID}}
	kubernetes.ConfigValues = []graph.ConfigValue{{
		Workload: billing.ID,
		Origin:   "configmap billing/application.yml",
		Value:    "spring:\n  kafka:\n    consumer:\n      group-id: billing\napp:\n  topic: orders.creatd\n",
	}}

	rules, err := rule.NewSet(nil)
	if err != nil {
		t.Fatal(err)
	}
	engine := New([]source.Source{
		returning("main", config.TypeKafka, kafka, nil),
		returning("test", config.TypeKubernetes, kubernetes, nil),
	}, rules, nil, time.Second)

	result := engine.Run(t.Context(), "stand-a")

	got := make(map[string]report.Finding)
	for _, finding := range result.Findings {
		got[finding.Rule] = finding
	}
	if len(got) != 2 || got["empty-subscription"].Rule == "" || got["near-miss-topic"].Rule == "" {
		t.Fatalf("findings = %+v, want empty-subscription and near-miss-topic", result.Findings)
	}
	wantMessage := "config names orders.creatd, a topic that has never received a record; did you mean orders.created?"
	if got["near-miss-topic"].Message != wantMessage {
		t.Errorf("message = %q, want %q", got["near-miss-topic"].Message, wantMessage)
	}
	wantEvidence := []graph.Evidence{{Source: "test", Detail: "configmap billing/application.yml"}}
	if !reflect.DeepEqual(got["near-miss-topic"].Evidence, wantEvidence) {
		t.Errorf("evidence = %+v, want %+v", got["near-miss-topic"].Evidence, wantEvidence)
	}
	for _, edge := range result.Chains[0].Edges {
		if edge.Kind == graph.EdgeProducer {
			t.Errorf("producer edge %s → %s: billing consumes the topic its config names", edge.From, edge.To)
		}
	}
	if result.Status != report.StatusWarning {
		t.Errorf("status = %s, want warning", result.Status)
	}
}
