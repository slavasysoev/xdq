package kafka

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/rule"
	"github.com/slavasysoev/xdq/internal/source"
	"github.com/slavasysoev/xdq/internal/testenv"
)

const sourceName = "main"

// Compile-time check that Source implements the source contract.
var _ source.Source = (*Source)(nil)

// TestDiscover runs against a real broker with this setup:
//
//   - orders.created: 3 partitions; 6 records in partition 0, 4 in partition 1, none in 2.
//   - orders.creatd: an empty topic, the typo case.
//   - billing: classic group with an active member that consumed and committed everything.
//   - stalled: a group without members that committed partition 0 at 2 and partition 1 at 4.
//   - audit: KIP-848 group with an active member on orders.creatd.
func TestDiscover(t *testing.T) {
	ctx := t.Context()
	brokers := testenv.StartKafka(t)
	setupCluster(t, brokers)

	snapshot, err := newSource(t, brokers).Discover(ctx)
	if err != nil {
		t.Fatalf("Discover: %v", err)
	}

	orders := graph.TopicID(sourceName, "orders.created")
	typo := graph.TopicID(sourceName, "orders.creatd")
	billing := graph.GroupID(sourceName, "billing")
	stalled := graph.GroupID(sourceName, "stalled")
	audit := graph.GroupID(sourceName, "audit")

	t.Run("nodes", func(t *testing.T) {
		for _, id := range []graph.NodeID{orders, typo, billing, stalled, audit} {
			if !hasNode(snapshot, id) {
				t.Errorf("missing node %s", id)
			}
		}
	})

	t.Run("subscriptions", func(t *testing.T) {
		wantEvidence := map[graph.NodeID]string{
			billing: "assigned partitions 0,1,2; committed offsets on partitions 0,1",
			stalled: "committed offsets on partitions 0,1",
		}
		for group, detail := range wantEvidence {
			edge, found := findEdge(snapshot, orders, graph.EdgeConsumer, group)
			if !found {
				t.Errorf("missing subscription %s -> %s", orders, group)

				continue
			}
			if edge.Evidence[0].Detail != detail {
				t.Errorf("evidence of %s = %q, want %q", group, edge.Evidence[0].Detail, detail)
			}
		}
		_, found := findEdge(snapshot, typo, graph.EdgeConsumer, audit)
		if !found {
			t.Errorf("missing subscription %s -> %s", typo, audit)
		}
	})

	t.Run("memberships", func(t *testing.T) {
		for _, group := range []graph.NodeID{billing, audit} {
			edges := edgesFrom(snapshot, group, graph.EdgeMembership)
			if len(edges) != 1 {
				t.Fatalf("membership edges of %s = %d, want 1", group, len(edges))
			}
			if !strings.HasPrefix(string(edges[0].To), "endpoint:") || strings.HasPrefix(string(edges[0].To), "endpoint:/") {
				t.Errorf("membership target = %s, want endpoint:<address>", edges[0].To)
			}
		}
		if len(edgesFrom(snapshot, stalled, graph.EdgeMembership)) != 0 {
			t.Error("stalled group has membership edges")
		}
	})

	t.Run("observations", func(t *testing.T) {
		ordersObservation := snapshot.Observations.Topics[orders]
		if ordersObservation.Partitions != 3 {
			t.Errorf("partitions = %d, want 3", ordersObservation.Partitions)
		}
		wantEnd := map[int32]int64{0: 6, 1: 4, 2: 0}
		for partition, want := range wantEnd {
			if ordersObservation.EndOffsets[partition] != want {
				t.Errorf("end offset of partition %d = %d, want %d", partition, ordersObservation.EndOffsets[partition], want)
			}
		}
		if ordersObservation.LatestRecord == nil {
			t.Error("orders.created has no latest record timestamp")
		}
		if snapshot.Observations.Topics[typo].LatestRecord != nil {
			t.Error("empty topic has a latest record timestamp")
		}

		billingObservation := snapshot.Observations.Groups[billing]
		if billingObservation.State != "Stable" || billingObservation.Lag[orders] != 0 {
			t.Errorf("billing state=%s lag=%d, want Stable 0", billingObservation.State, billingObservation.Lag[orders])
		}
		if len(billingObservation.Members) != 1 || billingObservation.Members[0].ClientID != "billing-1" ||
			!slices.Equal(billingObservation.Members[0].Assignment[orders], []int32{0, 1, 2}) {
			t.Errorf("billing members = %+v, want billing-1 with partitions 0,1,2", billingObservation.Members)
		}

		stalledObservation := snapshot.Observations.Groups[stalled]
		if stalledObservation.State != "Empty" || stalledObservation.Lag[orders] != 4 || len(stalledObservation.Members) != 0 {
			t.Errorf("stalled = %+v, want Empty, lag 4, no members", stalledObservation)
		}

		auditObservation := snapshot.Observations.Groups[audit]
		if len(auditObservation.Members) != 1 || auditObservation.Members[0].ClientID != "audit-1" {
			t.Errorf("audit members = %+v, want audit-1", auditObservation.Members)
		}
	})

	t.Run("rules on the real cluster", func(t *testing.T) {
		rules, err := rule.NewSet(nil)
		if err != nil {
			t.Fatal(err)
		}
		var got []string
		for _, finding := range rules.Check(rule.Input{Graph: snapshot, Now: time.Now()}) {
			got = append(got, finding.Rule+" "+string(finding.Subjects[0]))
		}
		want := []string{
			"stalled-consumer " + string(stalled),
			"empty-subscription " + string(audit),
		}
		if !slices.Equal(got, want) {
			t.Errorf("findings = %v, want %v", got, want)
		}
	})

	t.Run("xdq is never a group member", func(t *testing.T) {
		for group, observation := range snapshot.Observations.Groups {
			for _, member := range observation.Members {
				if member.ClientID == clientID {
					t.Errorf("xdq is a member of %s", group)
				}
			}
		}
		if len(snapshot.Observations.Groups) != 3 {
			t.Errorf("groups = %d, want 3: xdq must not create groups", len(snapshot.Observations.Groups))
		}
	})
}

func TestDiscoverUnreachableBroker(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	_, err := newSource(t, []string{"127.0.0.1:1"}).Discover(ctx)
	if err == nil {
		t.Fatal("expected an error, got nil")
	}
}

// newSource creates a source that is closed when the test ends.
func newSource(t *testing.T, brokers []string) *Source {
	t.Helper()
	kafkaSource, err := New(sourceName, config.KafkaConfig{Brokers: brokers})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(func() {
		closeErr := kafkaSource.Close()
		if closeErr != nil {
			t.Errorf("Close: %v", closeErr)
		}
	})

	return kafkaSource
}

// setupCluster creates the topics, records and groups described on TestDiscover.
// The consumers keep running until the test ends.
func setupCluster(t *testing.T, brokers []string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()

	client, err := kgo.NewClient(kgo.SeedBrokers(brokers...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)
	admin := kadm.NewClient(client)

	for topic, partitions := range map[string]int32{"orders.created": 3, "orders.creatd": 1} {
		_, err = admin.CreateTopic(ctx, partitions, 1, nil, topic)
		if err != nil {
			t.Fatalf("create topic %s: %v", topic, err)
		}
	}
	var records []*kgo.Record
	for index := range 10 {
		partition := int32(0)
		if index >= 6 {
			partition = 1
		}
		records = append(records, &kgo.Record{Topic: "orders.created", Partition: partition, Value: []byte("order")})
	}
	err = client.ProduceSync(ctx, records...).FirstErr()
	if err != nil {
		t.Fatalf("produce: %v", err)
	}

	offsets := make(kadm.Offsets)
	offsets.Add(kadm.Offset{Topic: "orders.created", Partition: 0, At: 2, LeaderEpoch: -1})
	offsets.Add(kadm.Offset{Topic: "orders.created", Partition: 1, At: 4, LeaderEpoch: -1})
	committed, err := admin.CommitOffsets(ctx, "stalled", offsets)
	if err == nil {
		err = committed.Error()
	}
	if err != nil {
		t.Fatalf("commit stalled offsets: %v", err)
	}

	billing := startConsumer(t, brokers, "billing", "billing-1", "orders.created")
	consumed := 0
	for consumed < 10 {
		fetches := billing.PollFetches(ctx)
		if ctx.Err() != nil {
			t.Fatalf("billing consumed %d of 10 records", consumed)
		}
		consumed += fetches.NumRecords()
	}
	err = billing.CommitUncommittedOffsets(ctx)
	if err != nil {
		t.Fatalf("commit billing offsets: %v", err)
	}

	audit := startConsumer(t, brokers, "audit", "audit-1", "orders.creatd", kgo.ServerSideBalancer(), kgo.Balancers(kgo.RangeBalancer()))
	waitForMember(ctx, t, admin, audit)
}

func startConsumer(t *testing.T, brokers []string, group, client, topic string, extra ...kgo.Opt) *kgo.Client {
	t.Helper()
	options := append([]kgo.Opt{
		kgo.SeedBrokers(brokers...),
		kgo.ClientID(client),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	}, extra...)
	consumer, err := kgo.NewClient(options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(consumer.Close)

	return consumer
}

// waitForMember polls the consumer until the broker reports it as an assigned group member.
func waitForMember(ctx context.Context, t *testing.T, admin *kadm.Client, consumer *kgo.Client) {
	t.Helper()
	for ctx.Err() == nil {
		pollContext, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
		consumer.PollFetches(pollContext)
		cancel()
		described, err := admin.DescribeConsumerGroups(ctx, "audit")
		// A map value is not addressable, so copy it before calling a pointer method.
		group := described["audit"]
		if err == nil && len(group.AssignedPartitions()) > 0 {
			return
		}
	}
	t.Fatal("audit consumer never got an assignment")
}

func hasNode(snapshot graph.Snapshot, id graph.NodeID) bool {
	return slices.ContainsFunc(snapshot.Nodes, func(node graph.Node) bool { return node.ID == id })
}

func findEdge(snapshot graph.Snapshot, from graph.NodeID, kind graph.EdgeKind, to graph.NodeID) (graph.Edge, bool) {
	for _, edge := range snapshot.Edges {
		if edge.From == from && edge.Kind == kind && edge.To == to {
			return edge, true
		}
	}

	return graph.Edge{}, false
}

func edgesFrom(snapshot graph.Snapshot, from graph.NodeID, kind graph.EdgeKind) []graph.Edge {
	var edges []graph.Edge
	for _, edge := range snapshot.Edges {
		if edge.From == from && edge.Kind == kind {
			edges = append(edges, edge)
		}
	}

	return edges
}
