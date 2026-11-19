package graph

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func TestSnapshotJSONRoundTrip(t *testing.T) {
	workload := NewWorkload("test", "stand-a", WorkloadDeployment, "billing")
	workload.Environment = "stand-a"
	topic := NewTopic("main", "orders.created")
	group := NewGroup("main", "billing")
	latestRecord := time.Date(2026, 11, 18, 12, 30, 0, 0, time.UTC)

	original := Snapshot{
		Nodes: []Node{topic, group, workload, NewDatabase("orders-db", "orders"), NewEndpoint("10.4.2.17")},
		Edges: []Edge{
			{
				From:       topic.ID,
				To:         group.ID,
				Kind:       EdgeConsumer,
				Confidence: ConfidenceObserved,
				Evidence:   []Evidence{{Source: "main", Detail: "committed offsets"}},
			},
			{
				From:       workload.ID,
				To:         TopicID("main", "invoices.created"),
				Kind:       EdgeProducer,
				Confidence: ConfidenceInferred,
				Evidence:   []Evidence{{Source: "test", Detail: "env KAFKA_OUT_TOPIC=invoices.created"}},
			},
		},
		Identities: []Identity{{Address: "10.4.2.17", Workload: workload.ID}},
		ConfigValues: []ConfigValue{
			{Workload: workload.ID, Origin: "env KAFKA_OUT_TOPIC", Value: "invoices.created"},
		},
		Observations: Observations{
			Topics: map[NodeID]TopicObservation{
				topic.ID: {Partitions: 2, EndOffsets: map[int32]int64{0: 10, 1: 0}, LatestRecord: &latestRecord},
			},
			Groups: map[NodeID]GroupObservation{
				group.ID: {
					State: "Stable",
					Lag:   map[NodeID]int64{topic.ID: 3},
					Members: []MemberObservation{
						{Endpoint: EndpointID("10.4.2.17"), ClientID: "billing-1", Assignment: map[NodeID][]int32{topic.ID: {0, 1}}},
					},
				},
			},
		},
	}

	encoded, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Snapshot
	err = json.Unmarshal(encoded, &decoded)
	if err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if !reflect.DeepEqual(decoded, original) {
		t.Errorf("round trip mismatch\n got: %+v\nwant: %+v", decoded, original)
	}
}

func TestNodeJSONOmitsWorkloadFieldsForOtherKinds(t *testing.T) {
	encoded, err := json.Marshal(NewTopic("main", "orders.created"))
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	want := `{"id":"topic:main/orders.created","kind":"topic","source":"main","name":"orders.created"}`
	if string(encoded) != want {
		t.Errorf("JSON = %s, want %s", encoded, want)
	}
}
