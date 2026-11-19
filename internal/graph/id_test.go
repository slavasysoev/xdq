package graph

import "testing"

func TestNodeIDs(t *testing.T) {
	tests := []struct {
		name string
		node Node
		want NodeID
	}{
		{name: "topic", node: NewTopic("main", "orders.created"), want: "topic:main/orders.created"},
		{name: "group", node: NewGroup("main", "billing"), want: "group:main/billing"},
		{name: "group with slash", node: NewGroup("main", "team/billing"), want: "group:main/team/billing"},
		{
			name: "workload",
			node: NewWorkload("test", "stand-a", WorkloadDeployment, "billing"),
			want: "workload:test/stand-a/deployment/billing",
		},
		{name: "bare pod", node: NewWorkload("test", "stand-a", WorkloadPod, "debug"), want: "workload:test/stand-a/pod/debug"},
		{name: "database", node: NewDatabase("orders-db", "orders"), want: "database:orders-db/orders"},
		{name: "endpoint", node: NewEndpoint("10.4.2.17"), want: "endpoint:10.4.2.17"},
		{name: "ipv6 endpoint", node: NewEndpoint("fd00::17"), want: "endpoint:fd00::17"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if test.node.ID != test.want {
				t.Errorf("ID = %q, want %q", test.node.ID, test.want)
			}
		})
	}
}
