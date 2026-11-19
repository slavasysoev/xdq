package kafka

import (
	"slices"
	"testing"

	"github.com/twmb/franz-go/pkg/kadm"
)

// Share and streams groups need feature flags on Kafka 4.1, so the filter is tested on the
// ListGroups answers instead of a real cluster.
func TestSelectGroups(t *testing.T) {
	listed := kadm.ListedGroups{}
	for _, group := range []kadm.ListedGroup{
		{Group: "billing", GroupType: "classic", ProtocolType: "consumer"},
		{Group: "audit", GroupType: "consumer", ProtocolType: "consumer"},
		{Group: "spark", GroupType: "classic", ProtocolType: ""},
		{Group: "legacy", GroupType: "", ProtocolType: "consumer"},
		{Group: "connect-cluster", GroupType: "classic", ProtocolType: "connect"},
		{Group: "shared", GroupType: "share", ProtocolType: "share"},
		{Group: "streams-app", GroupType: "streams", ProtocolType: "streams"},
	} {
		listed[group.Group] = group
	}

	classicGroups, consumerGroups := selectGroups(listed)

	if want := []string{"billing", "legacy", "spark"}; !slices.Equal(classicGroups, want) {
		t.Errorf("classic groups = %v, want %v", classicGroups, want)
	}
	if want := []string{"audit"}; !slices.Equal(consumerGroups, want) {
		t.Errorf("consumer groups = %v, want %v", consumerGroups, want)
	}
}
