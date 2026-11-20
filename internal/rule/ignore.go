package rule

import (
	"net/netip"
	"slices"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// exception is one entry of a rule's ignore list. It is either a selector of nodes or, when
// isEndpoint is set, an address range that matches endpoints.
type exception struct {
	selector graph.Selector
	// addresses is the range of an endpoint exception. An address is a range of one.
	addresses  netip.Prefix
	isEndpoint bool
}

// SelectorOf converts a selector of the config, used by chain entries and by exceptions.
func SelectorOf(entry config.SelectorConfig) graph.Selector {
	switch {
	case entry.Topic != "":
		return graph.Selector{Kind: graph.NodeTopic, Name: entry.Topic, Source: entry.Source}
	case entry.Group != "":
		return graph.Selector{Kind: graph.NodeGroup, Name: entry.Group, Source: entry.Source}
	case entry.Workload != "":
		return graph.Selector{Kind: graph.NodeWorkload, Name: entry.Workload, Source: entry.Source}
	default:
		return graph.Selector{Kind: graph.NodeDatabase, Name: entry.Database, Source: entry.Source}
	}
}

// newException converts a validated ignore entry.
func newException(entry config.IgnoreConfig) exception {
	if entry.Endpoint == "" {
		return exception{selector: SelectorOf(entry.SelectorConfig)}
	}
	prefix, err := netip.ParsePrefix(entry.Endpoint)
	if err != nil {
		address, _ := netip.ParseAddr(entry.Endpoint)
		prefix = netip.PrefixFrom(address, address.BitLen())
	}

	return exception{addresses: prefix, isEndpoint: true}
}

// matches tells whether the exception selects the node. A workload is selected in every
// environment: the workload to ignore is often in another one than the checked one.
func (exception exception) matches(node graph.Node) bool {
	if !exception.isEndpoint {
		return exception.selector.Matches(node, "")
	}
	if node.Kind != graph.NodeEndpoint {
		return false
	}
	address, err := netip.ParseAddr(node.Name)

	return err == nil && exception.addresses.Contains(address)
}

// markIgnored sets Ignored on every finding that has a subject selected by an exception.
func markIgnored(findings []report.Finding, exceptions []exception, nodes map[graph.NodeID]graph.Node) {
	if len(exceptions) == 0 {
		return
	}
	for index := range findings {
		findings[index].Ignored = slices.ContainsFunc(findings[index].Subjects, func(subject graph.NodeID) bool {
			node, found := nodes[subject]

			return found && slices.ContainsFunc(exceptions, func(exception exception) bool { return exception.matches(node) })
		})
	}
}
