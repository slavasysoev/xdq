// Package rule checks the invariants of §9.1. Rules work on one run's graph, without a lockfile.
package rule

import (
	"cmp"
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"
	"time"

	"github.com/slavasysoev/xdq/internal/config"
	"github.com/slavasysoev/xdq/internal/graph"
	"github.com/slavasysoev/xdq/internal/report"
)

// Input is what the rules check: the merged graph of one run and which sources answered.
type Input struct {
	Graph   graph.Snapshot
	Sources []report.SourceResult
	// Now is the time of the run. Rules about recent activity compare against it.
	Now time.Time
}

type checkFunc func(input Input, settings settings) []report.Finding

type definition struct {
	name string
	// threshold is the default threshold. Rules without a threshold leave it 0.
	threshold int
	check     checkFunc
}

var definitions = []definition{
	{name: "stalled-consumer", check: checkStalledConsumer},
	{name: "lagging-consumer", threshold: 1000, check: checkLaggingConsumer},
	{name: "empty-subscription", check: checkEmptySubscription},
	{name: "unconsumed-topic", check: checkUnconsumedTopic},
	{name: "unresolved-client", check: checkUnresolvedClient},
	{name: "group-spans-environments", check: checkGroupSpansEnvironments},
	{name: "group-multiple-workloads", check: checkGroupMultipleWorkloads},
	{name: "near-miss-topic", check: checkNearMissTopic},
}

// settings is a rule with the overrides from the config applied.
type settings struct {
	name string
	// severity overrides the rule's default severity when not empty.
	severity  report.Severity
	threshold int
}

// finding creates a finding of this rule. defaultSeverity applies unless the config overrides it.
func (settings settings) finding(defaultSeverity report.Severity, subjects []graph.NodeID, message string, evidence []graph.Evidence) report.Finding {
	return report.Finding{
		Rule:     settings.name,
		Severity: cmp.Or(settings.severity, defaultSeverity),
		Subjects: subjects,
		Message:  message,
		Evidence: evidence,
	}
}

type enabledRule struct {
	settings settings
	check    checkFunc
}

// Set is the rules enabled by the config, with their overrides. It is built once and can check
// many runs.
type Set struct {
	rules []enabledRule
}

// NewSet validates the rules section of the config and returns the enabled rules.
// Unknown rule names and thresholds on rules without one are errors.
func NewSet(configs map[string]config.RuleConfig) (*Set, error) {
	known := make(map[string]definition, len(definitions))
	for _, rule := range definitions {
		known[rule.name] = rule
	}
	var problems []error
	for _, name := range slices.Sorted(maps.Keys(configs)) {
		ruleConfig := configs[name]
		rule, isKnown := known[name]
		if !isKnown {
			problems = append(problems, fmt.Errorf("rules.%s: unknown rule; known rules: %s", name, strings.Join(names(), ", ")))

			continue
		}
		if ruleConfig.Threshold == nil {
			continue
		}
		switch {
		case rule.threshold == 0:
			problems = append(problems, fmt.Errorf("rules.%s.threshold: rule %s has no threshold", name, name))
		case *ruleConfig.Threshold < 0:
			problems = append(problems, fmt.Errorf("rules.%s.threshold: must not be negative", name))
		}
	}
	if len(problems) > 0 {
		return nil, errors.Join(problems...)
	}

	set := &Set{}
	for _, rule := range definitions {
		ruleConfig := configs[rule.name]
		if ruleConfig.Severity == config.SeverityOff {
			continue
		}
		ruleSettings := settings{name: rule.name, severity: report.Severity(ruleConfig.Severity), threshold: rule.threshold}
		if ruleConfig.Threshold != nil {
			ruleSettings.threshold = *ruleConfig.Threshold
		}
		set.rules = append(set.rules, enabledRule{settings: ruleSettings, check: rule.check})
	}

	return set, nil
}

// Check runs every enabled rule. The findings are in rule order, and each rule reports in
// a deterministic order, so the same input gives the same findings.
func (set *Set) Check(input Input) []report.Finding {
	var findings []report.Finding
	for _, rule := range set.rules {
		findings = append(findings, rule.check(input, rule.settings)...)
	}

	return findings
}

// names returns the names of all rules the config accepts, sorted.
func names() []string {
	var all []string
	for _, rule := range definitions {
		all = append(all, rule.name)
	}
	slices.Sort(all)

	return all
}

// nodesByID indexes the nodes of the graph.
func nodesByID(snapshot graph.Snapshot) map[graph.NodeID]graph.Node {
	nodes := make(map[graph.NodeID]graph.Node, len(snapshot.Nodes))
	for _, node := range snapshot.Nodes {
		nodes[node.ID] = node
	}

	return nodes
}
