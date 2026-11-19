package config

import (
	"errors"
	"fmt"
	"maps"
	"regexp"
	"slices"
)

// sourceNamePattern keeps source names safe inside node IDs: no "/", no spaces.
var sourceNamePattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

// validation collects problems, each prefixed with the path of the offending field.
type validation struct {
	problems []error
}

func (validation *validation) fail(path, format string, arguments ...any) {
	validation.problems = append(validation.problems, fmt.Errorf("%s: %s", path, fmt.Sprintf(format, arguments...)))
}

func validate(config *Config) error {
	var result validation
	if config.Version != 1 {
		result.fail("version", "must be 1, got %d", config.Version)
	}
	if len(config.Sources) == 0 {
		result.fail("sources", "at least one source is required")
	}
	for _, name := range slices.Sorted(maps.Keys(config.Sources)) {
		result.source(name, config.Sources[name])
	}
	for _, name := range slices.Sorted(maps.Keys(config.Chains)) {
		result.chain(name, config.Chains[name], config.Sources)
	}
	for _, name := range slices.Sorted(maps.Keys(config.Rules)) {
		result.rule(name, config.Rules[name])
	}

	return errors.Join(result.problems...)
}

func (validation *validation) source(name string, source SourceConfig) {
	path := "sources." + name
	if !sourceNamePattern.MatchString(name) {
		validation.fail(path, "name must match %s", sourceNamePattern)
	}
	found := source.settings()
	if len(found) != 1 {
		validation.fail(path, "exactly one of %s, %s, %s must be set", TypeKafka, TypeKubernetes, TypePostgres)

		return
	}
	found[0].validate(validation, path+"."+found[0].Type())
}

// secret checks the reference form only. The message never repeats what was written,
// because a value that is not a reference may be an inlined password.
func (validation *validation) secret(path string, secret Secret) {
	if secret.reference == "" {
		validation.fail(path, "is required")

		return
	}
	_, _, isReference := parseReference(secret.reference)
	if !isReference {
		validation.fail(path, "must be a reference ${env:NAME} or ${file:path}; credentials are never written in the config")
	}
}

func (validation *validation) chain(name string, chain ChainConfig, sources map[string]SourceConfig) {
	path := "chains." + name
	if len(chain.From) == 0 {
		validation.fail(path+".from", "at least one entry is required")
	}
	for index, selector := range chain.From {
		validation.selector(fmt.Sprintf("%s.from[%d]", path, index), selector, sources)
	}
}

func (validation *validation) selector(path string, selector SelectorConfig, sources map[string]SourceConfig) {
	sourceTypes := selector.sourceTypes()
	if len(sourceTypes) != 1 {
		validation.fail(path, "exactly one of topic, group, workload, database must be set")

		return
	}
	if selector.Source == "" {
		return
	}
	source, found := sources[selector.Source]
	if !found {
		validation.fail(path+".source", "unknown source %q", selector.Source)

		return
	}
	if source.Type() != sourceTypes[0] {
		validation.fail(path+".source", "source %q is %s, expected %s", selector.Source, source.Type(), sourceTypes[0])
	}
}

func (validation *validation) rule(name string, rule RuleConfig) {
	switch rule.Severity {
	case "", SeverityError, SeverityWarning, SeverityOff:
	default:
		validation.fail("rules."+name+".severity", "must be %s, %s or %s", SeverityError, SeverityWarning, SeverityOff)
	}
}
