// Package config loads and validates xdq.yaml. The config says where to look, never what is there.
package config

// Config is the parsed and validated xdq.yaml.
type Config struct {
	Version int                     `yaml:"version"`
	Sources map[string]SourceConfig `yaml:"sources"`
	Chains  map[string]ChainConfig  `yaml:"chains"`
	Rules   map[string]RuleConfig   `yaml:"rules"`
}

// SourceConfig is one system to discover. Exactly one of the fields is set; the key names the type:
//
//	main:
//	  kafka:
//	    brokers: [...]
//
// The fields are typed (not a map) so that the YAML decoder rejects unknown keys in every block.
type SourceConfig struct {
	Kafka      *KafkaConfig      `yaml:"kafka"`
	Kubernetes *KubernetesConfig `yaml:"kubernetes"`
	Postgres   *PostgresConfig   `yaml:"postgres"`
}

// Source types.
const (
	TypeKafka      = "kafka"
	TypeKubernetes = "kubernetes"
	TypePostgres   = "postgres"
)

// SourceSettings is what every source type has to provide to the config: its type, its own
// validation and the resolution of defaults, paths and secrets. Adding a source type means
// a new field in SourceConfig, one more line in SourceConfig.settings, and a type implementing
// this interface. Nothing else in this package changes.
type SourceSettings interface {
	// Type returns one of the Type constants.
	Type() string
	// validate reports problems with the block at path.
	validate(validation *validation, path string)
	// resolve applies defaults, makes file paths absolute against baseDir and reads secrets.
	// It runs only after validate found no problems. path prefixes the errors.
	resolve(baseDir, path string) error
}

// settings returns the settings of every type that is set in the block.
func (source SourceConfig) settings() []SourceSettings {
	// Each pointer is checked before it is stored in the interface: an interface holding a nil
	// *KafkaConfig is not equal to nil, so a later nil check would pass by mistake.
	var found []SourceSettings
	if source.Kafka != nil {
		found = append(found, source.Kafka)
	}
	if source.Kubernetes != nil {
		found = append(found, source.Kubernetes)
	}
	if source.Postgres != nil {
		found = append(found, source.Postgres)
	}

	return found
}

// Type returns the type of the source, or "" when no type is set.
// It assumes the config is validated, so at most one type is set.
func (source SourceConfig) Type() string {
	found := source.settings()
	if len(found) == 0 {
		return ""
	}

	return found[0].Type()
}

// ChainConfig is a business process, named by its entry points.
type ChainConfig struct {
	From []SelectorConfig `yaml:"from"`
}

// SelectorConfig matches nodes by kind and name. Exactly one of Topic, Group, Workload, Database is set.
type SelectorConfig struct {
	Topic    string `yaml:"topic"`
	Group    string `yaml:"group"`
	Workload string `yaml:"workload"`
	Database string `yaml:"database"`
	// Source optionally restricts the selector to one source.
	Source string `yaml:"source"`
}

// sourceTypes returns the source type of every selector field that is set.
func (selector SelectorConfig) sourceTypes() []string {
	var types []string
	if selector.Topic != "" {
		types = append(types, TypeKafka)
	}
	if selector.Group != "" {
		types = append(types, TypeKafka)
	}
	if selector.Workload != "" {
		types = append(types, TypeKubernetes)
	}
	if selector.Database != "" {
		types = append(types, TypePostgres)
	}

	return types
}

// RuleConfig overrides the defaults of one rule.
type RuleConfig struct {
	// Severity is error, warning or off. Empty means the rule's default.
	Severity string `yaml:"severity"`
	// Threshold is used by rules that compare against a number. Nil means the rule's default.
	Threshold *int `yaml:"threshold"`
}

// Rule severities that can be set in the config. They are plain strings, not report.Severity:
// the config also has "off", and it must not depend on the report package.
const (
	SeverityError   = "error"
	SeverityWarning = "warning"
	SeverityOff     = "off"
)
