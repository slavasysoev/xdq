package config

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Load reads, validates and resolves the config at path. Unknown fields are errors, so a typo
// in the config is reported instead of silently ignored. All validation problems are reported
// at once.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config: %w", err)
	}
	config, err := parse(data, filepath.Dir(path))
	if err != nil {
		return nil, fmt.Errorf("config %s:\n%w", path, err)
	}

	return config, nil
}

// parse does everything Load does except reading the file. baseDir resolves relative ${file:} paths.
func parse(data []byte, baseDir string) (*Config, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var config Config
	err := decoder.Decode(&config)
	if errors.Is(err, io.EOF) {
		return nil, errors.New("config is empty")
	}
	if err != nil {
		return nil, userFacingYAMLError(err)
	}

	err = validate(&config)
	if err != nil {
		return nil, err
	}
	err = resolveSources(&config, baseDir)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

var unknownFieldPattern = regexp.MustCompile(`field (\S+) not found in type \S+`)

// userFacingYAMLError hides Go type names from yaml errors:
// "line 4: field rabbitmq not found in type config.Source" becomes "line 4: unknown field rabbitmq".
func userFacingYAMLError(err error) error {
	var typeError *yaml.TypeError
	if !errors.As(err, &typeError) {
		return err
	}
	messages := make([]string, len(typeError.Errors))
	for index, message := range typeError.Errors {
		messages[index] = unknownFieldPattern.ReplaceAllString(message, "unknown field $1")
	}

	return errors.New(strings.Join(messages, "\n"))
}

// resolveSources resolves every source in name order, so errors come in a stable order.
func resolveSources(config *Config, baseDir string) error {
	var problems []error
	for _, name := range slices.Sorted(maps.Keys(config.Sources)) {
		// Validation guarantees exactly one type is set.
		settings := config.Sources[name].settings()[0]
		err := settings.resolve(baseDir, "sources."+name+"."+settings.Type())
		if err != nil {
			problems = append(problems, err)
		}
	}

	return errors.Join(problems...)
}

// resolvePath makes a file path relative to the config file, so xdq works from any directory.
func resolvePath(baseDir, path string) string {
	if path == "" || filepath.IsAbs(path) {
		return path
	}

	return filepath.Join(baseDir, path)
}

func resolveSecret(baseDir, path string, secret *Secret) error {
	err := secret.resolve(baseDir)
	if err != nil {
		return fmt.Errorf("%s: %w", path, err)
	}

	return nil
}
