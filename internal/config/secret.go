package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"go.yaml.in/yaml/v3"
)

// Secret is a credential referenced from the config as ${env:NAME} or ${file:path}.
// Its String method returns the reference, so printing a config never reveals the value.
// Mixed receivers are intentional: Value and String stay on values so fmt never prints the value,
// while resolve and UnmarshalYAML need a pointer to store the value.
//
//nolint:recvcheck // See the comment above.
type Secret struct {
	reference string
	value     string
}

// Value returns the resolved credential.
func (secret Secret) Value() string {
	return secret.value
}

// String returns the reference, never the value.
func (secret Secret) String() string {
	return secret.reference
}

// UnmarshalYAML stores the reference as written. It is resolved after validation.
func (secret *Secret) UnmarshalYAML(node *yaml.Node) error {
	return node.Decode(&secret.reference)
}

const (
	schemeEnv  = "env"
	schemeFile = "file"
)

// parseReference splits "${env:NAME}" into "env" and "NAME".
func parseReference(reference string) (scheme, target string, isReference bool) {
	inner, hasPrefix := strings.CutPrefix(reference, "${")
	inner, hasSuffix := strings.CutSuffix(inner, "}")
	if !hasPrefix || !hasSuffix {
		return "", "", false
	}
	scheme, target, hasColon := strings.Cut(inner, ":")
	if !hasColon || target == "" || (scheme != schemeEnv && scheme != schemeFile) {
		return "", "", false
	}

	return scheme, target, true
}

// resolve reads the value. Errors name the reference, never the value.
// A relative file path is relative to baseDir. One trailing newline is removed from a file,
// because secret files usually end with one.
func (secret *Secret) resolve(baseDir string) error {
	scheme, target, _ := parseReference(secret.reference)
	switch scheme {
	case schemeEnv:
		value, found := os.LookupEnv(target)
		if !found {
			return fmt.Errorf("environment variable %s is not set", target)
		}
		secret.value = value
	case schemeFile:
		path := target
		if !filepath.IsAbs(path) {
			path = filepath.Join(baseDir, path)
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("read secret file: %w", err)
		}
		value := strings.TrimSuffix(string(content), "\n")
		secret.value = strings.TrimSuffix(value, "\r")
	}

	return nil
}
