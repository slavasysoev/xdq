package lock

import (
	"bytes"
	"cmp"
	"errors"
	"fmt"
	"io"
	"maps"
	"os"
	"regexp"
	"slices"
	"strconv"
	"strings"

	"go.yaml.in/yaml/v3"
)

// header is the first line of every lockfile.
const header = "# Written by xdq lock: the chains in a known-good state. Do not edit.\n"

// plainPattern matches the values that are written without quotes. A colon is allowed only
// before another character, as in "topic:main/orders.created": followed by a space or the end of
// the value it would start a mapping. Everything else is double-quoted.
var plainPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_./@-]*(:[A-Za-z0-9_./@-]+)*$`)

// Encode returns the text of the lockfile: YAML with sorted chains and one edge per line, so an
// unchanged system gives a byte-identical file and a change is a one-line diff.
//
// The text is written by hand: the YAML library breaks lines longer than 80 characters and
// cannot be told not to, so an edge would not stay on one line.
func (file File) Encode() []byte {
	var text strings.Builder
	text.WriteString(header)
	text.WriteString("version: " + strconv.Itoa(file.Version) + "\n")
	if file.Environment != "" {
		text.WriteString("environment: " + scalar(file.Environment) + "\n")
	}
	if len(file.Chains) == 0 {
		text.WriteString("chains: {}\n")

		return []byte(text.String())
	}
	text.WriteString("chains:\n")
	for _, name := range slices.Sorted(maps.Keys(file.Chains)) {
		text.WriteString("  " + scalar(name) + ":\n")
		edges := sortedEdges(file.Chains[name].Edges)
		if len(edges) == 0 {
			text.WriteString("    edges: []\n")

			continue
		}
		text.WriteString("    edges:\n")
		for _, edge := range edges {
			text.WriteString("      - {from: " + scalar(string(edge.From)) + ", kind: " + scalar(string(edge.Kind)) + ", to: " + scalar(string(edge.To)) + "}\n")
		}
	}

	return []byte(text.String())
}

// Write writes the lockfile to path.
func Write(path string, file File) error {
	// The lockfile is committed to a repository and holds no secrets, so it is readable by all.
	//nolint:gosec // See the comment above.
	err := os.WriteFile(path, file.Encode(), 0o644)
	if err != nil {
		return fmt.Errorf("write lockfile: %w", err)
	}

	return nil
}

// Read reads the lockfile at path. A missing file gives an error matching fs.ErrNotExist.
func Read(path string) (File, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return File{}, fmt.Errorf("read lockfile: %w", err)
	}
	file, err := parse(data)
	if err != nil {
		return File{}, fmt.Errorf("lockfile %s: %w", path, err)
	}

	return file, nil
}

// parse decodes a lockfile. Unknown fields are errors, as in the config.
func parse(data []byte) (File, error) {
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var file File
	err := decoder.Decode(&file)
	if errors.Is(err, io.EOF) {
		return File{}, errors.New("lockfile is empty")
	}
	if err != nil {
		return File{}, err
	}
	if file.Version != Version {
		return File{}, fmt.Errorf("unsupported version %d, want %d", file.Version, Version)
	}

	return file, nil
}

func sortedEdges(edges []Edge) []Edge {
	sorted := slices.Clone(edges)
	slices.SortFunc(sorted, func(left, right Edge) int {
		return cmp.Or(cmp.Compare(left.From, right.From), cmp.Compare(left.Kind, right.Kind), cmp.Compare(left.To, right.To))
	})

	return slices.Compact(sorted)
}

// scalar returns the value as a YAML scalar: plain when that reads back as the same string,
// double-quoted otherwise. Go escapes in strconv.Quote are valid YAML escapes.
func scalar(value string) string {
	switch strings.ToLower(value) {
	case "true", "false", "null":
		return strconv.Quote(value)
	}
	if plainPattern.MatchString(value) {
		return value
	}

	return strconv.Quote(value)
}
