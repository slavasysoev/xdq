package graph

import (
	"strings"
	"unicode"
)

// tokenSeparators split configuration text into words that may be topic names (§7.2). Brackets
// split too, so a YAML flow list "[orders, invoices]" gives clean names. Dots, dashes and
// underscores are part of topic names and do not split.
const tokenSeparators = ",;:=\"'[]{}()"

// Tokens returns the distinct words of the value, in order of appearance.
func (value ConfigValue) Tokens() []string {
	fields := strings.FieldsFunc(value.Value, func(character rune) bool {
		return unicode.IsSpace(character) || strings.ContainsRune(tokenSeparators, character)
	})
	seen := make(map[string]bool, len(fields))
	var tokens []string
	for _, field := range fields {
		if !seen[field] {
			seen[field] = true
			tokens = append(tokens, field)
		}
	}

	return tokens
}
