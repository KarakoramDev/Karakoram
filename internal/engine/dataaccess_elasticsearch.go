// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

// ValidElasticsearchDataIndexPattern validates the deliberately narrow pattern
// syntax shared by Elasticsearch data MCP admission and native tool routing.
// Only lowercase ASCII letters, digits, underscores, dots, hyphens and the
// wildcard characters '*' and '?' are allowed. Patterns must be 1–255 bytes and
// cannot start with a dot, underscore or hyphen; '_all' is therefore excluded.
func ValidElasticsearchDataIndexPattern(pattern string) bool {
	if len(pattern) == 0 || len(pattern) > 255 {
		return false
	}
	for i := range len(pattern) {
		c := pattern[i]
		if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '*' || c == '?' {
			continue
		}
		if i != 0 && (c == '_' || c == '.' || c == '-') {
			continue
		}
		return false
	}
	return true
}
