// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package redact

import "strings"

func base64Byte(c byte) bool {
	return c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z' || c >= '0' && c <= '9' || c == '+' || c == '/' || c == '='
}

// verifierEnd recognizes long STANDARD-base64 tokens with mixed character
// classes or base64-specific punctuation. Resource-name separators (- _ . :)
// split tokens; generated initial-restore IDs contain a 12-character UID and
// operation IDs have 24 characters, both below the 28-character threshold.
// Hexadecimal product digests/checksums remain intact.
func verifierEnd(text string, start int) int {
	end := start
	hex := true
	upper, lower, digit, encoded := false, false, false, false
	for end < len(text) && base64Byte(text[end]) {
		c := text[end]
		upper = upper || c >= 'A' && c <= 'Z'
		lower = lower || c >= 'a' && c <= 'z'
		digit = digit || c >= '0' && c <= '9'
		encoded = encoded || c == '+' || c == '/' || c == '='
		hex = hex && (c >= '0' && c <= '9' || c >= 'a' && c <= 'f' || c >= 'A' && c <= 'F')
		end++
	}
	if end-start >= 28 && !hex && (encoded || upper && lower && digit) {
		return end
	}
	return -end - 1
}

func hasVerifier(text string) bool {
	for i := 0; i < len(text); {
		if !base64Byte(text[i]) {
			i++
			continue
		}
		end := verifierEnd(text, i)
		if end >= 0 {
			return true
		}
		i = -end - 1
	}
	return false
}

func redactVerifiers(text string) string {
	var out strings.Builder
	copied := 0
	for i := 0; i < len(text); {
		if !base64Byte(text[i]) {
			i++
			continue
		}
		end := verifierEnd(text, i)
		if end < 0 {
			i = -end - 1
			continue
		}
		out.WriteString(text[copied:i])
		out.WriteString("[redacted]")
		copied = end
		i = end
	}
	if copied == 0 {
		return text
	}
	out.WriteString(text[copied:])
	return out.String()
}
