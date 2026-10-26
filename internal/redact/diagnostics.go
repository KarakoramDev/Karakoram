// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package redact

import (
	"bytes"
	"encoding/json"
)

// candidate checks ASCII case-insensitively without allocating a lowercase copy.
// Backslashes require inspection because JSON may encode field names as Unicode.
func candidate(text string) bool {
	for i := range len(text) {
		c := text[i]
		if c == '\\' {
			return true
		}
		for _, marker := range nativeIndicators {
			if len(text)-i < len(marker) {
				continue
			}
			match := true
			for j := range marker {
				c := text[i+j]
				if c >= 'A' && c <= 'Z' {
					c += 'a' - 'A'
				}
				if c != marker[j] {
					match = false
					break
				}
			}
			if match {
				return true
			}
		}
	}
	return hasVerifier(text)
}

// Diagnostic is the allocation-free ordinary-message read/serve fast path.
func Diagnostic(text string) string {
	if !candidate(text) {
		return text
	}
	return redactVerifiers(NativeError(text))
}

// DiagnosticsJSON sanitizes diagnostic fields, not resource identities, payloads,
// timestamps or numeric values. Unchanged documents retain their exact bytes.
// Unknown fields survive migration, including numbers larger than float64 can hold.
func DiagnosticsJSON(data json.RawMessage) json.RawMessage {
	if !candidate(string(data)) && !bytes.Contains(data, []byte(`"details"`)) {
		return data
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 {
		return data
	}
	changed := false
	switch trimmed[0] {
	case '{':
		var fields map[string]json.RawMessage
		if json.Unmarshal(data, &fields) != nil {
			return data
		}
		for key, raw := range fields {
			next := raw
			switch key {
			case "details":
				next = EventDetailsJSON(raw)
			case "message", "error", "step", "lastError", "blockingMessage", "firstError", "warning":
				var text string
				if json.Unmarshal(raw, &text) == nil {
					if safe := Diagnostic(text); safe != text {
						next, _ = json.Marshal(safe)
					}
				}
			default:
				// Arbitrary operation payloads and provider checkpoints are not public diagnostics.
				if key != "payload" && key != "checkpoint" && key != "spec" && key != "internal" {
					next = DiagnosticsJSON(raw)
				}
			}
			if !bytes.Equal(next, raw) {
				fields[key] = next
				changed = true
			}
		}
		if changed {
			result, _ := json.Marshal(fields)
			return result
		}
	case '[':
		var values []json.RawMessage
		if json.Unmarshal(data, &values) != nil {
			return data
		}
		for i, raw := range values {
			next := DiagnosticsJSON(raw)
			if !bytes.Equal(raw, next) {
				values[i] = next
				changed = true
			}
		}
		if changed {
			result, _ := json.Marshal(values)
			return result
		}
	}
	return data
}
