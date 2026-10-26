// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package redact

import (
	"bytes"
	"encoding/json"
	"strings"
)

var detailKeyNormalizer = strings.NewReplacer("_", "", "-", "", ".", "")

func sensitiveDetailKey(key string) bool {
	if credentialField.MatchString(key + ":") {
		return true
	}
	normalized := strings.ToLower(detailKeyNormalizer.Replace(key))
	for _, suffix := range []string{"password", "secret", "token", "privatekey", "secretkey", "accesskey", "apikey"} {
		if strings.HasSuffix(normalized, suffix) {
			return true
		}
	}
	switch normalized {
	case "key", "apikey", "privatekey", "secretkey", "accesskey", "accesskeyid", "clientsecret", "clientkey", "signingkey":
		return true
	}
	return false
}

func detailIdentity(key string) bool {
	switch key {
	case "id", "operationId", "operation", "requestId", "name", "namespace", "cluster", "actor", "resource", "target", "path", "digest", "checksum", "sha256", "hash", "requestHash":
		return true
	}
	return false
}

// EventDetailsJSON is the credential-key-aware boundary for public event detail
// documents, including audit before/after values. It must not be used for engine
// state, resource records, checkpoints or payloads. Unknown numbers retain precision.
func EventDetailsJSON(data json.RawMessage) json.RawMessage {
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
		sensitiveChange := false
		var path string
		if json.Unmarshal(fields["path"], &path) == nil {
			sensitiveChange = sensitiveDetailKey(path)
		}
		for key, raw := range fields {
			next := raw
			if sensitiveDetailKey(key) || sensitiveChange && (key == "before" || key == "after") {
				next = json.RawMessage(`"[redacted]"`)
			} else if !detailIdentity(key) {
				next = EventDetailsJSON(raw)
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
			next := EventDetailsJSON(raw)
			if !bytes.Equal(next, raw) {
				values[i] = next
				changed = true
			}
		}
		if changed {
			result, _ := json.Marshal(values)
			return result
		}
	case '"':
		var text string
		if json.Unmarshal(data, &text) == nil {
			if safe := Diagnostic(text); safe != text {
				result, _ := json.Marshal(safe)
				return result
			}
		}
	}
	return data
}
