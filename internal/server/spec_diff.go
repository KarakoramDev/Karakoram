// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type specChange struct {
	Path      string `json:"path"`
	Before    any    `json:"before,omitempty"`
	After     any    `json:"after,omitempty"`
	Sensitive bool   `json:"sensitive,omitempty"`
	Truncated bool   `json:"truncated,omitempty"`
}

type specDetails struct {
	Changes   []specChange `json:"changes"`
	Truncated bool         `json:"truncated,omitempty"`
}

// These write-only schema paths are checked against ClusterSpec and its input
// schemas in tests. Opaque engine configuration is masked as a whole because it
// can contain credentials in provider-specific syntax.
var sensitiveSpecPaths = map[string]bool{
	"tls.certificate": true, "tls.privateKey": true,
	"tls.caCertificate": true, "tls.caPrivateKey": true,
	"engineConfig": true,
}

var sensitivePathNormalizer = strings.NewReplacer("_", "", "-", "", " ", "", ".", "", "[", "", "]", "", "/", "")

func sensitiveSpecPath(path string) bool {
	if sensitiveSpecPaths[path] {
		return true
	}
	// Free-form engine option keys can contain dotted sensitive parents.
	// Elsewhere only schema keys, never user-selected identities, are matched.
	key := strings.ToLower(path)
	if !strings.HasPrefix(key, "engineoptions.") {
		keyPath := specPathWithoutSelectors(path)
		if strings.HasPrefix(keyPath, "components.extra.") && !strings.Contains(strings.TrimPrefix(keyPath, "components.extra."), ".") {
			return false
		}
		path = keyPath[strings.LastIndex(keyPath, ".")+1:]
		key = strings.ToLower(path)
	}
	if key == "key" || key == "pem" || strings.HasSuffix(key, ".key") || strings.HasSuffix(key, ".pem") {
		return true
	}
	if strings.HasPrefix(key, "engineoptions.") {
		field := key[strings.LastIndex(key, ".")+1:]
		if strings.Contains(field, "config") || strings.Contains(field, "source") || strings.HasSuffix(field, "script") || field == "pipeline" {
			return true
		}
	}
	if (strings.Contains(key, "auth") || strings.Contains(key, "pass")) && sensitiveCredentialPath(path) {
		return true
	}
	key = sensitivePathNormalizer.Replace(key)
	for _, word := range []string{"password", "passwd", "passphrase", "pwd", "webhook", "secret", "token", "credential", "privatekey", "keymaterial", "accesskey", "apikey", "certificate", "signingkey", "encryptionkey", "keyfile", "keystore", "truststore"} {
		if strings.Contains(key, word) {
			return true
		}
	}
	return false
}

func specPathWithoutSelectors(path string) string {
	if !strings.Contains(path, "[") {
		return path
	}
	var out strings.Builder
	out.Grow(len(path))
	inSelector := false
	for _, r := range path {
		switch r {
		case '[':
			inSelector = true
		case ']':
			inSelector = false
		default:
			if !inSelector {
				out.WriteRune(r)
			}
		}
	}
	return out.String()
}

func sensitiveCredentialPath(path string) bool {
	for _, token := range strings.FieldsFunc(path, func(r rune) bool { return !unicode.IsLetter(r) && !unicode.IsDigit(r) }) {
		switch strings.ToLower(token) {
		case "auth", "authentication", "authorization", "pass":
			return true
		}
		for i := 0; i+4 <= len(token); i++ {
			if token[i:i+4] != "Auth" && token[i:i+4] != "Pass" && !(i == 0 && (token[:4] == "auth" || token[:4] == "pass")) {
				continue
			}
			end := i + 4
			if end == len(token) || token[end] >= 'A' && token[end] <= 'Z' {
				return true
			}
		}
	}
	return false
}

func sensitiveSpecValue(path string, value any) bool {
	if sensitiveSpecPath(path) {
		return true
	}
	key := strings.ToLower(path)
	if !strings.HasPrefix(key, "engineoptions.") || !strings.Contains(key, ".pipelines") {
		return false
	}
	field := key[strings.LastIndex(key, ".")+1:]
	if field == "id" || field == "name" {
		return false
	}
	switch value := value.(type) {
	case string:
		return true
	case []any:
		for _, item := range value {
			if _, ok := item.(string); ok {
				return true
			}
		}
	}
	return false
}

func maskedValue(before, after any) (any, any) {
	if before == nil {
		return "(unset)", "(set)"
	}
	if after == nil {
		return "(set)", "(unset)"
	}
	return "(changed)", "(changed)"
}

func safeSpecValue(path string, v any) any {
	if v == nil {
		return nil
	}
	if sensitiveSpecValue(path, v) {
		return "(set)"
	}
	switch v := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, item := range v {
			out[k] = safeSpecValue(joinSpecPath(path, k), item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = safeSpecValue(path, item)
		}
		return out
	default:
		return v
	}
}

func joinSpecPath(parent, child string) string {
	if parent == "" {
		return child
	}
	return parent + "." + child
}

func namedSpecArray(v any) (map[string]any, bool) {
	if v == nil {
		return map[string]any{}, true
	}
	list, ok := v.([]any)
	if !ok {
		return nil, false
	}
	out := make(map[string]any, len(list))
	for _, item := range list {
		object, ok := item.(map[string]any)
		if !ok {
			return nil, false
		}
		name, ok := object["name"].(string)
		if !ok || name == "" {
			name, ok = object["id"].(string)
			if !ok || name == "" {
				return nil, false
			}
		}
		if _, exists := out[name]; exists {
			return nil, false
		}
		out[name] = item
	}
	return out, true
}

func diffSpecValues(path string, before, after any, changes *[]specChange) {
	if reflect.DeepEqual(before, after) {
		return
	}
	if sensitiveSpecValue(path, before) || sensitiveSpecValue(path, after) {
		b, a := maskedValue(before, after)
		*changes = append(*changes, specChange{Path: path, Before: b, After: a, Sensitive: true})
		return
	}
	bmap, bok := before.(map[string]any)
	amap, aok := after.(map[string]any)
	if (bok || before == nil) && (aok || after == nil) && (bok || aok) {
		keys := make(map[string]bool, len(bmap)+len(amap))
		for k := range bmap {
			keys[k] = true
		}
		for k := range amap {
			keys[k] = true
		}
		if len(keys) == 0 {
			*changes = append(*changes, specChange{Path: path, Before: before, After: after})
			return
		}
		for _, k := range sortedSpecKeys(keys) {
			diffSpecValues(joinSpecPath(path, k), bmap[k], amap[k], changes)
		}
		return
	}
	if b, ok := namedSpecArray(before); ok {
		if a, ok := namedSpecArray(after); ok {
			keys := make(map[string]bool, len(b)+len(a))
			for k := range b {
				keys[k] = true
			}
			for k := range a {
				keys[k] = true
			}
			if len(keys) == 0 {
				*changes = append(*changes, specChange{Path: path, Before: before, After: after})
				return
			}
			for _, k := range sortedSpecKeys(keys) {
				p := path + "[" + k + "]"
				if b[k] == nil || a[k] == nil {
					*changes = append(*changes, specChange{Path: p, Before: safeSpecValue(p, b[k]), After: safeSpecValue(p, a[k])})
				} else {
					diffSpecValues(p, b[k], a[k], changes)
				}
			}
			return
		}
	}
	*changes = append(*changes, specChange{Path: path, Before: safeSpecValue(path, before), After: safeSpecValue(path, after)})
}

func sortedSpecKeys(keys map[string]bool) []string {
	out := make([]string, 0, len(keys))
	for k := range keys {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func boundedSpecText(s string, max int) string {
	if len(s) <= max {
		return s
	}
	end := max - len("…")
	for end > 0 && !utf8.RuneStart(s[end]) {
		end--
	}
	return s[:end] + "…"
}

func specValueText(v any) string {
	if v == nil {
		return "(unset)"
	}
	if s, ok := v.(string); ok {
		return boundedSpecText(s, 64)
	}
	b, _ := json.Marshal(v)
	return boundedSpecText(string(b), 64)
}

func specChangeText(c specChange) string {
	if strings.HasSuffix(c.Path, "]") && (c.Before == nil || c.After == nil) {
		if c.Before == nil {
			return c.Path + ": added"
		}
		return c.Path + ": removed"
	}
	if c.Sensitive {
		if c.Before == "(changed)" {
			return c.Path + ": (changed)"
		}
		if c.After == "(set)" {
			return c.Path + ": (set)"
		}
		return c.Path + ": (unset)"
	}
	return c.Path + ": " + specValueText(c.Before) + " → " + specValueText(c.After)
}

func clusterSpecAuditEvent(name string, generation int64, before, after domain.ClusterSpec, secrets []store.SpecSecretChange) domain.Event {
	var b, a any
	// Domain specs use the API JSON names and exclude write-only PEM inputs.
	oldJSON, _ := json.Marshal(before)
	newJSON, _ := json.Marshal(after)
	_ = json.Unmarshal(oldJSON, &b)
	_ = json.Unmarshal(newJSON, &a)
	details := specDetails{Changes: make([]specChange, 0)}
	diffSpecValues("", b, a, &details.Changes)
	paths := map[string]string{"tls/user.crt": "tls.certificate", "tls/user.key": "tls.privateKey", "tls/ca.crt": "tls.caCertificate", "tls/ca.key": "tls.caPrivateKey"}
	for _, secret := range secrets {
		path := paths[secret.Key]
		if path == "" {
			path = secret.Key
		}
		old, next := "(unset)", "(set)"
		if secret.WasSet {
			old, next = "(changed)", "(changed)"
		}
		details.Changes = append(details.Changes, specChange{Path: path, Before: old, After: next, Sensitive: true})
	}
	sort.Slice(details.Changes, func(i, j int) bool { return details.Changes[i].Path < details.Changes[j].Path })
	message := "Cluster spec change requested for " + name + ": no changes"
	if len(details.Changes) > 0 {
		count := min(3, len(details.Changes))
		prefix := fmt.Sprintf("Cluster spec changed for %s (generation %d): ", boundedSpecText(name, 64), generation)
		suffix := ""
		if remaining := len(details.Changes) - count; remaining > 0 {
			suffix = fmt.Sprintf(" and %d more", remaining)
		}
		parts := make([]string, count)
		perChange := (512 - len(prefix) - len(suffix) - 2*(count-1)) / count
		for i := range parts {
			parts[i] = boundedSpecText(specChangeText(details.Changes[i]), perChange)
		}
		message = prefix + strings.Join(parts, "; ") + suffix
	}
	data := boundedSpecDetails(details)
	return domain.Event{Cluster: name, Resource: "clusters", Target: name, Type: "Normal", Reason: "ClusterSpecChangeRequested", Message: boundedSpecText(message, 512), Details: data}
}

const (
	maxSpecChangeValueBytes = 1024
	maxSpecDetailsBytes     = 32 * 1024
)

// Values are already redacted before sizing: a sensitive input must never
// disclose its original length through the truncation marker.
func boundedSpecDetails(details specDetails) json.RawMessage {
	for i := range details.Changes {
		change := &details.Changes[i]
		for _, value := range []*any{&change.Before, &change.After} {
			encoded, _ := json.Marshal(*value)
			if len(encoded) > maxSpecChangeValueBytes {
				*value = fmt.Sprintf("(%d bytes)", len(encoded))
				change.Truncated, details.Truncated = true, true
			}
		}
	}
	data, _ := json.Marshal(details)
	if len(data) <= maxSpecDetailsBytes {
		return data
	}
	// Retain paths first when a large change set exceeds the total budget.
	// If even paths do not fit, retain their ordered prefix and flag omissions.
	details.Truncated = true
	for i := range details.Changes {
		change := &details.Changes[i]
		change.Before, change.After = nil, nil
		change.Truncated = true
		if len(change.Path) > maxSpecChangeValueBytes {
			change.Path = boundedSpecText(change.Path, maxSpecChangeValueBytes)
		}
	}
	empty, _ := json.Marshal(specDetails{Changes: []specChange{}, Truncated: true})
	size, kept := len(empty), 0
	for _, change := range details.Changes {
		encoded, _ := json.Marshal(change)
		added := len(encoded)
		if kept != 0 {
			added++
		}
		if size+added > maxSpecDetailsBytes {
			break
		}
		size += added
		kept++
	}
	details.Changes = details.Changes[:kept]
	data, _ = json.Marshal(details)
	return data
}
