// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package zerops

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"
)

const diagnosticLimit = 1024

var diagnosticURL = regexp.MustCompile(`(?i)(?:https?|mongodb(?:\+srv)?|s3)://\S+`)
var diagnosticOpaque = regexp.MustCompile(`[A-Za-z0-9_+/=-]{48,}`)
var diagnosticCode = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,79}$`)

func sensitiveDiagnostic(s string) bool {
	s = strings.ToLower(s)
	for _, word := range []string{"token", "password", "passwd", "secret", "credential", "authorization", "bearer", "privatekey", "private key", "accesskey", "apikey", "signingkey", "masterkey", "cookie", "signature", "connectionstring", "dsn", "env", "userdata", "user-data", "content", "yaml", "command", "script"} {
		if strings.Contains(s, word) {
			return true
		}
	}
	return false
}

func boundedDiagnostic(s string) string {
	if len(s) <= diagnosticLimit {
		return s
	}
	s = s[:diagnosticLimit-3]
	for !utf8.ValidString(s) {
		s = s[:len(s)-1]
	}
	return s + "..."
}

// errorSummary exports diagnostic messages/codes and allowlisted metadata:
// field/reason diagnostics and known validation limits. Unknown metadata keys
// are omitted; credential-bearing details, URLs and opaque values are redacted.
func errorSummary(code, message string, meta json.RawMessage, token string) string {
	clean := func(s string) string {
		if token != "" {
			s = strings.ReplaceAll(s, token, "[redacted]")
		}
		if sensitiveDiagnostic(s) {
			return "[redacted sensitive detail]"
		}
		s = diagnosticURL.ReplaceAllString(s, "[redacted URL]")
		s = diagnosticOpaque.ReplaceAllString(s, "[redacted]")
		return strings.Join(strings.FieldsFunc(s, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }), " ")
	}
	parts := []string{}
	size := 0
	add := func(s string) {
		if s == "" || strings.EqualFold(s, "See metadata") || size >= diagnosticLimit {
			return
		}
		for _, p := range parts {
			if p == s {
				return
			}
		}
		parts = append(parts, s)
		size += len(s) + 2
	}
	if diagnosticCode.MatchString(code) && code != token {
		add(code)
	}
	add(clean(message))
	var value any
	if json.Unmarshal(meta, &value) == nil {
		var walk func(any, string, bool, int)
		walk = func(v any, path string, metadata bool, depth int) {
			if depth > 8 || size >= diagnosticLimit {
				return
			}
			switch v := v.(type) {
			case []any:
				for _, child := range v {
					walk(child, path, metadata, depth+1)
				}
			case map[string]any:
				if field, ok := v["field"].(string); ok && sensitiveDiagnostic(field) {
					return
				}
				keys := make([]string, 0, len(v))
				for key := range v {
					keys = append(keys, key)
				}
				sort.Strings(keys)
				for _, key := range keys {
					if sensitiveDiagnostic(key) {
						continue
					}
					switch key {
					case "meta", "metadata":
						walk(v[key], path, true, depth+1)
					case "code", "error", "message", "reason", "field":
						walk(v[key], key, metadata, depth+1)
					case "minDisk", "maxDisk", "min", "max", "minFreeResource.memoryGBytes":
						if metadata {
							name := key
							if path != "" {
								name = path + "." + key
							}
							walk(v[key], name, true, depth+1)
						}
					}
				}
			case string:
				if strings.EqualFold(strings.Trim(v, " ."), "See metadata") {
					return
				}
				text := clean(v)
				if metadata && path != "" {
					text = clean(path) + ": " + text
				}
				add(text)
			case float64, bool:
				if metadata && path != "" {
					add(clean(path) + ": " + fmt.Sprint(v))
				}
			}
		}
		walk(value, "", true, 0)
	}
	return boundedDiagnostic(strings.Join(parts, "; "))
}

func processErrorSummary(raw json.RawMessage, token string) string {
	var detail struct {
		Code, Message string
		Meta          json.RawMessage
		Metadata      json.RawMessage
	}
	if json.Unmarshal(raw, &detail) != nil {
		return "Platform process failed; inspect Zerops process logs"
	}
	if len(detail.Meta) == 0 {
		detail.Meta = detail.Metadata
	}
	if summary := errorSummary(detail.Code, detail.Message, detail.Meta, token); summary != "" {
		return summary
	}
	return "Platform process failed; inspect Zerops process logs"
}

// IsAuthenticationError identifies an explicit rejection, never an outage.
func IsAuthenticationError(err error) bool {
	var api *APIError
	if !errors.As(err, &api) {
		return false
	}
	if api.StatusCode == http.StatusUnauthorized || api.StatusCode == http.StatusForbidden {
		return true
	}
	switch strings.ToLower(api.Code) {
	case "unauthorized", "forbidden", "invalidtoken", "tokeninvalid", "tokenexpired", "accesstokeninvalid", "accesstokenexpired", "authenticationfailed", "permissiondenied", "accessdenied":
		return true
	}
	return false
}

// SafeErrorDetail does not expose arbitrary transport errors (which can contain URLs).
func SafeErrorDetail(err error) string {
	var api *APIError
	if errors.As(err, &api) {
		return api.Error()
	}
	return "network request or platform response failed"
}
