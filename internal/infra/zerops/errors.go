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

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const diagnosticLimit = 1024

var diagnosticURL = regexp.MustCompile(`(?i)(?:https?|mongodb(?:\+srv)?|s3)://\S+`)
var diagnosticOpaque = regexp.MustCompile(`[A-Za-z0-9_+/=-]{48,}`)
var diagnosticCode = regexp.MustCompile(`^[A-Za-z][A-Za-z0-9]{0,79}$`)
var diagnosticResourceField = regexp.MustCompile(`^(?:(?:verticalAutoscaling\.)?(?:cpuMode|startCpuCoreCount|(?:minResource|maxResource|minFreeResource)\.(?:cpuCoreCount|memoryGBytes|diskGBytes))|(?:horizontalAutoscaling\.)?(?:minContainerCount|maxContainerCount))$`)
var diagnosticValidationMessage = regexp.MustCompile(`^Value must [A-Za-z0-9 \[\]().,_]+$`)
var diagnosticValidationOpaque = regexp.MustCompile(`[A-Za-z0-9_]{20,}`)

func sensitiveDiagnostic(s string) bool {
	s = strings.ToLower(s)
	for _, word := range []string{"token", "password", "passwd", "secret", "credential", "authorization", "bearer", "privatekey", "private key", "accesskey", "apikey", "signingkey", "masterkey", "cookie", "signature", "connectionstring", "dsn", "env", "userdata", "user-data", "content", "yaml", "command", "script"} {
		if strings.Contains(s, word) {
			return true
		}
	}
	return false
}

func boundedDiagnostic(s string) string { return truncateDiagnostic(s, diagnosticLimit) }

func truncateDiagnostic(s string, limit int) string {
	if len(s) <= limit {
		return s
	}
	s = s[:limit-3]
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
				if diagnosticResourceField.MatchString(path) {
					return
				}
				for _, key := range []string{"field", "parameter"} {
					if field, ok := v[key].(string); ok && sensitiveDiagnostic(field) {
						return
					}
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
					case "code", "error", "message", "reason", "field", "parameter":
						walk(v[key], key, metadata, depth+1)
					case "minDisk", "maxDisk", "min", "max", "value", "provided", "actual":
						if key == "value" || key == "provided" || key == "actual" {
							switch v[key].(type) {
							case float64, bool:
							case string:
								if text := v[key].(string); text != "SHARED" && text != "DEDICATED" {
									continue
								}
								field, _ := v["field"].(string)
								parameter, _ := v["parameter"].(string)
								if !diagnosticResourceField.MatchString(field) && !diagnosticResourceField.MatchString(parameter) {
									continue
								}
							default:
								continue
							}
						}
						if metadata {
							name := key
							if path != "" {
								name = path + "." + key
							}
							walk(v[key], name, true, depth+1)
						}
					default:
						if metadata && diagnosticResourceField.MatchString(key) {
							walk(v[key], key, true, depth+1)
						}
					}
				}
			case string:
				if metadata && (path == "code" || path == "error" || path == "message") {
					text := strings.TrimSpace(v)
					if strings.EqualFold(text, strings.TrimSpace(code)) || strings.EqualFold(text, strings.TrimSpace(message)) {
						return
					}
				}
				if diagnosticResourceField.MatchString(path) && (len(v) > 256 || !diagnosticValidationMessage.MatchString(v) || diagnosticValidationOpaque.MatchString(v)) {
					return
				}
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

type resourceUpdateError struct {
	cause     error
	action    string
	hostname  string
	attempted string
}

func (e *resourceUpdateError) Error() string {
	return rejectedUpdateMessage(e.action, e.hostname, e.cause.Error(), e.attempted)
}

func (e *resourceUpdateError) Unwrap() error { return e.cause }

func rejectedUpdate(err error, action, hostname, attempted string) error {
	var api *APIError
	if err == nil || !errors.As(err, &api) {
		return err
	}
	return &resourceUpdateError{cause: err, action: action, hostname: hostname, attempted: attempted}
}

func rejectedUpdateMessage(action, hostname, detail, attempted string) string {
	prefix := "Zerops rejected the " + action
	if hostname != "" {
		prefix += " for " + hostname
	}
	suffix := ""
	if attempted != "" {
		suffix = "; attempted " + truncateDiagnostic(attempted, 384)
	}
	return prefix + ": " + truncateDiagnostic(detail, diagnosticLimit-len(prefix)-2-len(suffix)) + suffix
}

// attemptedResources reads only resource scalars from the final request, never
// arbitrary configuration, environment values or platform identifiers.
func attemptedResources(config map[string]json.RawMessage) string {
	v, _ := decodeObject(config["verticalAutoscaling"])
	h, _ := decodeObject(config["horizontalAutoscaling"])
	low, _ := decodeObject(v["minResource"])
	high, _ := decodeObject(v["maxResource"])
	scalar := func(m map[string]json.RawMessage, key string) string {
		var n json.Number
		if json.Unmarshal(m[key], &n) == nil && len(n.String()) <= 24 {
			if _, err := n.Float64(); err == nil {
				return n.String()
			}
		}
		return "unset"
	}
	mode := "unset"
	var value string
	if json.Unmarshal(v["cpuMode"], &value) == nil && (value == "SHARED" || value == "DEDICATED") {
		mode = value
	}
	return fmt.Sprintf("cpuMode=%s cpu=%s-%s startCpu=%s ram=%s-%s disk=%s-%s containers=%s-%s",
		mode, scalar(low, "cpuCoreCount"), scalar(high, "cpuCoreCount"), scalar(v, "startCpuCoreCount"),
		scalar(low, "memoryGBytes"), scalar(high, "memoryGBytes"), scalar(low, "diskGBytes"), scalar(high, "diskGBytes"),
		scalar(h, "minContainerCount"), scalar(h, "maxContainerCount"))
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

// Is exposes stable backend-neutral classifications without losing API details.
func (e *APIError) Is(target error) bool {
	switch target {
	case infra.ErrPublicIPTypeUnsupported:
		return e.Code == "publicIpTypeNotSupported"
	case infra.ErrUnauthorized:
		return IsAuthenticationError(e)
	case infra.ErrNotFound:
		return notFound(e)
	case infra.ErrConflict:
		return e.StatusCode == http.StatusConflict
	case infra.ErrUnavailable:
		return e.StatusCode == http.StatusTooManyRequests || (e.StatusCode >= 500 && e.StatusCode < 600)
	case infra.ErrResourceChangeCooldown:
		return e.Code == "lastCpuModeChangeTooRecent"
	case infra.ErrRejected:
		return e.Code == "lastCpuModeChangeTooRecent" || (e.StatusCode >= 400 && e.StatusCode < 500 && !IsAuthenticationError(e) && !notFound(e) && e.StatusCode != 409 && e.StatusCode != 429)
	}
	return false
}

// SafeDetail returns the bounded diagnostic summary.
func (e *APIError) SafeDetail() string { return e.Error() }

type classifiedError struct {
	cause error
	class error
}

func (e *classifiedError) Error() string        { return e.cause.Error() }
func (e *classifiedError) Unwrap() error        { return e.cause }
func (e *classifiedError) Is(target error) bool { return target == e.class }
func (e *classifiedError) SafeDetail() string {
	if e.class == infra.ErrUnavailable {
		return "network request or platform response failed"
	}
	return e.class.Error()
}
func unavailable(err error) error { return &classifiedError{cause: err, class: infra.ErrUnavailable} }
