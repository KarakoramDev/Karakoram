// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"encoding/json"
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

var diagnosticURL = regexp.MustCompile(`(?i)[a-z][a-z0-9+.-]*://\S+`)
var diagnosticOpaque = regexp.MustCompile(`[A-Za-z0-9_+/=-]{48,}`)

// APIError is a credential-silent Dokploy failure. Code and Message contain only
// bounded, sanitized diagnostics, never the raw response or request body.
type APIError struct {
	StatusCode int
	Code       string
	Message    string
	class      error
}

func (e *APIError) Error() string { return e.SafeDetail() }
func (e *APIError) SafeDetail() string {
	if e.Message != "" {
		return "dokploy: " + e.Message
	}
	return "dokploy: " + e.neutral().Error()
}
func (e *APIError) Is(target error) bool { return target == e.neutral() }
func (e *APIError) neutral() error {
	if e.class != nil {
		return e.class
	}
	return errorClass(e.StatusCode, e.Code)
}

// Unwrap exposes stable rejection details to callers using errors.As.
func (e *APIError) Unwrap() error {
	if e.neutral() == infra.ErrRejected {
		return &infra.RejectedError{Code: e.Code, Message: e.SafeDetail()}
	}
	return nil
}

func errorClass(status int, code string) error {
	if status == http.StatusUnauthorized || status == http.StatusForbidden {
		return infra.ErrUnauthorized
	}
	switch code {
	case "UNAUTHORIZED", "FORBIDDEN":
		return infra.ErrUnauthorized
	case "NOT_FOUND":
		return infra.ErrNotFound
	case "CONFLICT":
		return infra.ErrConflict
	case "BAD_REQUEST", "UNPROCESSABLE_CONTENT":
		return infra.ErrRejected
	}
	switch status {
	case http.StatusNotFound:
		return infra.ErrNotFound
	case http.StatusConflict:
		return infra.ErrConflict
	case http.StatusBadRequest, http.StatusUnprocessableEntity:
		return infra.ErrRejected
	default:
		return infra.ErrUnavailable
	}
}

func silentFailure(message string) error {
	return &APIError{Message: message, class: infra.ErrUnavailable}
}

func sanitizedMessage(message string, secrets []string) string {
	// Longest first avoids exposing the suffix of a value containing another.
	sort.Slice(secrets, func(i, j int) bool { return len(secrets[i]) > len(secrets[j]) })
	for _, value := range secrets {
		if value != "" {
			message = strings.ReplaceAll(message, value, "[redacted]")
		}
	}
	lower := strings.ToLower(message)
	for _, word := range []string{"token", "password", "passwd", "secret", "credential", "authorization", "bearer", "private key", "privatekey", "api-key", "apikey", "x-api-key", "cookie", "signature", "environment", "dotenv", "script", "command"} {
		if strings.Contains(lower, word) {
			return "platform rejected the request; sensitive detail omitted"
		}
	}
	message = diagnosticURL.ReplaceAllString(message, "[redacted URL]")
	message = diagnosticOpaque.ReplaceAllString(message, "[redacted]")
	message = strings.Join(strings.FieldsFunc(message, func(r rune) bool { return unicode.IsSpace(r) || unicode.IsControl(r) }), " ")
	if len(message) > diagnosticLimit {
		message = message[:diagnosticLimit-3]
		for !utf8.ValidString(message) {
			message = message[:len(message)-1]
		}
		message += "..."
	}
	return message
}

func responseError(status int, body []byte, token string, values []string, requestJSON []byte) error {
	var envelope struct {
		Code    string `json:"code"`
		Message string `json:"message"`
	}
	_ = json.Unmarshal(body, &envelope)
	// Only platform-defined codes may escape, never arbitrary response values.
	code := ""
	switch envelope.Code {
	case "UNAUTHORIZED", "FORBIDDEN", "NOT_FOUND", "CONFLICT", "BAD_REQUEST", "UNPROCESSABLE_CONTENT", "TOO_MANY_REQUESTS", "INTERNAL_SERVER_ERROR", "SERVICE_UNAVAILABLE":
		code = envelope.Code
	}
	values = append(values, token)
	sensitivePayload := false
	// Request values are inspected only on failure, and never retained in errors.
	if len(requestJSON) != 0 {
		var request any
		if json.Unmarshal(requestJSON, &request) == nil {
			var collect func(any)
			collect = func(value any) {
				switch value := value.(type) {
				case string:
					values = append(values, value)
				case map[string]any:
					for key, child := range value {
						switch strings.ToLower(key) {
						case "env", "environment", "buildargs", "buildsecrets", "password", "token", "secret", "credential", "authorization", "privatekey", "apikey":
							sensitivePayload = true
						}
						collect(child)
					}
				case []any:
					for _, child := range value {
						collect(child)
					}
				}
			}
			collect(request)
		}
	}
	message := ""
	if sensitivePayload {
		// Platforms may echo decoded, unquoted or partially transformed secret
		// values. Redacting the serialized payload cannot make those messages
		// safe, so retain only the status/code and a fixed diagnostic.
		message = "platform request failed; sensitive detail omitted"
	} else {
		message = sanitizedMessage(envelope.Message, values)
	}
	if message == "" {
		message = fmt.Sprintf("platform request failed (HTTP %d)", status)
	}
	return &APIError{StatusCode: status, Code: code, Message: message, class: errorClass(status, code)}
}
