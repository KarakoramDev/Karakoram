// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package redact removes credential material from native process diagnostics.
// Both the control plane and agent use this boundary before retaining native
// output in operation state, events, log rings, or durable command receipts.
// Access-key IDs are public identifiers; only their associated secrets are hidden.
package redact

import (
	"bytes"
	"encoding/json"
	"regexp"
	"strconv"
	"strings"
)

// The final two indicators are URI schemes, not configuration field names.
var nativeIndicators = [...]string{
	"credentials", "scram-sha-1", "scram-sha-256", "storedkey", "serverkey",
	"salt", "password", "pwd", "secret", "secretkey", "secret-key",
	"secretaccesskey", "secret-access-key", "secret_access_key", "token",
	"mongodb", "mongodb+srv",
}

var credentialField = regexp.MustCompile(`(?i)\b(?:` + strings.Join(nativeIndicators[:len(nativeIndicators)-2], "|") + `)\b(?:\\*["'])?\s*[:=][ \t]*`)
var mongoUserinfo = regexp.MustCompile(`(?i)(mongodb(?:\+srv)?://)[^\s/@]+@`)
var encodedUnicode = regexp.MustCompile(`\\u[0-9a-fA-F]{4}`)
var recordStart = regexp.MustCompile(`^\s*(?:\{"t"\s*:\s*\{|\{"time"\s*:\s*"\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}|(?:time=)?(?:\[|")?\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}:\d{2}|\[[A-Za-z0-9_.-]+ (?:stdout|stderr)\] )`)

const streamOmissionLines = 64
const streamOmissionBytes = 64 << 10

// NativeError removes credential documents, verifiers, config secrets and MongoDB
// URI userinfo. Diagnostic text need not retain its original escaping convention.
// Incomplete sensitive values fail closed by dropping the unparseable suffix.
func NativeError(message string) string {
	return nativeError(message, nil)
}

func nativeError(message string, stream *Stream) string {
	trimmed := strings.TrimSpace(message)
	if len(trimmed) > 0 && (trimmed[0] == '{' || trimmed[0] == '[' || trimmed[0] == '"') && json.Valid([]byte(trimmed)) {
		// Decode complete envelopes before handling their diagnostic strings. This
		// also handles Unicode escapes and escaped quotes inside secret values.
		return string(JSON(json.RawMessage(message)))
	}
	message = mongoUserinfo.ReplaceAllString(message, "${1}[redacted]@")
	var out strings.Builder
	for {
		at := credentialField.FindStringIndex(message)
		before := len(message)
		if at != nil {
			before = at[0]
		}
		// Diagnostics can prefix a complete JSON envelope with native context.
		// Decode that envelope as a unit rather than matching escaped field names
		// inside its already-sanitized string values on subsequent passes.
		embedded := false
		for i := range message[:before] {
			if message[i] != '{' && message[i] != '[' {
				continue
			}
			d := json.NewDecoder(strings.NewReader(message[i:]))
			var raw json.RawMessage
			if d.Decode(&raw) != nil {
				continue
			}
			out.WriteString(message[:i])
			out.Write(JSON(raw))
			message = message[i+int(d.InputOffset()):]
			embedded = true
			break
		}
		if embedded {
			continue
		}
		if encoded := encodedUnicode.FindStringIndex(message[:before]); encoded != nil {
			// Encoded fields/delimiters outside a complete JSON envelope cannot
			// be classified safely. Omit their suffix instead of interpreting
			// another escaping dialect or retaining an incomplete document.
			out.WriteString(message[:encoded[0]])
			out.WriteString(`"[redacted]"`)
			if stream != nil {
				stream.pending = &valueState{started: true, omit: true}
			}
			return out.String()
		}
		if at == nil {
			if out.Len() == 0 {
				return message
			}
			out.WriteString(message)
			return out.String()
		}
		out.WriteString(message[:at[1]])
		value := message[at[1]:]
		if strings.Contains(strings.ToUpper(message[at[0]:at[1]]), "SCRAM-SHA-") && len(value) > 0 && value[0] != '{' && value[0] != '\\' {
			// A SASL mechanism followed by AuthenticationFailed is an error class.
			message = value
			continue
		}
		state := valueState{}
		end := state.consume(value)
		out.WriteString(`"[redacted]"`)
		if end < 0 {
			if stream != nil {
				if value == "" || value[0] == '|' || value[0] == '>' || value[0] == '\n' || value[0] == '\r' {
					stream.yaml = true
					stream.indent = indentation(message)
				} else {
					stream.pending = &state
				}
			}
			return out.String()
		}
		message = value[end:]
	}
}

// JSON sanitizes structured process output without destroying describe/status
// schemas or numeric precision. Secret-free success output is returned unchanged.
// Invalid/truncated output is diagnostic text and is returned as a JSON string.
func JSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return raw
	}
	var value any
	d := json.NewDecoder(bytes.NewReader(raw))
	d.UseNumber()
	if d.Decode(&value) != nil || !json.Valid(raw) {
		safe, _ := json.Marshal(NativeError(string(raw)))
		return safe
	}
	changed := false
	var visit func(any) any
	visit = func(v any) any {
		switch v := v.(type) {
		case map[string]any:
			for k, item := range v {
				if credentialField.MatchString(k + ":") {
					v[k] = "[redacted]"
					changed = true
				} else {
					v[k] = visit(item)
				}
			}
		case []any:
			for i := range v {
				v[i] = visit(v[i])
			}
		case string:
			safe := NativeError(v)
			if safe != v {
				changed = true
			}
			return safe
		}
		return v
	}
	value = visit(value)
	if !changed {
		return raw
	}
	safe, _ := json.Marshal(value)
	return safe
}

// Stream redacts line-oriented process output while retaining only bounded
// lexical state, never secret bytes, across lines. A Stream belongs to one pipe.
type Stream struct {
	pending      *valueState
	yaml         bool
	indent       int
	indicator    indicatorState
	recordBytes  int
	omittedLines int
	omittedBytes int
}

// Observe scans every byte, including bytes omitted from retained log lines.
// It retains only candidate-name bits and Unicode escape state, not input text.
func (s *Stream) Observe(part []byte, discarded bool) {
	s.recordBytes = min(streamOmissionBytes, s.recordBytes+len(part))
	for _, c := range part {
		s.indicator.byte(c, discarded)
	}
}

// EndLine completes observation of a physical line. Missing bytes are uncertain
// only if they contained a sensitive opener or a retained value was unfinished.
// A plain oversized line does not suppress subsequent public records.
func (s *Stream) EndLine(truncated bool) {
	if truncated && (s.indicator.risky || s.pending != nil || s.yaml) {
		s.yaml = false
		s.pending = &valueState{started: true, omit: true}
	}
	s.indicator = indicatorState{}
	s.recordBytes = 0
}

// Line returns a sanitized record, an optional omission-summary record preceding
// it, and whether to retain the record. Continuations retain no secret bytes.
// Omission ends at an unmistakable new record, 64 lines, or 64 KiB, whichever
// comes first. Call Omission at EOF to expose any remaining gap.
func (s *Stream) Line(message string) (safe, gap string, keep bool) {
	if (s.pending != nil || s.yaml) && (recordStart.MatchString(message) || s.omittedLines >= streamOmissionLines || s.omittedBytes >= streamOmissionBytes) {
		s.pending, s.yaml = nil, false
		return nativeError(message, s), s.Omission(), true
	}
	if s.yaml {
		if strings.TrimSpace(message) == "" || indentation(message) > s.indent {
			s.omitLine(message)
			return "", "", false
		}
		s.yaml = false
	}
	if s.pending != nil {
		// A physical newline, not the next line's first byte, consumes an
		// outstanding escape in a multiline quoted native value.
		s.pending.escaped = false
		end := s.pending.consume(message)
		if end < 0 {
			s.omitLine(message)
			return "", "", false
		}
		s.pending = nil
		return `"[redacted]"` + nativeError(message[end:], s), s.Omission(), true
	}
	return nativeError(message, s), s.Omission(), true
}

func (s *Stream) omitLine(message string) {
	s.omittedLines++
	s.omittedBytes += max(s.recordBytes, len(message))
}

// Omission returns one explicit summary for suppressed physical lines, resetting
// only the summary counters. It does not reinterpret or retain their content.
func (s *Stream) Omission() string {
	if s.omittedLines == 0 {
		return ""
	}
	marker := "[" + strconv.Itoa(s.omittedLines) + " lines omitted: possible credential material]"
	s.omittedLines, s.omittedBytes = 0, 0
	return marker
}

func indentation(message string) int {
	return len(message) - len(strings.TrimLeft(message, " \t"))
}

type valueState struct {
	started  bool
	quote    byte
	escaped  bool
	depth    int
	compound bool
	quoted   bool
	omit     bool
}

func (s *valueState) consume(value string) int {
	if !s.started {
		s.started = true
		if value == "" || value[0] == '|' || value[0] == '>' || value[0] == '\\' || value[0] == '\n' || value[0] == '\r' {
			// An escaped value outside a decodable JSON envelope is ambiguous:
			// do not reinterpret its escapes as native quoted-value boundaries.
			s.omit = true
		} else {
			s.compound = value[0] == '{' || value[0] == '[' || value[0] == '('
			s.quoted = value[0] == '"' || value[0] == '\''
		}
	}
	if s.omit {
		return -1
	}
	for i := range len(value) {
		c := value[i]
		if s.escaped {
			s.escaped = false
			continue
		}
		if s.quote != 0 {
			if c == '\\' {
				if s.quote == '\'' {
					// YAML treats this literally; other native dialects escape
					// the following quote. Ambiguity must not expose a suffix.
					s.omit = true
					return -1
				}
				s.escaped = true
			} else if c == s.quote {
				if s.quote == '\'' && i+1 < len(value) && value[i+1] == '\'' {
					// YAML single-quoted scalars escape a quote by doubling it.
					s.escaped = true
					continue
				}
				s.quote = 0
				if s.quoted && !s.compound {
					return i + 1
				}
			}
			continue
		}
		if c == '"' || c == '\'' {
			s.quote = c
			continue
		}
		if s.compound {
			switch c {
			case '{', '[', '(':
				s.depth++
			case '}', ']', ')':
				s.depth--
				if s.depth == 0 {
					return i + 1
				}
			}
		} else if c == '{' || c == '[' || c == '(' {
			// BinData(...) and Binary{...} are each one sensitive value.
			s.compound, s.depth = true, 1
		} else if c == ',' || c == '}' || c == ']' || c == '\n' || c == '|' || c == '\r' {
			return i
		}
	}
	if !s.compound && !s.quoted {
		return len(value)
	}
	return -1
}

// indicatorState recognizes sensitive field/scheme openers without retaining
// values or names from process output. Candidate bits survive reader chunks and
// the retention boundary, including JSON-encoded ASCII field-name characters.
type indicatorState struct {
	candidates uint32
	position   int
	word       bool
	pending    bool
	unicode    int
	code       uint16
	risky      bool
}

func (s *indicatorState) byte(c byte, discarded bool) {
	if s.unicode == 1 {
		s.unicode = 0
		if c == 'u' {
			s.unicode = 2
			s.code = 0
			return
		}
		s.literal('\\', discarded)
	} else if s.unicode >= 2 {
		var digit byte
		switch {
		case c >= '0' && c <= '9':
			digit = c - '0'
		case c >= 'a' && c <= 'f':
			digit = c - 'a' + 10
		case c >= 'A' && c <= 'F':
			digit = c - 'A' + 10
		default:
			s.unicode = 0
			s.literal(0, discarded) // An invalid escape terminates a candidate.
			s.byte(c, discarded)
			return
		}
		s.code = s.code<<4 | uint16(digit)
		s.unicode++
		if s.unicode == 6 {
			s.unicode = 0
			if s.code < 128 {
				s.literal(byte(s.code), discarded)
			} else {
				s.literal(0, discarded)
			}
		}
		return
	}
	if c == '\\' {
		s.unicode = 1
		return
	}
	s.literal(c, discarded)
}

func (s *indicatorState) literal(c byte, discarded bool) {
	if c >= 'A' && c <= 'Z' {
		c += 'a' - 'A'
	}
	if c >= 'a' && c <= 'z' || c >= '0' && c <= '9' || c == '-' || c == '_' || c == '+' {
		if !s.word {
			s.word, s.position, s.pending = true, 0, false
			s.candidates = 1<<len(nativeIndicators) - 1
		}
		if s.candidates != 0 {
			for i, name := range nativeIndicators {
				bit := uint32(1 << i)
				if s.candidates&bit != 0 && (s.position >= len(name) || name[s.position] != c) {
					s.candidates &^= bit
				}
			}
			s.position++
		}
		return
	}
	if s.word {
		s.pending = false
		for i, name := range nativeIndicators {
			if s.candidates&(1<<i) != 0 && s.position == len(name) {
				s.pending = true
				break
			}
		}
		s.word = false
	}
	if !s.pending {
		return
	}
	switch c {
	case ':', '=':
		s.risky = s.risky || discarded
		s.pending = false
	case ' ', '\t', '\r', '\n', '"', '\'', '\\':
		// Native/JSON/YAML key delimiters and arbitrary whitespace.
	default:
		s.pending = false
	}
}
