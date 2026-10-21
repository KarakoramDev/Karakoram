// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mcp

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"strconv"
	"strings"
	"time"

	sdk "github.com/modelcontextprotocol/go-sdk/mcp"
)

// Binding describes server-verified identity and the exact action being approved.
// Arguments must be canonical JSON; Summary must be server-written, never clientInfo.
type Binding struct {
	Principal, Tool, Operation, Target, Confirmation, Summary string
	Arguments                                                 []byte
}

// Confirmer is a stateless, argument-bound human approval gate. Sign must use a
// dedicated master-key-derived HMAC key, not an idempotency key. Audit records
// accept and decline; it must return an error if the event cannot be persisted.
type Confirmer struct {
	Sign  func([]byte) string
	Now   func() time.Time
	Audit func(Binding, bool) error
}

// Gate returns (nil, true) only after a valid human acceptance. Otherwise the
// returned result must be sent to the client and the operation must not execute.
func (c *Confirmer) Gate(req *sdk.CallToolRequest, b Binding) (*sdk.CallToolResult, bool) {
	fail := func(message string) (*sdk.CallToolResult, bool) { return toolError(message), false }
	now := time.Now()
	if c.Now != nil {
		now = c.Now()
	}
	if req == nil || req.Params == nil || c.Sign == nil {
		return fail("Confirmation unavailable; use karactl or the web UI (" + b.Operation + ").")
	}
	p := req.Params
	if p.RequestState == "" && len(p.InputResponses) == 0 {
		caps := req.ClientCapabilities()
		if caps == nil || caps.Elicitation == nil || caps.Elicitation.Form == nil && caps.Elicitation.URL != nil {
			return fail("Human confirmation requires elicitation; use karactl or the web UI (" + b.Operation + ").")
		}
		expires := strconv.FormatInt(now.Add(5*time.Minute).Unix(), 10)
		state := expires + "." + base64.RawURLEncoding.EncodeToString([]byte(c.Sign(confirmationPayload(b, expires))))
		properties := map[string]any{}
		required := []string{}
		if b.Confirmation == "name" {
			properties["name"] = map[string]any{"type": "string", "description": "Type the target name: " + b.Target}
			required = append(required, "name")
		}
		return &sdk.CallToolResult{RequestState: state, InputRequests: sdk.InputRequestMap{"confirmation": &sdk.ElicitParams{Mode: "form", Message: b.Summary, RequestedSchema: map[string]any{"type": "object", "properties": properties, "required": required}}}}, false
	}
	accepted := false
	parts := strings.Split(p.RequestState, ".")
	valid := len(parts) == 2
	if valid {
		expiry, err := strconv.ParseInt(parts[0], 10, 64)
		valid = err == nil && now.Unix() < expiry && expiry <= now.Add(5*time.Minute).Unix()
		signature, err := base64.RawURLEncoding.DecodeString(parts[1])
		valid = valid && err == nil && hmac.Equal(signature, []byte(c.Sign(confirmationPayload(b, parts[0]))))
	}
	if valid {
		response, ok := p.InputResponses["confirmation"].(*sdk.ElicitResult)
		accepted = ok && response.Action == "accept"
		if accepted && b.Confirmation == "name" {
			accepted = b.Target != "" && response.Content["name"] == b.Target
		}
	}
	if c.Audit != nil {
		if err := c.Audit(b, accepted); err != nil {
			return fail("Could not record confirmation audit; nothing executed.")
		}
	}
	if !accepted {
		return fail("Confirmation declined, missing, invalid or expired; nothing executed.")
	}
	return nil, true
}

func confirmationPayload(b Binding, expiry string) []byte {
	hash := sha256.Sum256(b.Arguments)
	data, _ := json.Marshal([]string{b.Principal, b.Tool, base64.RawURLEncoding.EncodeToString(hash[:]), expiry})
	return data
}

func toolError(message string) *sdk.CallToolResult {
	return &sdk.CallToolResult{IsError: true, Content: []sdk.Content{&sdk.TextContent{Text: message}}}
}
