// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package redact

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeError(t *testing.T) {
	cases := []string{
		`credentials:{SCRAM-SHA-1:{salt:"fixture-secret",storedKey:BinData(0,"fixture-secret"),serverKey:"fixture-secret"}}`,
		`SCRAM-SHA-256:{salt:"fixture-secret",storedKey:"fixture-secret"}`,
		`salt:BinData(0,"fixture-secret")`,
		`storedKey:Binary{Data:"fixture-secret"}`,
		`password=fixture-secret`, `pwd: "fixture-secret"`,
		`mongodb://fixture-user:fixture-secret@localhost/admin`,
		`mongodb+srv://fixture-user:fixture-secret@cluster/admin`,
		"secret-access-key: fixture-secret\naccess-key-id: public-identity",
		`{"password":"fixture-secret","secret":"fixture-secret","secretAccessKey":"fixture-secret","secret-access-key":"fixture-secret","token":"fixture-secret"}`,
		`credentials:{salt:"fixture-secret"`,
		`password:"fixture-secret`,
		"password: |\n  fixture-secret\n  another private line",
		"secret: >-\n  fixture-secret",
		"password: fixture-secret with spaces",
		`password:"prefix\"fixture-secret"`,
		`password:"prefix\"fixture-secret`,
		"password:\n  fixture-secret",
		`password:'prefix''fixture-secret'`,
		`password:'prefix''fixture-secret`,
		`password:'prefix\''fixture-secret'`,
		`{"pass\u0077ord":"fixture-secret"}`,
		`{"pass\u0077ord":"fixture-secret"`,
		`{\u0022password\u0022:\u0022fixture-secret\u0022}`,
		`\u0022password\u0022:\u0022fixture-secret\u0022`,
	}
	for _, original := range cases {
		for layer := range 6 {
			input := "AuthenticationFailed apply oplog: " + original
			for range layer {
				b, _ := json.Marshal(input)
				input = string(b)
			}
			got := NativeError(input)
			if strings.Contains(got, "fixture-secret") || strings.Contains(got, "fixture-user") {
				t.Fatalf("credential escaped at layer %d, case %d", layer, len(original))
			}
			if !strings.Contains(got, "AuthenticationFailed") {
				t.Fatal("lost authentication failure marker")
			}
		}
	}
	const mechanism = `unable to authenticate using mechanism "SCRAM-SHA-256": (AuthenticationFailed) Authentication failed`
	if NativeError(mechanism) != mechanism {
		t.Fatal("redacted authentication classification")
	}
}

func TestPrefixedEscapedPasswordRetainsAuthenticationFailure(t *testing.T) {
	input := `apply oplog: password:"prefix\"fixture-secret" | (AuthenticationFailed)`
	for range 6 {
		got := NativeError(input)
		if strings.Contains(got, "fixture-secret") || !strings.Contains(got, "AuthenticationFailed") {
			t.Fatal("escaped password escaped redaction or lost error class")
		}
		got = NativeError("restore failed: " + got)
		if strings.Contains(got, "fixture-secret") || !strings.Contains(got, "AuthenticationFailed") {
			t.Fatal("re-sanitizing a prefixed envelope lost safe diagnostic context")
		}
		data, _ := json.Marshal(map[string]string{"error": input})
		input = string(data)
	}
}

func TestYAMLDoubledQuotePreservesAuthenticationFailure(t *testing.T) {
	input := `apply oplog: password:'prefix''fixture-secret' | AuthenticationFailed`
	if got := NativeError(input); strings.Contains(got, "fixture-secret") || !strings.Contains(got, "AuthenticationFailed") {
		t.Fatal("YAML doubled quote broke sensitive value boundaries")
	}
}

func TestJSONPreservesSchemaAndSuccessBytes(t *testing.T) {
	success := json.RawMessage(`{ "status":"done", "name":"restore-123", "ts":9223372036854775807, "replsets":[{"name":"rs0"}] }`)
	if string(JSON(success)) != string(success) {
		t.Fatal("modified secret-free success output")
	}
	raw := json.RawMessage(`{"status":"error","error":"AuthenticationFailed password: fixture-secret","config":{"token":"fixture-secret","access-key-id":"public-identity"},"ts":9223372036854775807}`)
	safe := JSON(raw)
	if !json.Valid(safe) || strings.Contains(string(safe), "fixture-secret") || !strings.Contains(string(safe), "public-identity") || !strings.Contains(string(safe), "9223372036854775807") {
		t.Fatal("redaction lost JSON schema, public identity, or precision")
	}
	var result struct{ Status, Error string }
	if json.Unmarshal(safe, &result) != nil || result.Status != "error" || !strings.Contains(result.Error, "AuthenticationFailed") {
		t.Fatal("lost structured error")
	}
	for range 6 {
		nested, _ := json.Marshal(map[string]any{"error": string(raw)})
		raw = nested
		if got := JSON(raw); !json.Valid(got) || strings.Contains(string(got), "fixture-secret") {
			t.Fatal("nested JSON escaped redaction")
		}
	}
	if got := JSON(json.RawMessage(`{"error":"AuthenticationFailed password: fixture-secret`)); !json.Valid(got) || strings.Contains(string(got), "fixture-secret") {
		t.Fatal("truncated JSON escaped redaction")
	}
}
