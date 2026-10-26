// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package redact

import (
	"bytes"
	"encoding/json"
	"testing"
)

func TestDiagnosticFastPathAndIdempotence(t *testing.T) {
	ordinary := "Waiting for database member readiness"
	if allocs := testing.AllocsPerRun(100, func() {
		if Diagnostic(ordinary) != ordinary {
			panic("changed ordinary diagnostic")
		}
	}); allocs != 0 {
		t.Fatal("ordinary diagnostic allocated", allocs)
	}
	for _, text := range []string{`AuthenticationFailed salt: "synthetic-salt" storedKey: "synthetic-stored" serverKey: "synthetic-server"`, `{"message":"SALT: synthetic-salt"}`, `{"message":"\u0073alt: synthetic-salt"}`} {
		safe := Diagnostic(text)
		for _, value := range []string{"synthetic-salt", "synthetic-stored", "synthetic-server"} {
			if bytes.Contains([]byte(safe), []byte(value)) {
				t.Fatal("verifier escaped")
			}
		}
		if Diagnostic(safe) != safe {
			t.Fatal("diagnostic sanitizer not idempotent")
		}
	}
	raw := json.RawMessage(`{"message":"salt: synthetic-salt","name":"salt: identity","number":9007199254740993,"step":42}`)
	safe := DiagnosticsJSON(raw)
	if bytes.Contains(safe, []byte("synthetic-salt")) || !bytes.Contains(safe, []byte("salt: identity")) || !bytes.Contains(safe, []byte("9007199254740993")) || !bytes.Contains(safe, []byte(`"step":42`)) {
		t.Fatal("diagnostic field boundary violated")
	}
	if !bytes.Equal(safe, DiagnosticsJSON(safe)) {
		t.Fatal("JSON sanitizer not idempotent")
	}
	ordinaryJSON := json.RawMessage(`{ "message": "ordinary progress", "number": 7 }`)
	if !bytes.Equal(ordinaryJSON, DiagnosticsJSON(ordinaryJSON)) {
		t.Fatal("ordinary JSON changed")
	}
}

func TestOperationBlockerDiagnosticSanitization(t *testing.T) {
	raw := json.RawMessage(`{"blockingMessage":"password: synthetic-password","blockingObservedAt":"2026-10-22T01:23:17Z"}`)
	safe := DiagnosticsJSON(raw)
	if bytes.Contains(safe, []byte("synthetic-password")) || !bytes.Contains(safe, []byte("2026-10-22T01:23:17Z")) {
		t.Fatal("blocker diagnostic escaped sanitization or timestamp changed")
	}
}
