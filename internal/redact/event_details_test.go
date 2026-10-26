// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package redact

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"
)

func TestEventDetailsCredentialKeysAndAuditValues(t *testing.T) {
	raw := json.RawMessage(`{"credentials":{"SCRAM-SHA-256":{"iterationCount":4096,"salt":"fake-salt","storedKey":"fake-stored","serverKey":"fake-server"}},"changes":[{"path":"users.credentials","before":{"salt":"fake-before"},"after":{"storedKey":"fake-after"}},{"path":"spec.password","before":"fake-plain-before","after":"fake-plain-after"}],"password":"fake-password","secret_key":"fake-secret","key":"fake-key","nested":{"clientKey":"fake-client"},"operationId":"ui9Z3FEPPOSGoLNi7MP2UY9q","digest":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789","unknown":9007199254740993}`)
	safe := EventDetailsJSON(raw)
	if bytes.Contains(safe, []byte("fake-")) {
		t.Fatal("event detail credential escaped")
	}
	for _, kept := range []string{"ui9Z3FEPPOSGoLNi7MP2UY9q", "abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789", "9007199254740993"} {
		if !bytes.Contains(safe, []byte(kept)) {
			t.Fatal("event detail identity or precision changed")
		}
	}
	if !bytes.Equal(safe, EventDetailsJSON(safe)) {
		t.Fatal("event detail sanitizer not idempotent")
	}
	// Resource execution state is not routed through the event-detail sanitizer.
	record := json.RawMessage(`{"id":"ui9Z3FEPPOSGoLNi7MP2UY9q","payload":{"password":"fake-payload"},"checkpoint":{"storedKey":"fake-checkpoint"},"internal":{"salt":"fake-internal"},"name":"fake-identity","sha256":"abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"}`)
	if !bytes.Equal(record, DiagnosticsJSON(record)) {
		t.Fatal("resource execution state changed")
	}
}

func TestDiagnosticUnlabeledVerifiersPreservesIdentitiesAndDigests(t *testing.T) {
	for _, verifier := range []string{"QWxwaGFCZXRhR2FtbWFEZWx0YUVwc2lsb25PbmVUd28=", "QWxwaGFCZXRhR2FtbWFEZWx0YU9uZQ==", "QWxwaGFCZXRhR2FtbWFEZWx0YUVwc2lsb25PbmVUd28"} {
		raw := "native failure value " + verifier + " end"
		safe := Diagnostic(raw)
		if strings.Contains(safe, verifier) || !strings.Contains(safe, "native failure value") {
			t.Fatal("unlabeled verifier escaped or context lost")
		}
		if safe != Diagnostic(safe) {
			t.Fatal("unlabeled verifier sanitization not idempotent")
		}
		document, _ := json.Marshal(map[string]string{"message": raw, "id": verifier})
		var decoded map[string]string
		if err := json.Unmarshal(DiagnosticsJSON(document), &decoded); err != nil {
			t.Fatal(err)
		}
		if decoded["id"] != verifier || strings.Contains(decoded["message"], verifier) {
			t.Fatal("resource diagnostic boundary violated")
		}
	}
	for _, kept := range []string{"ui9Z3FEPPOSGoLNi7MP2UY9q", "YbJYablCtsnrZBSnlPivXW8o", "d41d8cd98f00b204e9800998ecf8427e", "da39a3ee5e6b4b0d3255bfef95601890afd80709", "sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789"} {
		text := "operation/checksum " + kept
		if Diagnostic(text) != text {
			t.Fatal("product identity/digest changed")
		}
	}
}
