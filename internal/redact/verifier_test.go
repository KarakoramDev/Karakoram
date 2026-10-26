// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package redact

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func TestDiagnosticPreservesGeneratedResourceNames(t *testing.T) {
	// psmdb initialRestoreID: cluster ref + 12-character RandomString UID + suffix.
	for _, identity := range []string{
		"prod-Ab3dE6gH9jK2-initial-restore",
		"tenant:production-mongodb-target-Ab3dE6gH9jK2-initial-restore",
		"averylongproductionmongodbclustername2026-Ab3dE6gH9jK2-initial-restore",
		"production-mongodb-daily-logical-backup-20261021",
		"averylongproductionmongodbbackupname20261021",
		"production_mongodb_daily_backup_20261021",
		"production.mongodb.daily.backup.20261021",
		"ui9Z3FEPPOSGoLNi7MP2UY9q",
		"sha256:abcdef0123456789abcdef0123456789abcdef0123456789abcdef0123456789",
	} {
		text := "Waiting for initial restore operation " + identity
		if Diagnostic(text) != text {
			t.Fatal("product identity changed")
		}
		data, _ := json.Marshal(map[string]string{"message": text})
		if !bytes.Equal(data, DiagnosticsJSON(data)) {
			t.Fatal("persisted identity-bearing diagnostic changed")
		}
	}
}

func TestDiagnosticSCRAMFieldAndEncodingSizes(t *testing.T) {
	// MongoDB Presecrets<HashBlock>::saltLength() is kHashLength-4:
	// SHA-1 uses 16 bytes (24 base64 chars), SHA-256 uses 28 (40 chars).
	// PostgreSQL's local SCRAMVerifier uses 16 salt bytes even for SHA-256.
	// Stored/server keys use the full hash length: 20/32 bytes (28/44 chars).
	for _, size := range []int{16, 20, 28, 32} {
		material := make([]byte, size)
		for i := range material {
			material[i] = byte(i + 17)
		}
		encoded := base64.StdEncoding.EncodeToString(material)
		for _, label := range []string{"salt", "storedKey", "serverKey"} {
			text := label + ": " + encoded
			if strings.Contains(Diagnostic(text), encoded) {
				t.Fatal("labeled SCRAM material escaped")
			}
		}
		if len(encoded) >= 28 {
			if strings.Contains(Diagnostic("native value "+encoded), encoded) {
				t.Fatal("unlabeled long SCRAM encoding escaped")
			}
		} else if Diagnostic("native value "+encoded) != "native value "+encoded {
			t.Fatal("short unlabeled token should be below heuristic threshold")
		}
	}
}

func TestPublicEventDetailsDoNotExcludeExecutionNamedSubtrees(t *testing.T) {
	details := json.RawMessage(`{"payload":{"salt":"fake-payload"},"checkpoint":{"storedKey":"fake-checkpoint"},"internal":{"serverKey":"fake-internal"}}`)
	if bytes.Contains(EventDetailsJSON(details), []byte("fake-")) {
		t.Fatal("public event subtree escaped sanitization")
	}
	if !bytes.Equal(DiagnosticsJSON(details), details) {
		t.Fatal("resource execution state changed")
	}
}
