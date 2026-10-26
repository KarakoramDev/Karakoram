// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"os"
	"strings"
	"testing"
)

func TestUpgradeFailureDiagnosticPreservesCheckpoint(t *testing.T) {
	p := dataPaths(t.TempDir())
	request := upgradeRequest()
	name, err := receiptPath(p, "upgrade", request.Receipt)
	if err != nil {
		t.Fatal(err)
	}
	for _, phase := range []string{"planned", "checked", "linking", "linked", "old-retired"} {
		if err := durableJSON(name, upgradeReceipt{Request: request, Phase: phase}); err != nil {
			t.Fatal(err)
		}
		message := upgradeFailureDiagnostic(p, request)
		if !strings.Contains(message, "checkpoint "+phase) {
			t.Fatalf("checkpoint missing: %s", message)
		}
		if strings.Contains(message, p.root) || strings.Contains(message, request.Receipt) {
			t.Fatal("diagnostic exposed path or untrusted receipt")
		}
		if phase == "linking" && (!strings.Contains(message, "Do not clear the receipt or retry the link") || !strings.Contains(message, "validated pre-upgrade backup")) {
			t.Fatal("unsafe link retry advice")
		}
	}
}

func TestWALHealthSamplesRetentionAndArchiveBacklog(t *testing.T) {
	p, _ := statusSQLFixture(t, false)
	st := New().status(t.Context(), testSpec(), p)
	if !st.WALStatusKnown || st.WALBytes != 16<<20 || st.ArchivePending != 2 || !st.SlotStatusKnown || len(st.ReplicationSlots) != 1 || st.ReplicationSlots[0].RetainedBytes != 2<<30 {
		t.Fatalf("WAL/slot observations lost: %+v", st)
	}
}

func TestUpgradeFailureDiagnosticUnownedTarget(t *testing.T) {
	p := dataPaths(t.TempDir())
	request := upgradeRequest()
	if err := os.Mkdir(pathsForUpgrade(p, request).target, 0700); err != nil {
		t.Fatal(err)
	}
	message := upgradeFailureDiagnostic(p, request)
	if !strings.Contains(message, "checkpoint planned") || !strings.Contains(message, "without a durable ownership receipt") || !strings.Contains(message, "do not erase or adopt") {
		t.Fatalf("ambiguous target not actionable: %s", message)
	}
}
