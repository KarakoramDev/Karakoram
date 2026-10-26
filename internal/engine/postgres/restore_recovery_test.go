// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRestoreRecoveryOwnershipEveryCheckpoint(t *testing.T) {
	steps := []string{"hold-access", "stop", "credentials", "restore-configure", "dcs-reset", "wipe", "restore-single", "publish-seed", "start-seed", "primary", "clone-primary", "replicas", "ready", "restore-publish", "restore-archive", "restore-cleanup", "stanza", "access", "publish-access", "fresh-base", "clone-cutover", "clone-publish", "clone-archive"}
	for _, step := range steps {
		t.Run(step, func(t *testing.T) {
			p, rc, _ := setup(t, "single")
			cp := backupCheckpoint{Step: step, Member: "demop0", Generation: 7, Index: 1, Source: &restoreSource{Namespace: "default", Cluster: "demo", Backup: "validated-set"}}
			raw, _ := json.Marshal(cp)
			op := &domain.Operation{ID: "same-restore", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recovery"}`), Checkpoint: raw}
			if !p.SupportsFailureCleanup(op) {
				t.Fatal("restore loses ownership at retry exhaustion")
			}
			result, err := p.CancelOperation(t.Context(), rc, op)
			if err != nil || !result.Done {
				t.Fatal("recovery handoff failed", err)
			}
			s, _ := load(rc.Cluster)
			if s.RestoreRecovery == nil || s.RestoreRecovery.ID != op.ID || string(s.RestoreRecovery.Checkpoint) != string(raw) {
				t.Fatal("restore checkpoint/source/generation not preserved")
			}
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
				t.Fatal("new operation may race restore recovery", err)
			}
			// A reconstructed provider must retain ownership without contacting agents.
			p = New(Options{})
			if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
				t.Fatal("handoff is not idempotent", err)
			}
		})
	}
}

func TestRestoreRecoveryStatusIdentifiesFrozenSource(t *testing.T) {
	p, rc, _ := setup(t, "single")
	s := converge(t, p, rc)
	cp := backupCheckpoint{Step: "wipe", Generation: 7, Source: &restoreSource{Namespace: "default", Cluster: "origin", Backup: "validated-set"}, Label: "20261017-021859F"}
	raw, _ := json.Marshal(cp)
	s.RestoreRecovery = &domain.Operation{ID: "restore-owner", Kind: domain.OperationRestore, Checkpoint: raw}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, value := range []string{"wipe", "7", "default/origin", "validated-set", "20261017-021859F"} {
		if !strings.Contains(status.Message, value) {
			t.Fatalf("restore status omitted frozen recovery identity %q", value)
		}
	}
}
