// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
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

func TestRestoreAccessFailureResumesAndReleasesClientHold(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	b := &domain.Backup{Namespace: "default", Cluster: "demo", Name: "source", Storage: "archive", Provider: "postgres", Phase: domain.PhaseSucceeded, SourceVersion: rc.Cluster.Spec.Version, Postgres: &domain.PostgresBackup{Label: "20261017-021859F", Repository: "prefix/postgres/default/demo-incarnation"}}
	if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	r := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "recovery", Source: domain.RestoreSource{Backup: b.Name}, Phase: domain.PhaseRunning}
	if err := rc.Store.CreateRestore(t.Context(), r); err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, "access/roles", []byte(`["app"]`)); err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, rolePasswordKey("app"), []byte("current-app-password")); err != nil {
		t.Fatal(err)
	}
	if err := saveBackupState(rc.Cluster, backupState{ManagedRolesHeld: true, RestoreOperation: "access-owner"}); err != nil {
		t.Fatal(err)
	}
	fail := true
	reloads := 0
	installBackupAgent(t, rc, func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.Path {
		case "/v1/postgres/status":
			json.NewEncoder(w).Encode(pgspec.StatusResponse{Running: true, Role: "standalone"})
		case "/v1/postgres/roles":
			var request pgspec.AccessRequest
			json.NewDecoder(req.Body).Decode(&request)
			if request.Operation == "list" {
				json.NewEncoder(w).Encode([]Role{{Name: "app", Login: true}})
				return
			}
			if fail || request.Binding || request.Operation != "password" {
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			json.NewEncoder(w).Encode(pgspec.ActionResponse{OK: true})
		case "/v1/reload":
			reloads++
			json.NewEncoder(w).Encode(struct{}{})
		default:
			t.Error("access recovery replayed a destructive action", req.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	})
	raw, _ := json.Marshal(backupCheckpoint{Step: "access", Generation: 7})
	op := &domain.Operation{ID: "access-owner", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recovery"}`), Checkpoint: raw}
	if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	if handled, err := p.reconcileRestoreRecovery(t.Context(), rc, s); !handled || err == nil || !strings.Contains(err.Error(), "delete the cluster") {
		t.Fatal("failed recovery omitted supported operator actions", err)
	}
	bs, _ := loadBackupState(rc.Cluster)
	if !bs.ManagedRolesHeld || s.RestoreRecovery == nil {
		t.Fatal("failed password recovery opened clients")
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	fail = false
	for range 4 {
		s, _ = load(rc.Cluster)
		if _, err := p.reconcileRestoreRecovery(t.Context(), rc, s); err != nil {
			t.Fatal(err)
		}
		if err := save(rc.Cluster, s); err != nil {
			t.Fatal(err)
		}
		if s.RestoreRecovery == nil {
			break
		}
	}
	bs, _ = loadBackupState(rc.Cluster)
	restored, err := rc.Store.GetRestore(t.Context(), rc.Cluster.Ref(), r.Name)
	if err != nil || s.RestoreRecovery != nil || bs.ManagedRolesHeld || reloads == 0 || restored.Phase != domain.PhaseSucceeded {
		t.Fatal("repaired access did not release clients and finish exact restore", err)
	}
}
