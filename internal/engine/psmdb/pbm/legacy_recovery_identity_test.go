// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

type credentialCheckingHooks struct {
	fakeHooks
	rc *engine.Context
}

func (h *credentialCheckingHooks) ResetSystemUsers(ctx context.Context, identity string) error {
	pw, err := h.rc.Secrets.GetClusterSecret(ctx, domain.ParseClusterKey(identity), "users/userAdmin")
	if err != nil {
		return err
	}
	if string(pw) != "source-snapshot-password" {
		return fmt.Errorf("physical clone did not recover the snapshot's source credential")
	}
	return h.fakeHooks.ResetSystemUsers(ctx, identity)
}

func TestLegacyPhysicalCloneProtectsSourceRecoveryIdentity(t *testing.T) {
	for _, mode := range []string{"snapshot", "pitr"} {
		t.Run(mode, func(t *testing.T) {
			env, st, fake, _ := setup(t)
			ctx := context.Background()
			source := &domain.Cluster{Name: "source", Spec: domain.ClusterSpec{Backup: domain.BackupConfig{Storage: "storage"}}}
			if err := st.CreateCluster(ctx, source); err != nil {
				t.Fatal(err)
			}
			if err := st.SetClusterSecret(ctx, domain.ClusterRef{Name: source.Name}, "users/userAdmin", []byte("source-snapshot-password")); err != nil {
				t.Fatal(err)
			}
			last := time.Unix(1704453059, 0).UTC()
			b := &domain.Backup{Name: "legacy", Cluster: source.Name, Storage: "storage", Type: "physical", PBMName: "2024-01-05T11:10:54Z", Phase: domain.PhaseSucceeded, PBMPrefix: "legacy/source-prefix", LastWriteTime: &last}
			if err := st.CreateBackup(ctx, b); err != nil {
				t.Fatal(err)
			}
			r := &domain.Restore{Name: "clone", Cluster: "target", Source: domain.RestoreSource{SourceCluster: source.Name, Backup: b.Name}}
			if mode == "pitr" {
				target := time.Unix(1704453100, 0).UTC()
				r.Source.Backup, r.Source.PITRTime = "", &target
			}
			if err := st.CreateRestore(ctx, r); err != nil {
				t.Fatal(err)
			}
			hooks := &credentialCheckingHooks{rc: env.RC}
			env.Hooks = hooks
			op := &domain.Operation{ID: "legacy-clone", Payload: json.RawMessage(`{"restore":"clone"}`)}
			first, err := RunRestore(ctx, env, op)
			if err != nil {
				t.Fatal(err)
			}
			persisted, err := st.GetBackup(ctx, domain.ClusterRef{Name: source.Name}, b.Name)
			if err != nil || persisted.RecoveryIdentity == "" || persisted.RecoveryIdentity == source.Name {
				t.Fatalf("protected provenance=%+v error=%v", persisted, err)
			}
			if len(fake.requests) != 0 || len(fake.actions) != 0 || fake.starts != 0 {
				t.Fatal("restore performed side effects before durable identity resolution")
			}
			// Recovery no longer depends on the source cluster surviving the clone.
			if err := st.DeleteCluster(ctx, domain.ClusterRef{Name: source.Name}); err != nil {
				t.Fatal(err)
			}
			op.Checkpoint = first.Checkpoint
			drive(t, env, op, RunRestore, false)
			got, err := st.GetRestore(ctx, domain.ClusterRef{Name: "target"}, r.Name)
			if err != nil || got.Phase != domain.PhaseSucceeded || fake.starts != 1 {
				t.Fatalf("clone=%+v starts=%d error=%v", got, fake.starts, err)
			}
			if len(hooks.reset) == 0 || hooks.reset[0] != persisted.RecoveryIdentity {
				t.Fatalf("clone reset did not receive protected identity: %v", hooks.reset)
			}
			if strings.Contains(string(op.Checkpoint), "source-snapshot-password") {
				t.Fatal("source recovery password leaked into checkpoint")
			}
		})
	}
}

func TestLegacyPhysicalRestoreRejectsUnrecoverableIdentityBeforeSideEffects(t *testing.T) {
	for _, mode := range []string{"missing-source", "missing-source-secret", "empty-source-secret", "missing-protected-secret"} {
		t.Run(mode, func(t *testing.T) {
			env, st, fake, hooks := setup(t)
			ctx := context.Background()
			if mode != "missing-source" {
				if err := st.CreateCluster(ctx, &domain.Cluster{Name: "source"}); err != nil {
					t.Fatal(err)
				}
			}
			if mode == "empty-source-secret" {
				if err := st.SetClusterSecret(ctx, domain.ClusterRef{Name: "source"}, "users/userAdmin", nil); err != nil {
					t.Fatal(err)
				}
			}
			b := &domain.Backup{Name: "unrecoverable", Cluster: "source", Storage: "storage", Type: "physical", PBMName: "2024-01-05T11:10:54Z", Phase: domain.PhaseSucceeded, PBMPrefix: "legacy/source-prefix"}
			if mode == "missing-protected-secret" {
				b.RecoveryIdentity = "lost-snapshot-identity"
			}
			if err := st.CreateBackup(ctx, b); err != nil {
				t.Fatal(err)
			}
			if err := st.CreateRestore(ctx, &domain.Restore{Name: "clone", Cluster: "target", Phase: domain.PhasePending, Source: domain.RestoreSource{SourceCluster: "source", Backup: b.Name}}); err != nil {
				t.Fatal(err)
			}
			_, err := RunRestore(ctx, env, &domain.Operation{ID: "rejected-clone", Payload: json.RawMessage(`{"restore":"clone"}`)})
			if err == nil || !strings.Contains(err.Error(), "unrecoverable") || (!strings.Contains(err.Error(), "recovery identity") && !strings.Contains(err.Error(), "recoverable userAdmin credential")) {
				t.Fatalf("missing clear recovery preflight error: %v", err)
			}
			if fake.starts != 0 || len(fake.requests) != 0 || len(fake.actions) != 0 || len(hooks.reset) != 0 || len(hooks.balancers) != 0 {
				t.Fatal("unrecoverable clone modified the target")
			}
			got, err := st.GetRestore(ctx, domain.ClusterRef{Name: "target"}, "clone")
			if err != nil || got.Phase != domain.PhasePending || got.StartedAt != nil {
				t.Fatalf("unrecoverable clone was started: %+v error=%v", got, err)
			}
		})
	}
}
