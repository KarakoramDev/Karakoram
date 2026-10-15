// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package pbm

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type interruptedCleanupStore struct {
	engine.ClusterStore
	interrupted bool
}

func (s *interruptedCleanupStore) DeleteBackup(ctx context.Context, cluster domain.ClusterRef, name string) error {
	if !s.interrupted {
		s.interrupted = true
		return errors.New("control plane stopped before inventory deletion")
	}
	return s.ClusterStore.DeleteBackup(ctx, cluster, name)
}

type cleanupNotFoundSecrets struct {
	engine.SecretStore
	err error
}

func (s cleanupNotFoundSecrets) DeleteClusterSecret(context.Context, domain.ClusterRef, string) error {
	return s.err
}

func TestDeleteAllSecretDeletedCrashResume(t *testing.T) {
	env, st, _, _ := setup(t)
	ctx := context.Background()
	b := &domain.Backup{Name: "base", Cluster: "target", Storage: "storage", Type: "logical", PBMName: "2024-01-05T11:10:54Z", RecoveryIdentity: "base-recovery"}
	if err := st.CreateBackup(ctx, b); err != nil {
		t.Fatal(err)
	}
	if err := st.SetClusterSecret(ctx, domain.ClusterRef{Name: b.RecoveryIdentity}, "users/userAdmin", []byte("backup-password")); err != nil {
		t.Fatal(err)
	}
	env.RC.Store = &interruptedCleanupStore{ClusterStore: st}
	interrupted := false
	for range 30 {
		_, err := DeleteAll(ctx, env)
		if err == nil {
			continue
		}
		if _, err := st.GetClusterSecret(ctx, domain.ClusterRef{Name: b.RecoveryIdentity}, "users/userAdmin"); !errors.Is(err, store.ErrNotFound) {
			t.Fatalf("secret was not removed before crash: %v", err)
		}
		if _, err := st.GetBackup(ctx, domain.ClusterRef{Name: b.Cluster}, b.Name); err != nil {
			t.Fatalf("inventory record must survive crash: %v", err)
		}
		var state map[string]json.RawMessage
		if err := json.Unmarshal(env.RC.Cluster.Internal, &state); err != nil {
			t.Fatal(err)
		}
		var cp checkpoint
		if err := json.Unmarshal(state["pbmCleanup"], &cp); err != nil {
			t.Fatal(err)
		}
		if cp.Step != "observe-snapshot" {
			t.Fatalf("unexpected crash checkpoint: %+v", cp)
		}
		// Resume with fresh cluster allocation and the ordinary durable store.
		raw, err := json.Marshal(env.RC.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		var resumed domain.Cluster
		if err := json.Unmarshal(raw, &resumed); err != nil {
			t.Fatal(err)
		}
		resumed.Internal = append(json.RawMessage(nil), env.RC.Cluster.Internal...)
		env.RC.Cluster, env.RC.Store = &resumed, st
		interrupted = true
		break
	}
	if !interrupted {
		t.Fatal("did not reach secret/inventory crash boundary")
	}
	for range 10 {
		res, err := DeleteAll(ctx, env)
		if err != nil {
			t.Fatal(err)
		}
		if res.Done {
			if _, err := st.GetBackup(ctx, domain.ClusterRef{Name: b.Cluster}, b.Name); !errors.Is(err, store.ErrNotFound) {
				t.Fatalf("cleanup retained backup record: %v", err)
			}
			var state map[string]json.RawMessage
			if err := json.Unmarshal(env.RC.Cluster.Internal, &state); err != nil || string(state["uid"]) != `"target-id"` {
				t.Fatal("cleanup lost unrelated durable cluster state")
			}
			return
		}
	}
	t.Fatal("cleanup did not finish after restart with missing recovery secret")
}

func TestDeleteAllMissingRecoverySecretErrors(t *testing.T) {
	for _, test := range []struct {
		name string
		err  error
		done bool
	}{
		{"store-not-found", fmt.Errorf("delete secret: %w", store.ErrNotFound), true},
		{"engine-not-found", fmt.Errorf("delete secret: %w", engine.ErrNotFound), true},
		{"other-error", errors.New("secret store unavailable"), false},
	} {
		t.Run(test.name, func(t *testing.T) {
			env, st, _, _ := setup(t)
			ctx := context.Background()
			env.RC.Secrets = cleanupNotFoundSecrets{SecretStore: st, err: test.err}
			b := &domain.Backup{Name: "base", Cluster: "target", Storage: "storage", PBMName: "2024-01-05T11:10:54Z", RecoveryIdentity: "missing"}
			if err := st.CreateBackup(ctx, b); err != nil {
				t.Fatal(err)
			}
			for range 20 {
				res, err := DeleteAll(ctx, env)
				if !test.done && err != nil {
					if !errors.Is(err, test.err) {
						t.Fatalf("unexpected error: %v", err)
					}
					if _, err := st.GetBackup(ctx, domain.ClusterRef{Name: b.Cluster}, b.Name); err != nil {
						t.Fatal("secret-store failure removed inventory record")
					}
					return
				}
				if err != nil {
					t.Fatal(err)
				}
				if res.Done {
					if !test.done {
						t.Fatal("unexpected cleanup success")
					}
					if _, err := st.GetBackup(ctx, domain.ClusterRef{Name: b.Cluster}, b.Name); !errors.Is(err, store.ErrNotFound) {
						t.Fatalf("missing secret prevented inventory deletion: %v", err)
					}
					return
				}
			}
			t.Fatal("cleanup did not reach expected outcome")
		})
	}
}

func TestDeleteAllPreservesPriorIncarnationRecordWithSameSnapshotName(t *testing.T) {
	env, st, _, _ := setup(t)
	b := &domain.Backup{Name: "retained", Cluster: "target", Storage: "storage", PBMName: "2024-01-05T11:10:54Z", PBMPrefix: "karakoram/target-prior", RecoveryIdentity: "retained-recovery"}
	if err := st.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	identity := domain.ClusterRef{Name: b.RecoveryIdentity}
	if err := st.SetClusterSecret(t.Context(), identity, "users/userAdmin", []byte("recovery")); err != nil {
		t.Fatal(err)
	}
	for range 30 {
		res, err := DeleteAll(t.Context(), env)
		if err != nil {
			t.Fatal(err)
		}
		if res.Done {
			if _, err := st.GetBackup(t.Context(), env.RC.Cluster.Ref(), b.Name); err != nil {
				t.Fatal("PBM cleanup discarded an earlier incarnation's record", err)
			}
			if _, err := st.GetClusterSecret(t.Context(), identity, "users/userAdmin"); err != nil {
				t.Fatal("PBM cleanup discarded retained recovery credentials", err)
			}
			return
		}
	}
	t.Fatal("PBM cleanup did not finish")
}
