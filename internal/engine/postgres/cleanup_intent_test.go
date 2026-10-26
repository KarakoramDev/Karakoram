// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type cleanupIntentBackend struct {
	*fakeBackend
	t          *testing.T
	member, id string
	full       bool
	observed   bool
}

func (b *cleanupIntentBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID != "persisted-delete-receipt" {
		return b.fakeBackend.OpStatus(ctx, op)
	}
	intent, ok := infra.CleanupIntentFromContext(ctx)
	if !ok || intent.FullDelete != b.full || intent.Services[b.member] != b.id {
		b.t.Fatalf("cleanup intent lost on pending observation: %+v", intent)
	}
	b.observed = true
	return infra.OpState{Status: "pending", Message: "Swarm inconsistency: desired 0 but a task is desired-running; escalated removal"}, nil
}

func TestCleanupReconstructsAuthorizationForPersistedDeleteReceipt(t *testing.T) {
	for _, mode := range []string{"full-delete", "suspend", "not-deleting"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, backend := setup(t, "single")
			s := converge(t, p, rc)
			member := s.data()[0]
			s.Pending = &pending{Action: "delete-runtime", Member: member.Name, Op: infra.Op{ID: "persisted-delete-receipt", Kind: "delete", ServiceID: member.ServiceID}}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Deleting = mode != "not-deleting"
			rc.Cluster.Internal = append([]byte(nil), rc.Cluster.Internal...)
			b := &cleanupIntentBackend{fakeBackend: backend, t: t, member: member.Name, id: member.ServiceID, full: mode == "full-delete"}
			rc.Backend = b
			done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: mode == "suspend"})
			if err != nil || done || !b.observed {
				t.Fatalf("reconstructed cleanup: done=%t observed=%t err=%v", done, b.observed, err)
			}
			restored, err := load(rc.Cluster)
			if err != nil || restored.Pending == nil || restored.Pending.Op.ID != "persisted-delete-receipt" {
				t.Fatalf("original receipt lost: %+v %v", restored, err)
			}
			if rc.Cluster.Deleting && !strings.Contains(rc.Cluster.Status.Message, "escalated removal") {
				t.Fatal("backend diagnostic not surfaced")
			}
		})
	}
}
