// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type queuedDeleteBackend struct {
	*testBackend
	deletes          int
	observationError error
}

func (b *queuedDeleteBackend) DeleteServiceByID(_ context.Context, name, id string) (infra.Op, error) {
	if service := b.services[name]; service != nil && service.ID != id {
		return infra.Op{}, infra.ErrConflict
	}
	b.deletes++
	return infra.Op{ID: "delete-process", ServiceID: id, Kind: "delete"}, nil
}

func (b *queuedDeleteBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "delete-process" {
		return infra.OpState{Status: "PENDING", Message: "waiting for Zerops delete process delete-process: PENDING (age 44m0s); same-service platform process deploy-process (stack.deploy) RUNNING (age 1h8m0s)"}, b.observationError
	}
	return b.testBackend.OpStatus(ctx, op)
}

func TestCleanupReportsQueuedPlatformDeletionWithoutReplay(t *testing.T) {
	p, rc, backend := setupProvider(t)
	convergeProvider(t, p, rc)
	rc.Cluster.Deleting = true
	queued := &queuedDeleteBackend{testBackend: backend}
	rc.Backend = queued
	for range 3 {
		// Reconstruction must not lose the accepted deletion handle.
		p = New(p.options)
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
		if err != nil || done {
			t.Fatalf("queued delete not retained: done=%v err=%v", done, err)
		}
	}
	for _, want := range []string{"delete-process", "PENDING", "44m0s", "deploy-process", "RUNNING", "1h8m0s"} {
		if !strings.Contains(rc.Cluster.Status.Message, want) {
			t.Errorf("cleanup hid %q: %q", want, rc.Cluster.Status.Message)
		}
	}
	if queued.deletes != 1 {
		t.Fatalf("pending platform delete was submitted %d times", queued.deletes)
	}
	reset := time.Now().Add(time.Hour)
	queued.observationError = &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: reset}
	done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{})
	if done || !infra.RetryAt(err).Equal(reset) || queued.deletes != 1 {
		t.Fatalf("cleanup lost observation cooldown or replayed delete: done=%v err=%v deletes=%d", done, err, queued.deletes)
	}
	s, err := loadState(rc.Cluster)
	if err != nil || s.Pending == nil || s.Pending.Op.ID != "delete-process" {
		t.Fatalf("observation cooldown lost accepted deletion: %+v err=%v", s, err)
	}
}
