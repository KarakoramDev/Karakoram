// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type pendingDeleteBackend struct {
	*projectBackend
	supersedes  bool
	deleteDone  bool
	deleteErr   error
	deleteID    string
	afterDelete func()
}

func (b *pendingDeleteBackend) SupersedesDeploymentsOnDelete() bool { return b.supersedes }
func (b *pendingDeleteBackend) DeleteServiceByID(_ context.Context, hostname, id string) (infra.Op, error) {
	b.deletes++
	b.deleteID = id
	if hostname != "controlrelay" || id != "owned-relay" {
		return infra.Op{}, infra.ErrConflict
	}
	if b.deleteErr != nil {
		return infra.Op{}, b.deleteErr
	}
	if b.afterDelete != nil {
		b.afterDelete()
	}
	return infra.Op{ID: "delete-receipt", Kind: "delete", ServiceID: id}, nil
}
func (b *pendingDeleteBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.Kind == "delete" {
		return infra.OpState{Done: b.deleteDone}, nil
	}
	return b.projectBackend.OpStatus(ctx, op)
}

func setupPendingRelayDelete(t *testing.T, supersedes bool) (*Registry, *pendingDeleteBackend, *domain.Namespace) {
	t.Helper()
	r, original, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	n.Phase = "Deleting"
	n.Relay.ServiceID = "owned-relay"
	n.PendingOpID = "never-terminal-deploy"
	n.PendingOpKind = "deploy"
	n.PendingOpStartedAt = time.Now().Add(-16 * time.Minute)
	original.opDone = false
	original.service = &infra.Service{ID: n.Relay.ServiceID, Hostname: n.Relay.Hostname}
	b := &pendingDeleteBackend{projectBackend: original, supersedes: supersedes}
	r.backends[n.Name] = b
	if err := r.save(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	return r, b, n
}

func assertNamespacePresent(t *testing.T, r *Registry, name string) *domain.Namespace {
	t.Helper()
	n, err := r.o.Store.GetNamespaceForUpdate(t.Context(), name)
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func finishPendingRelayDelete(t *testing.T, r *Registry, b *pendingDeleteBackend, name string) {
	t.Helper()
	b.deleteDone = true
	r.step(t.Context(), name)
	assertNamespacePresent(t, r, name) // A successful receipt alone is not absence.
	b.service = nil
	r.step(t.Context(), name)
	if _, err := r.o.Store.GetNamespaceForUpdate(t.Context(), name); !errors.Is(err, store.ErrNotFound) {
		t.Fatalf("namespace not removed after successful delete and absence: %v", err)
	}
}

func TestDeletingNamespaceSupersedesPendingRelayDeploy(t *testing.T) {
	for _, kind := range []string{"deploy", "restart"} {
		t.Run(kind, func(t *testing.T) {
			r, b, n := setupPendingRelayDelete(t, true)
			n.PendingOpKind = kind
			if err := r.save(t.Context(), n); err != nil {
				t.Fatal(err)
			}
			r.step(t.Context(), n.Name)
			got := assertNamespacePresent(t, r, n.Name)
			if b.deletes != 1 || b.deleteID != n.Relay.ServiceID || got.PendingOpID != "delete-receipt" || got.PendingOpKind != "delete" {
				t.Fatalf("delete did not supersede %s: calls=%d id=%q pending=%q/%q", kind, b.deletes, b.deleteID, got.PendingOpKind, got.PendingOpID)
			}
			// Even observed absence cannot bypass an unfinished delete receipt.
			service := b.service
			b.service = nil
			r.step(t.Context(), n.Name)
			assertNamespacePresent(t, r, n.Name)
			if b.deletes != 1 {
				t.Fatal("in-flight delete was resubmitted")
			}
			b.service = service
			finishPendingRelayDelete(t, r, b, n.Name)
		})
	}
}

func TestDeletingNamespaceWaitsForNonSupersedingRelayBackend(t *testing.T) {
	r, b, n := setupPendingRelayDelete(t, false)
	r.step(t.Context(), n.Name)
	got := assertNamespacePresent(t, r, n.Name)
	if b.deletes != 0 || got.PendingOpID != n.PendingOpID || !strings.Contains(got.Relay.LastError, "terminal outcome") || strings.Contains(got.Relay.LastError, "cancel") {
		t.Fatalf("unsafe waiting policy: calls=%d pending=%q error=%q", b.deletes, got.PendingOpID, got.Relay.LastError)
	}
	got.RetryAt = time.Time{}
	if err := r.save(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	b.opDone = true
	r.step(t.Context(), n.Name)
	r.step(t.Context(), n.Name)
	if b.deletes != 1 {
		t.Fatal("terminal operation did not release cleanup")
	}
	finishPendingRelayDelete(t, r, b, n.Name)
}

func TestDeletingNamespaceSupersedeRefusalRetainsPendingRelay(t *testing.T) {
	r, b, n := setupPendingRelayDelete(t, true)
	b.deleteErr = infra.ErrConflict
	r.step(t.Context(), n.Name)
	got := assertNamespacePresent(t, r, n.Name)
	if got.PendingOpID != n.PendingOpID || got.PendingOpKind != n.PendingOpKind || !got.PendingOpStartedAt.Equal(n.PendingOpStartedAt) || !strings.Contains(got.Relay.LastError, "active lifecycle leases") || !got.RetryAt.After(time.Now()) {
		t.Fatalf("refusal lost receipt or actionable backoff: pending=%q error=%q", got.PendingOpID, got.Relay.LastError)
	}
	got.RetryAt = time.Time{}
	if err := r.save(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	b.deleteErr = nil
	r.step(t.Context(), n.Name)
	if assertNamespacePresent(t, r, n.Name).PendingOpKind != "delete" {
		t.Fatal("retry did not accept delete")
	}
	finishPendingRelayDelete(t, r, b, n.Name)
}

func TestDeletingNamespaceLostSupersedeSaveConverges(t *testing.T) {
	for _, absent := range []bool{false, true} {
		t.Run(map[bool]string{false: "service-remains", true: "already-absent"}[absent], func(t *testing.T) {
			r, b, n := setupPendingRelayDelete(t, true)
			// Cancel after backend acceptance to model a CP stopping before
			// its receipt save. Only the old durable receipt may survive.
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			b.afterDelete = func() {
				b.afterDelete = nil
				cancel()
			}
			r.step(ctx, n.Name)
			if assertNamespacePresent(t, r, n.Name).PendingOpID != n.PendingOpID {
				t.Fatal("lost save unexpectedly replaced durable old receipt")
			}
			if absent {
				b.service = nil
			}
			r.step(t.Context(), n.Name)
			got := assertNamespacePresent(t, r, n.Name)
			if b.deletes != 2 || got.PendingOpID != "delete-receipt" {
				t.Fatalf("lost save did not converge: calls=%d pending=%q", b.deletes, got.PendingOpID)
			}
			finishPendingRelayDelete(t, r, b, n.Name)
		})
	}
}

func TestDeletingNamespaceStillResolvesPendingRelayImport(t *testing.T) {
	r, b, n := setupPendingRelayDelete(t, true)
	n.PendingOpKind = "import"
	if err := r.save(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	r.step(t.Context(), n.Name)
	got := assertNamespacePresent(t, r, n.Name)
	if b.deletes != 0 || got.PendingOpID != n.PendingOpID || !strings.Contains(got.Relay.LastError, "terminal outcome") {
		t.Fatal("unresolved import lost its normal observation fence")
	}
}
