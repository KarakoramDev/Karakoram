// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestStoppedMatchingRelayRestarts(t *testing.T) {
	for _, status := range []string{infra.StatusStopped, infra.StatusFailed, infra.StatusReadyToDeploy, infra.StatusActive} {
		t.Run(status, func(t *testing.T) {
			r, b, _ := setupRegistry(t)
			n := createTestNamespace(t, r)
			n.DeployedExecutableHash = r.o.ExecutableHash
			n.SecretsPending = false
			b.service = &infra.Service{ID: "owned-relay", Hostname: n.Relay.Hostname, Status: status, Ownership: map[string]string{"KARAKORAM_RELAY_OWNER": n.OwnershipMarker}}
			if err := r.save(t.Context(), n); err != nil {
				t.Fatal(err)
			}
			r.step(t.Context(), n.Name)
			got, err := r.o.Store.GetNamespaceForUpdate(t.Context(), n.Name)
			if err != nil {
				t.Fatal(err)
			}
			switch status {
			case infra.StatusStopped, infra.StatusFailed:
				if b.restarts != 1 || got.PendingOpID == "" || !got.RetryAt.After(time.Now()) {
					t.Fatalf("stopped relay not recovered: restarts=%d operation=%q retry=%s", b.restarts, got.PendingOpID, got.RetryAt)
				}
			case infra.StatusReadyToDeploy:
				if b.deploys != 1 {
					t.Fatal("missing executable not redeployed")
				}
			default:
				if b.restarts != 0 || b.deploys != 0 {
					t.Fatal("healthy runtime restarted")
				}
			}
			for range 3 {
				r.step(t.Context(), n.Name)
			}
			if b.restarts > 1 || b.deploys > 1 {
				t.Fatal("recovery storm")
			}
		})
	}
}

func TestPendingRelayOperationSurfacesSafeRecovery(t *testing.T) {
	r, b, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	n.PendingOpID = "stalled-operation"
	n.PendingOpKind = "deploy"
	b.opDone = false
	if err := r.save(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	r.step(t.Context(), n.Name)
	got, err := r.o.Store.GetNamespaceForUpdate(t.Context(), n.Name)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(got.Relay.LastError, "stalled-operation") || !strings.Contains(got.Relay.LastError, "backend") {
		t.Fatalf("missing actionable pending diagnostic: %q", got.Relay.LastError)
	}
	if b.imports+b.deploys+b.restarts+b.deletes != 0 || got.PendingOpID != n.PendingOpID {
		t.Fatal("in-flight fence bypassed")
	}
	got.PendingOpStartedAt = time.Now().Add(-16 * time.Minute)
	if err := r.save(t.Context(), got); err != nil {
		t.Fatal(err)
	}
	r.step(t.Context(), got.Name)
	got, err = r.o.Store.GetNamespaceForUpdate(t.Context(), got.Name)
	if err != nil || !strings.Contains(got.Relay.LastError, "15 minutes") || got.PendingOpID == "" || b.deploys+b.deletes+b.restarts+b.imports != 0 {
		t.Fatalf("aged operation lost its actionable safety fence: %v", err)
	}
}
