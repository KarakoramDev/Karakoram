// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type heldComponentBackend struct {
	*supersedingScaleBackend
	polls int
}

// A backend without safe deletion supersession must retain every receipt.
func (*heldComponentBackend) SupersedesDeploymentsOnDelete() bool { return false }

func (b *heldComponentBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "held-component-receipt" {
		b.polls++
		return infra.OpState{Status: "pending"}, nil
	}
	return b.supersedingScaleBackend.OpStatus(ctx, op)
}

func TestDisabledComponentPreservesPendingReceiptsThroughSync(t *testing.T) {
	for _, action := range []string{"import", "delete", "resize", "deploy"} {
		t.Run(action, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			for _, name := range []string{"meta", "studio"} {
				desired := rc.Cluster.Spec.Components.Extra[name]
				desired.Replicas = 1
				rc.Cluster.Spec.Components.Extra[name] = desired
			}
			convergeProvider(t, p, rc)
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			state.Pending = &componentPending{Component: "studio", Action: action, Op: infra.Op{ID: "held-component-receipt", Kind: action}}
			if err := saveState(rc.Cluster, state); err != nil {
				t.Fatal(err)
			}
			desired := rc.Cluster.Spec.Components.Extra["studio"]
			desired.Replicas = 0
			rc.Cluster.Spec.Components.Extra["studio"] = desired
			b := &heldComponentBackend{supersedingScaleBackend: &supersedingScaleBackend{blockedDeployBackend: blockedDeployBackend{testBackend: backend}}}
			rc.Backend = b
			// Core recovery must not admit competing optional work or discard
			// receipt ownership even when the disabled service is the remedy.
			backend.stopped = map[string]bool{"demop0": true}
			for range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				state, err = loadState(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				if state.Pending == nil || state.Pending.Action != action || state.Pending.Op.ID != "held-component-receipt" {
					t.Fatalf("disabled component discarded %s receipt: %+v", action, state.Pending)
				}
			}
			if b.polls != 3 || len(b.deletes) != 0 {
				t.Fatal("import/deletion observation was bypassed", b.polls, b.deletes)
			}
		})
	}
}

func TestDisabledComponentCleanupDuringCoreFailureFencesPositiveWork(t *testing.T) {
	for _, conflict := range []bool{false, true} {
		t.Run(map[bool]string{false: "owned", true: "foreign"}[conflict], func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			convergeProvider(t, p, rc)
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			rest := state.Members["rest"]
			authReplicas := backend.services["demoau"].MinContainers
			disabled := rc.Cluster.Spec.Components.Extra["rest"]
			disabled.Replicas = 0
			rc.Cluster.Spec.Components.Extra["rest"] = disabled
			for _, name := range []string{"meta", "studio"} {
				desired := rc.Cluster.Spec.Components.Extra[name]
				desired.Replicas = 1
				rc.Cluster.Spec.Components.Extra[name] = desired
			}
			auth := rc.Cluster.Spec.Components.Extra["auth"]
			auth.Replicas++
			rc.Cluster.Spec.Components.Extra["auth"] = auth
			rc.Cluster.Spec.EngineOptions = []byte(`{"gateway":{"publicHttps":true}}`)
			rc.Cluster.Generation++
			applied := rc.Cluster.Status.ObservedGeneration
			backend.stopped = map[string]bool{"demop0": true}
			if conflict {
				backend.services[rest.Name].Ownership[engine.OwnerVariable] = "foreign"
			}
			public := &publicBackend{testBackend: backend}
			rc.Backend = public
			for range 4 {
				_, err := p.Sync(t.Context(), rc)
				if conflict {
					if err == nil {
						t.Fatal("foreign component deletion was admitted")
					}
				} else if err != nil {
					t.Fatal(err)
				}
			}
			if (backend.services[rest.Name] != nil) != conflict {
				t.Fatal("cleanup did not respect existing service ownership")
			}
			if backend.services["demomt"] != nil || backend.services["demosd"] != nil ||
				backend.services["demoau"].MinContainers != authReplicas || public.enabled {
				t.Fatal("core failure admitted new deployment, positive scaling or public exposure")
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase == domain.PhaseReady || rc.Cluster.Status.ObservedGeneration != applied {
				t.Fatal("cleanup hid core failure or acknowledged generation")
			}
		})
	}
}

func TestDisabledComponentCleanupDoesNotSwallowPendingError(t *testing.T) {
	p, rc, backend := setupProvider(t)
	convergeProvider(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	state.Pending = &componentPending{Component: "rest", Action: "resize", Op: infra.Op{ID: "unready-deploy"}}
	if err := saveState(rc.Cluster, state); err != nil {
		t.Fatal(err)
	}
	rest := rc.Cluster.Spec.Components.Extra["rest"]
	rest.Replicas = 0
	rc.Cluster.Spec.Components.Extra["rest"] = rest
	backend.stopped = map[string]bool{"demop0": true}
	rc.Backend = blockedDeployBackend{testBackend: backend, pollError: infra.ErrUnavailable}
	if _, err := p.Sync(t.Context(), rc); !errors.Is(err, infra.ErrUnavailable) {
		t.Fatalf("pending receipt error swallowed: %v", err)
	}
	state, err = loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if state.Pending == nil || state.Pending.Op.ID != "unready-deploy" || backend.services["demors"] == nil {
		t.Fatal("cleanup replaced a failing receipt or submitted a competing deletion")
	}
}

func TestDisabledMissingComponentIsNotReimportedBeforeCoreReady(t *testing.T) {
	p, rc, backend := setupProvider(t)
	convergeProvider(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := state.Members["rest"]
	delete(backend.services, m.Name)
	m.Stage = engine.ComponentPlanned
	m.ServiceID = ""
	if err := saveState(rc.Cluster, state); err != nil {
		t.Fatal(err)
	}
	rest := rc.Cluster.Spec.Components.Extra["rest"]
	rest.Replicas = 0
	rc.Cluster.Spec.Components.Extra["rest"] = rest
	backend.stopped = map[string]bool{"demop0": true}
	for range 4 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if backend.services[m.Name] != nil {
			t.Fatal("disabled missing component was re-imported")
		}
	}
	state, err = loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if state.Members["rest"] != nil || state.Pending != nil {
		t.Fatal("missing disabled identity did not finish normal deletion")
	}
}
