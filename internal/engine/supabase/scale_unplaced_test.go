// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type supersedingScaleBackend struct {
	blockedDeployBackend
	deployPolls int
	deletes     []struct{ name, id string }
}

func (*supersedingScaleBackend) SupersedesDeploymentsOnDelete() bool { return true }
func (b *supersedingScaleBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "unready-deploy" {
		b.deployPolls++
	}
	return b.blockedDeployBackend.OpStatus(ctx, op)
}
func (b *supersedingScaleBackend) DeleteServiceByID(ctx context.Context, name, id string) (infra.Op, error) {
	b.deletes = append(b.deletes, struct{ name, id string }{name, id})
	return b.testBackend.DeleteServiceByID(ctx, name, id)
}
func TestDisableComponentSupersedesUnplacedDeploy(t *testing.T) {
	p, rc, b := setupProvider(t)
	for _, name := range []string{"meta", "studio"} {
		d := rc.Cluster.Spec.Components.Extra[name]
		d.Replicas = 1
		rc.Cluster.Spec.Components.Extra[name] = d
	}
	convergeProvider(t, p, rc)
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := s.Members["studio"]
	m.Stage = engine.ComponentImported
	s.Pending = &componentPending{Component: "studio", Action: "deploy", Op: infra.Op{ID: "unready-deploy", Kind: "deploy"}}
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	d := rc.Cluster.Spec.Components.Extra["studio"]
	d.Replicas = 0
	rc.Cluster.Spec.Components.Extra["studio"] = d
	backend := &supersedingScaleBackend{blockedDeployBackend: blockedDeployBackend{testBackend: b}}
	rc.Backend = backend
	for range 40 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		if backend.deployPolls != 0 {
			t.Fatal("public Sync polled the obsolete disabled-component deployment")
		}
		s, _ = loadState(rc.Cluster)
		if s.Members["studio"] == nil {
			if b.services[m.Name] != nil {
				t.Fatal("disabled component resource remains")
			}
			if len(backend.deletes) != 1 || backend.deletes[0].name != m.Name || backend.deletes[0].id != m.ServiceID {
				t.Fatal("disabled component was not deleted by its exact recorded identity", backend.deletes)
			}
			return
		}
	}
	t.Fatal("disabled component waited for obsolete deployment")
}
