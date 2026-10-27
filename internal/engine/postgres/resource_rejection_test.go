// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type resourceRejectedReceiptBackend struct {
	*fakeBackend
	cause          *infra.ResourceUpdateRejectedError
	polls, resizes int
}

func (b *resourceRejectedReceiptBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "rejected-resource-op" {
		b.polls++
		return infra.OpState{Failed: true, Message: b.cause.Error()}, b.cause
	}
	return b.fakeBackend.OpStatus(ctx, op)
}

func (b *resourceRejectedReceiptBackend) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	b.resizes++
	return b.fakeBackend.SetResources(ctx, name, resources)
}

func TestResourcePlacementRejectedReceiptAllowsRevert(t *testing.T) {
	for _, topology := range []string{"single", "ha", "distributed"} {
		t.Run(topology, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var base *fakeBackend
			if topology == "distributed" {
				p, rc, base, _ = setupDistributed(t, 3)
			} else {
				p, rc, base = setup(t, topology)
			}
			s := converge(t, p, rc)
			m := s.data()[0]
			original, applied := rc.Cluster.Spec.Components.Engine.Resources, m.Resources
			target := original
			target.MaxCPU = 8
			p.track(s, m, "resize", infra.Op{ID: "rejected-resource-op"})
			s.Pending.Resources = target
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.Components.Engine.Resources = target
			rc.Cluster.Generation++
			backend := &resourceRejectedReceiptBackend{fakeBackend: base, cause: &infra.ResourceUpdateRejectedError{Message: "node node-a: dimension cpu exhausted; running allocation unchanged"}}
			rc.Backend = backend
			_, err := p.Sync(t.Context(), rc)
			var rejected *infra.ResourceUpdateRejectedError
			if !errors.As(err, &rejected) || rejected != backend.cause {
				t.Fatalf("terminal receipt lost rejection type: %v", err)
			}
			reloaded, err := load(rc.Cluster)
			if err != nil || reloaded.Pending != nil || !reflect.DeepEqual(reloaded.find(m.Name).Resources, applied) {
				t.Fatalf("terminal receipt retained/applied rejected bounds: %+v %v", reloaded, err)
			}
			// The backend keeps returning this terminal outcome forever. A
			// corrected generation must not poll that receipt again.
			rc.Cluster.Spec.Components.Engine.Resources = original
			rc.Cluster.Generation++
			reloaded = converge(t, p, rc)
			if backend.polls != 1 || backend.resizes != 0 || reloaded.Pending != nil || !reflect.DeepEqual(reloaded.find(m.Name).Resources, applied) {
				t.Fatalf("revert held by rejected receipt: polls=%d resizes=%d", backend.polls, backend.resizes)
			}
		})
	}
}
