// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type rejectedResourceBackend struct {
	infra.Backend
	calls int
	cause *infra.ResourceUpdateRejectedError
}

func (b *rejectedResourceBackend) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	b.calls++
	if resources.MaxCPU == 8 {
		return infra.Op{}, b.cause
	}
	return b.Backend.SetResources(ctx, name, resources)
}

func TestResourcePlacementRejectionAllowsRevert(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.Members[0]
	original := rc.Cluster.Spec.Components.Engine.Resources
	applied := m.Resources
	target := applied
	target.MaxCPU = 8
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU = 8
	backend := &rejectedResourceBackend{Backend: rc.Backend, cause: &infra.ResourceUpdateRejectedError{Message: "node node-a: dimension cpu exhausted"}}
	rc.Backend = backend
	err := p.submit(t.Context(), rc, s, &pending{Action: "resources", Member: m.Name, Resources: target})
	var rejected *infra.ResourceUpdateRejectedError
	if !errors.As(err, &rejected) || rejected != backend.cause || s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 {
		t.Fatalf("rejection retained retry or erased type: %v %+v", err, s)
	}
	if !reflect.DeepEqual(m.Resources, applied) {
		t.Fatal("rejected resources became applied")
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Components.Engine.Resources = original
	rc.Cluster.Generation++
	ready(t, p, rc)
	if backend.calls != 1 || state(t, rc).Retry != nil {
		t.Fatalf("revert replayed rejected resource target: %d", backend.calls)
	}
}
