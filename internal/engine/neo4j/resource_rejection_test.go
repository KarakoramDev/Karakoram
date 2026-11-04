// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type resourceRejectedReceiptBackend struct {
	infra.Backend
	cause          *infra.ResourceUpdateRejectedError
	polls, resizes int
}

func (b *resourceRejectedReceiptBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "rejected-resource-op" {
		b.polls++
		return infra.OpState{Failed: true, Message: b.cause.Error()}, b.cause
	}
	return b.Backend.OpStatus(ctx, op)
}

func (b *resourceRejectedReceiptBackend) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	b.resizes++
	return b.Backend.SetResources(ctx, name, resources)
}

func TestResourcePlacementRejectedReceiptAllowsRevert(t *testing.T) {
	p, rc, base, _ := setup(t)
	converge(t, p, rc)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	original, applied := rc.Cluster.Spec.Components.Engine.Resources, s.Member.Resources
	target := original
	target.MaxCPU = 8
	track(s.Member, "resize", infra.Op{ID: "rejected-resource-op"})
	s.Member.PendingResources = target
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Components.Engine.Resources = target
	rc.Cluster.Generation++
	backend := &resourceRejectedReceiptBackend{Backend: base, cause: &infra.ResourceUpdateRejectedError{Message: "node node-a: dimension cpu exhausted; running allocation unchanged"}}
	rc.Backend = backend
	_, err = p.Sync(t.Context(), rc)
	var rejected *infra.ResourceUpdateRejectedError
	if !errors.As(err, &rejected) || rejected != backend.cause {
		t.Fatalf("terminal receipt lost rejection type: %v", err)
	}
	s, err = load(rc.Cluster)
	if err != nil || s.Member.Pending != nil || s.Member.PendingAction != "" || !reflect.DeepEqual(s.Member.Resources, applied) {
		t.Fatalf("terminal receipt retained/applied rejected bounds: %+v %v", s, err)
	}
	rc.Cluster.Spec.Components.Engine.Resources = original
	rc.Cluster.Generation++
	converge(t, p, rc)
	if backend.polls != 1 || backend.resizes != 0 {
		t.Fatalf("revert replayed rejected receipt: polls=%d resizes=%d", backend.polls, backend.resizes)
	}
}
