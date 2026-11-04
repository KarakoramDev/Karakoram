// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type rejectedResourceInfra struct {
	*coreInfra
	calls int
	cause *infra.ResourceUpdateRejectedError
}

func (b *rejectedResourceInfra) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	b.calls++
	if resources.MaxCPU == 8 {
		return infra.Op{}, b.cause
	}
	return b.coreInfra.SetResources(ctx, name, resources)
}

func TestResourcePlacementRejectionAllowsRevert(t *testing.T) {
	p, rc, f, _ := coreSetup(t, 1)
	s := coreConverge(t, p, rc, f)
	m := s.Members[0]
	original := rc.Cluster.Spec.Components.Engine.Resources
	applied := m.Resources
	target := applied
	target.MaxCPU = 8
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU = 8
	backend := &rejectedResourceInfra{coreInfra: f, cause: &infra.ResourceUpdateRejectedError{Message: "node node-a: dimension cpu exhausted; running allocation unchanged"}}
	rc.Backend = backend
	err := p.issueAction(t.Context(), rc, s, &pending{Action: "resources", Member: m.Name, Resources: target})
	var rejected *infra.ResourceUpdateRejectedError
	if !errors.As(err, &rejected) || rejected != backend.cause {
		t.Fatalf("provider erased terminal resource rejection: %v", err)
	}
	if s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 || !reflect.DeepEqual(m.Resources, applied) {
		t.Fatalf("rejected intent remained queued: %+v", s)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Components.Engine.Resources = original
	rc.Cluster.Generation++
	reloaded := coreConverge(t, p, rc, f)
	if reloaded.Pending != nil || reloaded.Retry != nil || !reflect.DeepEqual(reloaded.find(m.Name).Resources, applied) || backend.calls != 1 {
		t.Fatalf("revert replayed rejected target: calls=%d state=%+v", backend.calls, reloaded)
	}
}
