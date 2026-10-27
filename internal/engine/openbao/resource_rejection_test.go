// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestResourcePlacementRejectionAllowsRevert(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom1")
	original := rc.Cluster.Spec.Components.Engine.Resources
	applied := m.Resources
	oversized := applied
	oversized.MaxCPU = 8
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU = 8
	m.Change = &memberChange{Kind: "resources", Step: "runtime-resources", Version: rc.Cluster.Spec.Version, Resources: oversized, Disk: m.Disk}
	cause := &infra.ResourceUpdateRejectedError{Message: "node node-a: dimension cpu exhausted; running allocation unchanged"}
	f.failures["resources:"+m.Name] = cause
	err := p.changeStep(t.Context(), rc, s, m, true)
	var rejected *infra.ResourceUpdateRejectedError
	if !errors.As(err, &rejected) || rejected != cause {
		t.Fatalf("provider erased definitive placement type: %v", err)
	}
	if s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 || m.Change != nil || !reflect.DeepEqual(m.Resources, applied) {
		t.Fatalf("rejection retained obsolete target or applied resources: %+v member=%+v", s, m)
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	// A controller restart followed by corrective intent must not replay the
	// rejected target first, even while that same backend rejection persists.
	rc.Cluster.Spec.Components.Engine.Resources = original
	rc.Cluster.Generation++
	mutations := f.mutations
	ready(t, p, rc)
	reloaded := state(t, rc)
	if reloaded.Retry != nil || reloaded.Pending != nil || reloaded.find(m.Name).Change != nil || !reflect.DeepEqual(reloaded.find(m.Name).Resources, applied) {
		t.Fatalf("revert stayed behind rejected resources: %+v", reloaded)
	}
	if f.mutations != mutations {
		t.Fatal("revert retried an unapplied rejected target")
	}
}

type resourceRejectedReceiptBackend struct {
	infra.Backend
	cause *infra.ResourceUpdateRejectedError
}

func (b *resourceRejectedReceiptBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Failed: true, Message: b.cause.Error()}, b.cause
}

func TestResourcePlacementRejectionDuringReceiptReleasesChange(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom1")
	m.Change = &memberChange{Kind: "resources", Step: "runtime-resources", Resources: m.Resources}
	s.Pending = &pending{Action: "resources", Member: m.Name, Resources: m.Resources, Op: infra.Op{ID: "accepted-stop"}}
	cause := &infra.ResourceUpdateRejectedError{Message: "node node-a: dimension cpu exhausted"}
	rc.Backend = &resourceRejectedReceiptBackend{Backend: rc.Backend, cause: cause}
	if _, err := p.observePending(t.Context(), rc, s); !errors.Is(err, cause) || s.Pending != nil || s.Retry != nil || m.Change != nil {
		t.Fatalf("terminal receipt retained rejected change: %v %+v", err, s)
	}
}
