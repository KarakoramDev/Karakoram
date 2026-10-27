// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestResourcePlacementRejectionReleasesRetryForRevert(t *testing.T) {
	p, rc, f, _ := retrySetup(t)
	s := syncToPending(t, p, rc, "deploy")
	m := s.Members[0]
	m.Role = agentapi.RoleRouter
	original := rc.Cluster.Spec.Components.Engine.Resources
	applied := m.Resources
	target := original
	target.MaxCPU = 8
	v := &pending{Action: "resources", Member: m.Name, Resources: target}
	s.Pending, s.Retry = nil, v
	s.PlatformRetries = map[string]*platformRetry{platformRetryKey(v): {}}
	cause := &infra.ResourceUpdateRejectedError{Message: "node node-a: dimension cpu exhausted"}
	rc.Backend = &cooldownInfra{fakeInfra: f.fakeInfra, err: fmt.Errorf("wrapped: %w", cause)}
	_, err := p.retryPending(t.Context(), rc, s)
	var rejected *infra.ResourceUpdateRejectedError
	if !errors.As(err, &rejected) || rejected != cause || s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 {
		t.Fatalf("rejected resource target retained/retyped: %v %+v", err, s)
	}
	if !reflect.DeepEqual(m.Resources, applied) {
		t.Fatal("rejected bounds became applied")
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Components.Engine.Resources = original
	rc.Cluster.Generation++
	s = retryState(t, rc)
	before := f.mutations
	if busy, err := p.retryPending(t.Context(), rc, s); busy || err != nil || f.mutations != before {
		t.Fatalf("revert blocked by obsolete retry: busy=%v err=%v", busy, err)
	}
}
