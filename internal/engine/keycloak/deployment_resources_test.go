// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package keycloak

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestExhaustedInitialDeploymentRearmAppliesResources(t *testing.T) {
	p, rc, _, _ := setup(t)
	enginetest.AssertDeploymentResourceRearm(t, rc, func() error {
		_, err := p.Sync(t.Context(), rc)
		return err
	}, func() infra.Resources {
		r := &rc.Cluster.Spec.Components.Engine.Resources
		r.MinRAM *= 2
		r.MaxRAM *= 2
		return *r
	})
}

func TestRuntimeRecoveryPreservesAcceptedResourcesInClosedWindow(t *testing.T) {
	p, rc, bindings, _ := setup(t)
	converge(t, p, rc)
	before, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	accepted := before.Resources
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
	bindings.backend.service = nil
	enginetest.AssertRuntimeRecoveryPreservesResources(t, rc, before.Member, accepted, accepted, func() error {
		_, err := p.Sync(t.Context(), rc)
		return err
	}, func() (infra.Resources, bool, error) {
		s, err := load(rc.Cluster)
		if err != nil {
			return infra.Resources{}, false, err
		}
		return s.Resources, s.Deployed && s.Pending == nil, nil
	})
}
