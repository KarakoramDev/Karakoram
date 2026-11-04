// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine/enginetest"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestExhaustedInitialDeploymentRearmAppliesResources(t *testing.T) {
	f := coreSetup(t, 1)
	enginetest.AssertDeploymentResourceRearm(t, f.rc, func() error {
		_, err := f.p.Sync(t.Context(), f.rc)
		return err
	}, func() infra.Resources {
		r := &f.rc.Cluster.Spec.Components.Engine.Resources
		r.MinRAM *= 2
		r.MaxRAM *= 2
		return *r
	})
}

func TestRuntimeRecoveryPreservesAcceptedResourcesInClosedWindow(t *testing.T) {
	f := coreSetup(t, 1)
	before := coreConverge(t, f)
	m := before.Members[0]
	accepted := m.Resources
	f.rc.Cluster.Generation++
	f.rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
	if _, err := f.backend.DeleteServiceByID(t.Context(), m.Name, m.ServiceID); err != nil {
		t.Fatal(err)
	}
	enginetest.AssertRuntimeRecoveryPreservesResources(t, f.rc, m.Name, accepted, runtimeResources(accepted), func() error {
		_, err := f.p.Sync(t.Context(), f.rc)
		return err
	}, func() (infra.Resources, bool, error) {
		s, err := load(f.rc.Cluster)
		if err != nil {
			return infra.Resources{}, false, err
		}
		current := s.find(m.Name)
		return current.Resources, current.Stage == "ready" && s.Pending == nil, nil
	})
}
