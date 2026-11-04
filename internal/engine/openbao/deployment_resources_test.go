// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

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
	p, rc, base, _ := setup(t)
	ready(t, p, rc)
	m := state(t, rc).find("demom1")
	accepted := m.Resources
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU++
	delete(base.services, m.Name)
	enginetest.AssertRuntimeRecoveryPreservesResources(t, rc, m.Name, accepted, accepted, func() error {
		_, err := p.Sync(t.Context(), rc)
		return err
	}, func() (infra.Resources, bool, error) {
		s := state(t, rc)
		current := s.find(m.Name)
		// This regression covers the completed infrastructure receipt; native
		// unseal/readiness remains a separate engine-owned recovery lifecycle.
		restored := current.Stage == "configured" || current.Stage == "ready"
		return current.Resources, restored && s.Pending == nil && current.ResumeDeploy == nil, nil
	})
}

func TestImportedRecoveryDoesNotReplaceAdmittedResourceTarget(t *testing.T) {
	p, rc, base, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom1")
	accepted := m.Resources
	frozen := accepted
	frozen.MaxCPU++
	m.Stage = "imported"
	m.Initialized = false
	m.RuntimeRecovery.Initialized = false
	m.RuntimeRecovery.Deployed = false
	m.Change = &memberChange{Kind: "resources", Step: "runtime-resources", Started: true, Resources: frozen}
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU += 2
	base.log.clear()
	if err := p.deploy(t.Context(), rc, s, m, m.DesiredVersion); err != nil {
		t.Fatal(err)
	}
	if m.Resources != accepted || m.Change.Resources != frozen || m.Change.Step != "runtime-resources" || s.Pending != nil {
		t.Fatal("recovery overwrote or advanced an admitted member-resource target")
	}
	for _, call := range base.log.snapshot() {
		if call == "infra:"+m.Name+":resources" {
			t.Fatal("recovery submitted current desired resources over the frozen target")
		}
	}
}
