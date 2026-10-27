// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package neo4j

import (
	"errors"
	"reflect"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type noDiskGrowthBackend struct{ infra.Backend }

func (b noDiskGrowthBackend) Capabilities() infra.Capabilities {
	caps := b.Backend.Capabilities()
	caps.DiskGrowth = false
	return caps
}

func TestUnsupportedDiskGrowthKeepsServing(t *testing.T) {
	p, rc, b, a := setup(t)
	converge(t, p, rc)
	before, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	calls, restarts := b.Calls(), a.restarts
	rc.Backend = noDiskGrowthBackend{b}
	rc.Cluster.Spec.Components.Engine.Resources.MinDisk++
	rc.Cluster.Spec.Components.Engine.Resources.MaxDisk++
	rc.Cluster.Generation++
	// Replay with a fresh provider too: rejection must not persist a resize
	// receipt, change config or submit a runtime resize before the volume gate.
	for range 3 {
		p = New(Options{ControlPlaneURL: "https://control.example.test"})
		if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrNotSupported) {
			t.Fatalf("growth: %v", err)
		}
		after, err := load(rc.Cluster)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatalf("unsupported growth changed member state: %v", err)
		}
		if !reflect.DeepEqual(calls, b.Calls()) || a.restarts != restarts {
			t.Fatal("unsupported growth touched infrastructure or restarted Neo4j")
		}
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady || len(status.Members) != 1 || !status.Members[0].Health || status.Members[0].Version == "" {
		t.Fatalf("serving cluster lost readiness: %+v %v", status, err)
	}
	// Revert the unapplied desire, using the same baseline as admission. The
	// old desired value must not turn this correction into a forbidden shrink.
	previous, err := engine.VolumeAdmissionBaseline(p, rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Components.Engine.Resources.MinDisk--
	rc.Cluster.Spec.Components.Engine.Resources.MaxDisk--
	rc.Cluster.Generation++
	if err := p.Validate(t.Context(), previous, rc.Cluster); err != nil {
		t.Fatal("revert rejected", err)
	}
	converge(t, p, rc)
	status, err = p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("reverted size did not converge: %+v %v", status, err)
	}
}
