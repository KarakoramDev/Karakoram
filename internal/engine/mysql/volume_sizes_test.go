// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"testing"
)

func TestLegacyRetainedVolumeSizingUsesBackend(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := s.data()[0]
	actual := b.services[m.Volume].Resources
	// Model a legacy retained import that cached unapplied desired sizing.
	m.Resources.MinDisk, m.Resources.MaxDisk = actual.MinDisk+1, actual.MaxDisk+1
	rc.Cluster.Spec.Components.Engine.Resources.MinDisk, rc.Cluster.Spec.Components.Engine.Resources.MaxDisk = actual.MinDisk+1, actual.MaxDisk+1
	rc.Cluster.Generation++
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrNotSupported) {
		t.Fatalf("stale cache masked unsupported growth: %v", err)
	}
	s, _ = load(rc.Cluster)
	if s.Rolling != nil || s.data()[0].Offline {
		t.Fatal("unsupported growth started member maintenance")
	}
	rc.Cluster.Spec.Components.Engine.Resources.MinDisk, rc.Cluster.Spec.Components.Engine.Resources.MaxDisk = actual.MinDisk, actual.MaxDisk
	previous, err := engine.VolumeAdmissionBaseline(t.Context(), p, rc.Cluster, rc.Cluster.Spec, rc.Backend)
	if err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(t.Context(), previous, rc.Cluster); err != nil {
		t.Fatal("actual-size revert rejected", err)
	}
	rc.Cluster.Generation++
	converge(t, p, rc, b)
	s, _ = load(rc.Cluster)
	m = s.data()[0]
	if m.Resources.MinDisk != actual.MinDisk || m.Resources.MaxDisk != actual.MaxDisk {
		t.Fatal("stale retained volume cache survived corrected convergence")
	}
	if r := b.services[m.Volume].Resources; r.MinDisk != actual.MinDisk || r.MaxDisk != actual.MaxDisk {
		t.Fatal("actual-size correction resized volume")
	}
}
