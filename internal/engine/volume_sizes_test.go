// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

type sizingProvider struct {
	Provider
	refs []VolumeReference
}

func (p sizingProvider) VolumeReferences(*domain.Cluster) ([]VolumeReference, error) {
	return p.refs, nil
}

func TestVolumeAdmissionUsesEveryBackendVolume(t *testing.T) {
	b := fake.New(fake.Options{Name: "nomad", Capabilities: infra.Capabilities{Logs: true}})
	var refs []VolumeReference
	for _, v := range []struct {
		name string
		size float64
	}{{"small", 10}, {"large", 11}} {
		op, err := b.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: v.name, Kind: infra.KindVolume, Resources: domain.Resources{MinDisk: v.size, MaxDisk: v.size}}})
		if err != nil {
			t.Fatal(err)
		}
		refs = append(refs, VolumeReference{Component: "engine", Hostname: v.name, ID: op.Services[v.name]})
	}
	p := sizingProvider{refs: refs}
	c := &domain.Cluster{}
	c.Spec.Components.Engine = domain.ComponentSpec{Replicas: 2, Resources: domain.Resources{MinDisk: 12, MaxDisk: 12}}
	target := c.Spec
	target.Components.Engine.Resources.MinDisk, target.Components.Engine.Resources.MaxDisk = 11, 11
	if _, err := VolumeAdmissionBaseline(t.Context(), p, c, target, b); !errors.Is(err, ErrNotSupported) {
		t.Fatalf("smaller existing volume growth admitted: %v", err)
	}
	target.Components.Engine.Resources.MinDisk, target.Components.Engine.Resources.MaxDisk = 10, 10
	previous, err := VolumeAdmissionBaseline(t.Context(), p, c, target, b)
	if err != nil {
		t.Fatal(err)
	}
	if r := previous.Spec.Components.Engine.Resources; r.MinDisk != 11 || r.MaxDisk != 11 {
		t.Fatalf("shrink floor lost larger volume: %+v", r)
	}
	if c.Spec.Components.Engine.Resources.MinDisk != 12 {
		t.Fatal("admission changed caller")
	}
	refs = refs[:1]
	p.refs = refs
	previous, err = VolumeAdmissionBaseline(t.Context(), p, c, target, b)
	if err != nil || previous.Spec.Components.Engine.Resources.MinDisk != 10 {
		t.Fatalf("actual-size revert rejected by stale desired size: %+v %v", previous, err)
	}
	if _, err := ObserveVolumeSize(t.Context(), b, "small", "reused-id"); !errors.Is(err, infra.ErrConflict) {
		t.Fatalf("identity reuse accepted: %v", err)
	}
}
