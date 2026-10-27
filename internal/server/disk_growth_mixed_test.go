// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
	"reflect"
	"strings"
	"testing"
)

type mixedVolumeProvider struct {
	fakeProvider
	refs []engine.VolumeReference
}

func (p *mixedVolumeProvider) VolumeReferences(*domain.Cluster) ([]engine.VolumeReference, error) {
	return p.refs, nil
}

func TestDiskGrowthAdmissionChecksSmallerExistingVolume(t *testing.T) {
	s, _, token := testServer(t)
	b := fake.New(fake.Options{Name: "nomad", Capabilities: infra.Capabilities{Logs: true}})
	s.namespaces = engine.StaticNamespaces{DefaultBackend: b}
	p := &mixedVolumeProvider{}
	var err error
	s.Registry, err = engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	for _, item := range []struct {
		name string
		size float64
	}{{"small", 10}, {"large", 11}} {
		op, err := b.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: item.name, Kind: infra.KindVolume, Resources: domain.Resources{MinDisk: item.size, MaxDisk: item.size}}})
		if err != nil {
			t.Fatal(err)
		}
		p.refs = append(p.refs, engine.VolumeReference{Component: "engine", Hostname: item.name, ID: op.Services[item.name]})
	}
	c := &domain.Cluster{Name: "mixed", Generation: 6, Status: domain.ClusterStatus{Phase: domain.PhaseSuspended}}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Components.Engine.Resources.MinDisk, c.Spec.Components.Engine.Resources.MaxDisk = 11, 11
	if err := s.Store.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	before, err := s.Store.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	for _, prefix := range []string{"/api/v1/dry-run/clusters/", "/api/v1/clusters/"} {
		w := call(s, "PATCH", prefix+"mixed", `{"spec":{"components":{"engine":{"resources":{"minDisk":11,"maxDisk":11}}}}}`, token)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "does not provide diskGrowth") {
			t.Fatal("smaller member growth admitted", w.Code, w.Body.String())
		}
	}
	w := call(s, "POST", "/api/v1/clusters/mixed/resume", "", token)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "does not provide diskGrowth") {
		t.Fatal("mixed-size resume admitted", w.Code, w.Body.String())
	}
	after, err := s.Store.GetCluster(t.Context(), c.Ref())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("rejection changed cluster", err)
	}
}
