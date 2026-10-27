// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestDiskGrowthLegacyRevertAndResume(t *testing.T) {
	s, _, token := testServer(t)
	p := neo4j.New(neo4j.Options{})
	var err error
	s.Registry, err = engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	caps := infra.Capabilities{Logs: true}
	b := fake.New(fake.Options{Name: "dokploy", Capabilities: caps})
	s.namespaces = engine.StaticNamespaces{DefaultBackend: b}
	op, err := b.ImportServices(t.Context(), []infra.ServiceSpec{engine.VolumeServiceSpec("legacy", "legacyn0", "legacyn0v", "token", domain.Resources{MinDisk: 10, MaxDisk: 10})})
	if err != nil {
		t.Fatal(err)
	}
	c := &domain.Cluster{Name: "legacy", Generation: 6}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Components.Engine.Resources.MinDisk = 11
	c.Spec.Components.Engine.Resources.MaxDisk = 11
	c.Status = domain.ClusterStatus{Phase: domain.PhaseSuspended, ObservedGeneration: 5}
	// Deliberately poison the legacy provider cache: the backend still has 10.
	c.Internal = []byte(`{"neo4j":{"uid":"legacy","configVersion":1,"suspended":true,"member":{"name":"legacyn0","volume":"legacyn0v","volumeId":"` + op.Services["legacyn0v"] + `","stage":"planned","disk":{"minDisk":11,"maxDisk":11}}}}`)
	if err := s.Store.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	before, err := s.Store.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters/legacy/resume", "", token)
	if w.Code != 400 || !strings.Contains(w.Body.String(), "does not provide diskGrowth") {
		t.Fatal("stale desired size resumed", w.Code, w.Body.String())
	}
	after, err := s.Store.GetCluster(t.Context(), c.Ref())
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("rejected resume changed cluster: %v", err)
	}
	for _, prefix := range []string{"/api/v1/dry-run/clusters/", "/api/v1/clusters/"} {
		w = call(s, "PATCH", prefix+"legacy", `{"spec":{"components":{"engine":{"resources":{"minDisk":9,"maxDisk":9}}}}}`, token)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "cannot shrink") {
			t.Fatal("actual volume shrink accepted", w.Code, w.Body.String())
		}
		w = call(s, "PATCH", prefix+"legacy", `{"spec":{"components":{"engine":{"resources":{"minDisk":10,"maxDisk":10}}}}}`, token)
		if w.Code != 200 {
			t.Fatal("unapplied disk growth could not be reverted", w.Code, w.Body.String())
		}
	}
	w = call(s, "POST", "/api/v1/clusters/legacy/resume", "", token)
	if w.Code != 202 {
		t.Fatal("corrected retained volume could not resume", w.Code, w.Body.String())
	}
}
