// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic"
)

func TestElasticKibanaLegacyPatchAdmission(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	for _, tc := range []struct {
		name, patch      string
		status           int
		replicas         int
		minimum, maximum float64
	}{
		{"empty", `{"spec":{}}`, 200, 2, 1, 1},
		{"unrelated", `{"spec":{"monitoring":{"enabled":false}}}`, 200, 2, 1, 1},
		{"decrease", `{"spec":{"components":{"extra":{"kibana":{"replicas":1}}}}}`, 200, 1, 1, 1},
		{"disable", `{"spec":{"components":{"extra":{"kibana":{"replicas":0}}}}}`, 200, 0, 1, 1},
		{"increase", `{"spec":{"components":{"extra":{"kibana":{"replicas":3}}}}}`, 400, 2, 1, 1},
		{"partial", `{"spec":{"components":{"extra":{"kibana":{"resources":{"minRam":1.25,"maxRam":1.25}}}}}}`, 400, 2, 1, 1},
		{"correct", `{"spec":{"components":{"extra":{"kibana":{"resources":{"minRam":1.5,"maxRam":1.5}}}}}}`, 200, 2, 1.5, 1.5},
		{"upgrade", `{"spec":{"version":"9.5.5"}}`, 400, 2, 1, 1},
		{"combined upgrade", `{"spec":{"version":"9.5.5","components":{"extra":{"kibana":{"resources":{"minRam":1.5,"maxRam":1.5}}}}}}`, 400, 2, 1, 1},
		{"minimum only", `{"spec":{"components":{"extra":{"kibana":{"resources":{"minRam":1.5}}}}}}`, 400, 2, 1, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, token := testServer(t)
			p := elastic.New(elastic.ProviderOptions{Store: s.Store})
			if err := s.Registry.Register(p); err != nil {
				t.Fatal(err)
			}
			c := &domain.Cluster{Name: "legacy"}
			p.ApplyDefaults(&c.Spec)
			k := c.Spec.Components.Extra["kibana"]
			c.Spec.Version = "9.5.4"
			k.Replicas, k.Resources.MinRAM, k.Resources.MaxRAM = 2, 1, 1
			c.Spec.Components.Extra["kibana"] = k
			if err := s.Store.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			w := call(s, "PATCH", "/api/v1/clusters/legacy", tc.patch, token)
			if w.Code != tc.status {
				t.Fatalf("status %d: %s", w.Code, w.Body.String())
			}
			if tc.name == "increase" || tc.name == "partial" {
				for _, text := range []string{"spec.components.extra.kibana.resources.minRam", "1.5", "minRam", "maxRam"} {
					if !strings.Contains(w.Body.String(), text) {
						t.Errorf("response omits %q: %s", text, w.Body.String())
					}
				}
			}
			after, err := s.Store.GetCluster(t.Context(), c.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if (tc.name == "upgrade" || tc.name == "combined upgrade") && !strings.Contains(w.Body.String(), "resize Kibana first") {
				t.Fatalf("missing resize-first guidance: %s", w.Body.String())
			}
			got := after.Spec.Components.Extra["kibana"]
			if got.Replicas != tc.replicas || got.Resources.MinRAM != tc.minimum || got.Resources.MaxRAM != tc.maximum {
				t.Fatalf("unexpected persisted Kibana after merged/defaulted PATCH: %+v", got)
			}
		})
	}
}

func TestElasticKibanaUpgradeWaitsForResizeConvergence(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	s, _, token := testServer(t)
	p := elastic.New(elastic.ProviderOptions{Store: s.Store})
	if err := s.Registry.Register(p); err != nil {
		t.Fatal(err)
	}
	c := &domain.Cluster{Name: "legacy"}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Version = "9.5.4"
	k := c.Spec.Components.Extra["kibana"]
	k.Replicas, k.Resources.MinRAM, k.Resources.MaxRAM = 1, 1, 1
	c.Spec.Components.Extra["kibana"] = k
	if err := s.Store.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	c.Status.Phase, c.Status.ObservedGeneration = domain.PhaseReady, c.Generation
	if err := s.Store.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	w := call(s, "PATCH", "/api/v1/clusters/legacy", `{"spec":{"components":{"extra":{"kibana":{"resources":{"minRam":1.5,"maxRam":1.5}}}}}}`, token)
	if w.Code != 200 {
		t.Fatalf("resize status %d: %s", w.Code, w.Body.String())
	}
	resized, err := s.Store.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if resized.Status.ObservedGeneration == resized.Generation {
		t.Fatal("resize was already observed without reconciliation")
	}
	ops, err := s.Store.ListOperations(t.Context(), c.Ref(), domain.PhasePending, domain.PhaseRunning)
	if err != nil || len(ops) != 1 || ops[0].Kind != domain.OperationUpdate {
		t.Fatalf("missing pending resize operation: %+v %v", ops, err)
	}
	upgrade := `{"spec":{"version":"9.5.5"}}`
	w = call(s, "PATCH", "/api/v1/clusters/legacy", upgrade, token)
	if w.Code != 400 && w.Code != 409 {
		t.Fatalf("pending resize upgrade status %d: %s", w.Code, w.Body.String())
	}
	for _, text := range []string{"spec.version", `"reason":"conflict"`, "pending change", "Kibana", "ready"} {
		if !strings.Contains(w.Body.String(), text) {
			t.Errorf("response omits %q: %s", text, w.Body.String())
		}
	}
	after, err := s.Store.GetCluster(t.Context(), c.Ref())
	if err != nil {
		t.Fatal(err)
	}
	if after.Spec.Version != resized.Spec.Version || after.Generation != resized.Generation {
		t.Fatal("rejected upgrade changed desired generation")
	}
	pending, err := s.Store.GetOperation(t.Context(), ops[0].ID)
	if err != nil || pending.Phase != ops[0].Phase {
		t.Fatalf("rejected upgrade superseded resize: %+v %v", pending, err)
	}
	// Sync acknowledges a generation only after resources and component readiness converge.
	resized.Status.Phase, resized.Status.ObservedGeneration = domain.PhaseReady, resized.Generation
	if err := s.Store.UpdateClusterState(t.Context(), resized); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	pending.Phase, pending.CompletedAt = domain.PhaseSucceeded, &now
	if err := s.Store.UpdateOperation(t.Context(), pending); err != nil {
		t.Fatal(err)
	}
	w = call(s, "PATCH", "/api/v1/clusters/legacy", upgrade, token)
	if w.Code != 200 {
		t.Fatalf("converged upgrade status %d: %s", w.Code, w.Body.String())
	}
}
