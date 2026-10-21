// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/nativeartifacts"
)

func TestCitusOptions(t *testing.T) {
	for _, raw := range []string{"", `{}`, `{"citus":{}}`} {
		o, err := ParseOptions(json.RawMessage(raw))
		if err != nil {
			t.Fatal(err)
		}
		if got := o.citusOptions(); got.WorkerGroups != 2 || got.Database != "app" {
			t.Fatalf("defaults: %+v", got)
		}
	}
	for _, raw := range []string{
		`{"citus":null}`, `{"citus":[]}`, `{"citus":{"unknown":1}}`,
		`{"citus":{"workerGroups":0}}`, `{"citus":{"workerGroups":9}}`,
		`{"citus":{"workerGroups":1.5}}`, `{"citus":{"workerGroups":null}}`,
		`{"citus":{"database":null}}`, `{"citus":{"database":""}}`,
		`{"citus":{"database":"a\u0000b"}}`,
		`{"citus":{"database":"` + strings.Repeat("a", 64) + `"}}`,
	} {
		if _, err := ParseOptions(json.RawMessage(raw)); err == nil {
			t.Errorf("accepted %s", raw)
		}
	}
	o, err := ParseOptions(json.RawMessage(`{"citus":{"workerGroups":8,"database":"tenant"}}`))
	if err != nil || o.citusOptions() != (CitusOptions{WorkerGroups: 8, Database: "tenant"}) {
		t.Fatalf("explicit options: %+v, %v", o, err)
	}
}

func TestDistributedAdmission(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	p, rc, _ := setup(t, "distributed")
	if rc.Cluster.Spec.Components.Engine.Replicas != 3 {
		t.Fatal("distributed groups must default to three members")
	}
	found := false
	for _, topology := range p.Describe().Topologies {
		if topology.Type == "distributed" {
			found = true
			e := topology.Components["engine"]
			if len(e.AllowedReplicas) != 2 || e.AllowedReplicas[0] != 1 || e.AllowedReplicas[1] != 3 {
				t.Fatal("distributed group replica catalog is incorrect", e)
			}
		}
	}
	if !found {
		t.Fatal("distributed topology missing from catalog")
	}
	for _, tc := range []struct {
		name        string
		mutate      func(*domain.Cluster)
		oldTopology string
		want        string
	}{
		{"replicas", func(c *domain.Cluster) { c.Spec.Components.Engine.Replicas = 5 }, "", "one or three"},
		{"workers", func(c *domain.Cluster) { c.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":9}}`) }, "", "workerGroups"},
		{"proxy-required", func(c *domain.Cluster) {
			proxy := c.Spec.Components.Extra["proxy"]
			proxy.Replicas = 0
			c.Spec.Components.Extra = map[string]domain.ComponentSpec{"proxy": proxy}
		}, "", "require at least one proxy"},
		{"conversion-in", func(*domain.Cluster) {}, "ha", "conversion to distributed"},
		{"conversion-out", func(c *domain.Cluster) { c.Spec.Topology.Type = "ha" }, "distributed", "conversion from distributed"},
		{"major-upgrade", func(c *domain.Cluster) { c.Spec.Version = "18.6.1" }, "distributed", "major upgrades of distributed"},
		{"single-options", func(c *domain.Cluster) {
			c.Spec.Topology.Type = "single"
			c.Spec.Components.Engine.Replicas = 1
			c.Spec.EngineOptions = json.RawMessage(`{"citus":{}}`)
		}, "", "citus options require"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := *rc.Cluster
			c.Spec = rc.Cluster.Spec
			var old *domain.Cluster
			if tc.oldTopology != "" {
				copy := c
				copy.Spec.Topology.Type = tc.oldTopology
				old = &copy
			}
			tc.mutate(&c)
			if err := p.Validate(t.Context(), old, &c); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("want %q, got %v", tc.want, err)
			}
		})
	}
	for _, replicas := range []int{1, 3} {
		c := *rc.Cluster
		c.Spec.Components.Engine.Replicas = replicas
		err := p.Validate(t.Context(), nil, &c)
		if _, artifactErr := nativeartifacts.Get("citus-pg17"); artifactErr != nil {
			if err == nil || !strings.Contains(err.Error(), "locked Citus artifact") {
				t.Fatalf("missing artifact must block admission: %v", err)
			}
		} else if err != nil {
			t.Fatalf("locked distributed topology rejected: %v", err)
		}
	}
}

func TestDistributedReplicaUpdateAdmission(t *testing.T) {
	t.Setenv("KARAKORAM_ARTIFACT_MIRROR", "https://artifacts.example.invalid")
	p, rc, _ := setup(t, "distributed")
	for _, tc := range []struct {
		name       string
		before     int
		after      int
		workers    int
		wantReject bool
	}{
		{"reduce-replicas", 3, 1, 2, true},
		{"increase-replicas", 1, 3, 2, true},
		{"reduce-replicas-and-workers", 3, 1, 1, true},
		{"increase-replicas-and-workers", 1, 3, 3, true},
		{"unchanged-single-member-groups", 1, 1, 2, false},
		{"unchanged-replicated-groups", 3, 3, 2, false},
		{"scale-workers-in", 3, 3, 1, false},
		{"scale-workers-out", 3, 3, 3, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := *rc.Cluster
			old.Spec.Components.Engine.Replicas = tc.before
			old.Spec.EngineOptions = json.RawMessage(`{"citus":{"workerGroups":2}}`)
			next := old
			next.Spec.Components.Engine.Replicas = tc.after
			options, err := json.Marshal(EngineOptions{Citus: &CitusOptions{WorkerGroups: tc.workers, Database: "app"}})
			if err != nil {
				t.Fatal(err)
			}
			next.Spec.EngineOptions = options
			err = p.Validate(t.Context(), &old, &next)
			if tc.wantReject {
				if !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "components.engine.replicas cannot change") || !strings.Contains(err.Error(), "group-local replica scaling is not supported") {
					t.Fatalf("want invalid unsupported replica update, got %v", err)
				}
				return
			}
			if _, artifactErr := nativeartifacts.Get("citus-pg17"); artifactErr != nil {
				if err == nil || !strings.Contains(err.Error(), "locked Citus artifact") {
					t.Fatalf("missing artifact must block admission: %v", err)
				}
			} else if err != nil {
				t.Fatalf("unchanged replica count must allow worker-group scaling: %v", err)
			}
		})
	}
}
