// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

func TestShardedVolumeSpreadGroups(t *testing.T) {
	p, rc, backend, _ := setup(t)
	rc.Cluster.Spec.Topology = domain.Topology{Type: "sharded", Shards: 2}
	p.ApplyDefaults(&rc.Cluster.Spec)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	groups := map[string]string{}
	for _, m := range ordered(s) {
		if m.Volume == "" {
			continue
		}
		before := len(backend.imports)
		if err := p.importMember(t.Context(), rc, s, m); err != nil {
			t.Fatal(err)
		}
		found := false
		for _, spec := range backend.imports[before] {
			if spec.Kind == infra.KindVolume {
				found = true
				want := rc.Cluster.Name + "/" + m.Set
				if spec.SpreadGroup != want {
					t.Fatalf("%s spread group = %q, want %q", m.Name, spec.SpreadGroup, want)
				}
				groups[m.Set] = spec.SpreadGroup
			}
		}
		if !found {
			t.Fatalf("no volume imported for %s", m.Name)
		}
	}
	if len(groups) != 3 {
		t.Fatalf("expected separate config-server and two shard groups, got %v", groups)
	}
}

func TestBackendPlacementOverridesAgentFingerprint(t *testing.T) {
	for _, tc := range []struct {
		name          string
		authoritative bool
		group         string
		want          string
		warning       string
	}{
		{"backend", true, "node-a", "node-a", "are co-located"},
		{"backend unknown", true, "", "", "backend placement is unknown"},
		{"agent fallback", false, "node-a", "host-a", "best-effort"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, _, _ := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = 3
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			plan(rc.Cluster, s)
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			backend := fake.New(fake.Options{Capabilities: infra.Capabilities{Placement: tc.authoritative, VolumeQuota: true}})
			rc.Backend = backend
			for _, m := range ordered(s) {
				if _, err := backend.ImportServices(t.Context(), []infra.ServiceSpec{{Hostname: m.Name, Kind: infra.KindRuntime, Base: infra.BaseUbuntu2404}}); err != nil {
					t.Fatal(err)
				}
				if _, err := backend.Start(t.Context(), m.Name); err != nil {
					t.Fatal(err)
				}
				if err := backend.SetPlacementGroup(m.Name, tc.group); err != nil {
					t.Fatal(err)
				}
			}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			for _, member := range status.Members {
				if member.PlacementGroup != tc.want {
					t.Fatalf("member %s placement = %q, want %q", member.Name, member.PlacementGroup, tc.want)
				}
			}
			group := tc.want
			if group == "" {
				group = "unknown"
			}
			if len(status.Placement.Groups[group]) != 3 || !strings.Contains(status.Placement.Warning, tc.warning) {
				t.Fatalf("placement = %+v", status.Placement)
			}
			if tc.authoritative && strings.Contains(status.Placement.Warning, "fingerprint") {
				t.Fatalf("authoritative placement described as fingerprints: %+v", status.Placement)
			}
			for _, condition := range status.Conditions {
				if condition.Type == "Placement" && tc.authoritative && condition.Reason == "BestEffortOnly" {
					t.Fatalf("backend placement is not best effort: %+v", condition)
				}
			}
		})
	}
}

func TestReplacementVolumePlacementHint(t *testing.T) {
	for _, tc := range []struct {
		name, topology, set string
	}{
		{"replica set", "replicaSet", "rs0"},
		{"shard", "sharded", "rs1"},
		{"config servers", "sharded", "cfg"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, backend, _ := setup(t)
			rc.Cluster.Spec.Topology = domain.Topology{Type: tc.topology, Shards: 2}
			p.ApplyDefaults(&rc.Cluster.Spec)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			plan(rc.Cluster, s)
			old := s.setMembers(tc.set)[0]
			old.Stage = "active"
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			payload, err := json.Marshal(map[string]string{"member": old.Name})
			if err != nil {
				t.Fatal(err)
			}
			result, err := p.RunOperation(t.Context(), rc, &domain.Operation{Kind: domain.OperationReplaceMember, Payload: payload})
			if err != nil || result.Done {
				t.Fatalf("plan replacement: %+v %v", result, err)
			}
			var checkpoint operationState
			if err := json.Unmarshal(result.Checkpoint, &checkpoint); err != nil {
				t.Fatal(err)
			}
			// Import after reloading durable state rather than using the planner's pointer.
			s, err = load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			replacement := s.find(checkpoint.NewMember)
			if replacement == nil || replacement.ReplacesVolume != old.Volume {
				t.Fatalf("replacement placement hint was not persisted: %+v", replacement)
			}
			if err := p.ensureSecrets(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if err := p.importMember(t.Context(), rc, s, replacement); err != nil {
				t.Fatal(err)
			}
			found := false
			for _, spec := range backend.imports[0] {
				if spec.Kind == infra.KindVolume {
					found = true
					wantGroup := rc.Cluster.Name
					if tc.topology == "sharded" {
						wantGroup += "/" + tc.set
					}
					if spec.SpreadReplaces != old.Volume || spec.SpreadGroup != wantGroup {
						t.Fatalf("replacement spread group = %q, replaces = %q; want %q and %q", spec.SpreadGroup, spec.SpreadReplaces, wantGroup, old.Volume)
					}
				} else if spec.SpreadReplaces != "" {
					t.Fatal("replacement placement hint belongs only on volumes")
				}
			}
			if !found {
				t.Fatal("replacement volume was not imported")
			}
		})
	}
}
