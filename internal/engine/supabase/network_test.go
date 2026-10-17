// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase/sbspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type forwarderFamilyBackend struct {
	infra.Backend
	family string
}

func (b forwarderFamilyBackend) Layout() infra.Layout {
	layout := b.Backend.Layout()
	layout.PrivateAddressFamily = b.family
	return layout
}

func TestForwarderAddressFamilyMemberConfig(t *testing.T) {
	for _, family := range []string{"", infra.AddressFamilyIPv4, infra.AddressFamilyIPv6} {
		t.Run("family="+family, func(t *testing.T) {
			p, rc, _ := setupProvider(t)
			rc.Backend = forwarderFamilyBackend{Backend: rc.Backend, family: family}
			convergeProvider(t, p, rc)
			config, err := p.MemberConfig(context.Background(), rc, "demoau")
			if err != nil {
				t.Fatal(err)
			}
			var spec sbspec.Spec
			if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
				t.Fatal(err)
			}
			if spec.DatabaseIPv6 != (family != infra.AddressFamilyIPv4) {
				t.Fatal("component forwarding family disagrees with the backend's database HBA family")
			}
		})
	}
}

func TestRealtimeReplicaAdmission(t *testing.T) {
	for _, replicas := range []int{-1, 0, 1, 2, 5} {
		p, rc, _ := setupProvider(t)
		component := rc.Cluster.Spec.Components.Extra["realtime"]
		component.Replicas = replicas
		rc.Cluster.Spec.Components.Extra["realtime"] = component
		for _, old := range []*domain.Cluster{nil, rc.Cluster} {
			err := p.Validate(context.Background(), old, rc.Cluster)
			if replicas < 0 || replicas > 1 {
				if err == nil || !strings.Contains(err.Error(), "realtime replicas must be 0..1") {
					t.Fatalf("replicas=%d create=%v: expected singleton bound, got %v", replicas, old == nil, err)
				}
			} else if err != nil {
				t.Fatalf("replicas=%d: %v", replicas, err)
			}
		}
		for _, topology := range p.Describe().Topologies {
			if component := topology.Components["realtime"]; component.MaxReplicas != 1 {
				t.Fatal("provider catalog advertises unsupported multi-node Realtime")
			}
		}
		for _, preset := range p.Describe().Presets {
			if preset.Components.Extra["realtime"].Replicas > 1 {
				t.Fatalf("%s preset enables unsupported multi-node Realtime", preset.Name)
			}
		}
	}
}

func TestRealtimeLegacyMultiReplicaReconciliation(t *testing.T) {
	p, rc, _ := setupProvider(t)
	component := rc.Cluster.Spec.Components.Extra["realtime"]
	component.Replicas = 2
	rc.Cluster.Spec.Components.Extra["realtime"] = component
	// Simulate a stored spec accepted before the singleton admission limit.
	// Reconciliation must not start calling new admission rules on old intent.
	convergeProvider(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if state.Members["realtime"] == nil || state.Members["realtime"].Replicas != 2 {
		t.Fatal("legacy reconciliation silently changed stored replica intent")
	}
	old := *rc.Cluster
	old.Spec.Components.Extra = maps.Clone(rc.Cluster.Spec.Components.Extra)
	component.Replicas = 1
	rc.Cluster.Spec.Components.Extra["realtime"] = component
	rc.Cluster.Generation++
	if err := p.Validate(context.Background(), &old, rc.Cluster); err != nil {
		t.Fatalf("legacy spec cannot be corrected to a singleton: %v", err)
	}
	convergeProvider(t, p, rc)
	state, err = loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if state.Members["realtime"] == nil || state.Members["realtime"].Replicas != 1 {
		t.Fatal("explicit legacy-to-singleton update did not reconcile")
	}
}
