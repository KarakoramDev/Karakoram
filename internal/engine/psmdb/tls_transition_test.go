// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/mongoadmin"
)

func TestTLSModeTransitionsPreserveMemberConnectivity(t *testing.T) {
	for _, test := range []struct {
		from, to string
		passes   []string
	}{
		{"requireTLS", "preferTLS", []string{"preferTLS"}},
		{"preferTLS", "disabled", []string{"allowTLS", "disabled"}},
		{"disabled", "requireTLS", []string{"allowTLS", "preferTLS", "requireTLS"}},
		{"disabled", "preferTLS", []string{"allowTLS", "preferTLS"}},
		{"requireTLS", "disabled", []string{"preferTLS", "allowTLS", "disabled"}},
		{"preferTLS", "requireTLS", []string{"requireTLS"}},
	} {
		t.Run(test.from+"To"+test.to, func(t *testing.T) {
			p, rc, agents, _ := renewalCluster(t, 3)
			ctx := context.Background()
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.TLSMode = test.from
			s.ConfigHash = ""
			for _, m := range s.Members {
				activateConfig(rc.Cluster, s, m)
				agents[m.Name].version.Store(m.ConfigVersion)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.TLS.Mode = test.to
			var passes []string
			publications := 0
			prepare := func(_ context.Context, _ *engine.Context, state *internalState, candidate *member) (bool, error) {
				// Each full-cluster pass must finish before changing the transport
				// used for the next one, including across durable reload checkpoints.
				if len(passes) == 0 || passes[len(passes)-1] != state.TLSMode {
					if publications%len(state.Members) != 0 {
						t.Fatal("advanced TLS mode before every member completed the preceding pass")
					}
					passes = append(passes, state.TLSMode)
				}
				for _, peer := range state.Members {
					if peer.Name == candidate.Name {
						continue
					}
					for _, pair := range [][2]string{{state.TLSMode, peer.TLSMode}, {peer.TLSMode, state.TLSMode}} {
						sendsTLS := pair[0] == "preferTLS" || pair[0] == "requireTLS"
						if sendsTLS && pair[1] == "disabled" || !sendsTLS && pair[1] == "requireTLS" {
							t.Fatalf("publishing %s=%s breaks replication with %s=%s", candidate.Name, state.TLSMode, peer.Name, peer.TLSMode)
						}
					}
				}
				if candidate.Stage == "reload-publish" {
					publications++
				}
				return false, nil
			}
			replication := func(_ context.Context, _ *engine.Context, _ *internalState, m *member) (mongoadmin.Status, error) {
				return mongoadmin.Status{Members: []mongoadmin.Member{{Name: m.Name + ":27017", State: "SECONDARY", Health: 1, OptimeDate: time.Now()}}}, nil
			}
			for range 100 {
				if _, err := p.sync(ctx, rc, prepare, replication); err != nil {
					t.Fatal(err)
				}
				current, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				for _, m := range current.Members {
					config, err := p.MemberConfig(ctx, rc, m.Name)
					if err != nil {
						t.Fatal(err)
					}
					agents[m.Name].version.Store(config.ConfigVersion)
				}
				if current.Phase == "ready" {
					if current.TLSMode != test.to || current.TargetTLS != "" || !reflect.DeepEqual(passes, test.passes) {
						t.Fatalf("mode=%s target=%s passes=%v, want %v", current.TLSMode, current.TargetTLS, passes, test.passes)
					}
					for _, m := range current.Members {
						if m.TLSMode != test.to || m.Stage != "active" {
							t.Fatalf("member %s mode=%s stage=%s", m.Name, m.TLSMode, m.Stage)
						}
					}
					return
				}
			}
			t.Fatal("TLS transition did not converge")
		})
	}
}
