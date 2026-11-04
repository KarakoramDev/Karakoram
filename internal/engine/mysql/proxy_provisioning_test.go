// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestProxyDeploymentWaitsForJoinedBackends(t *testing.T) {
	for _, topology := range []string{"single", "group", "cluster"} {
		t.Run(topology, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var b *fakeBackend
			if topology == "cluster" {
				p, rc, b = setupPXC(t)
			} else {
				p, rc, b = setup(t, topology)
			}
			proxy := rc.Cluster.Spec.Components.Extra["proxy"]
			proxy.Replicas = 1
			rc.Cluster.Spec.Components.Extra["proxy"] = proxy
			for range 120 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				for _, member := range s.Members {
					if member.Role != "proxy" || !b.deployed[member.Name] {
						continue
					}
					cfg, err := p.MemberConfig(t.Context(), rc, member.Name)
					if err != nil {
						t.Fatal(err)
					}
					var spec myspec.Spec
					if err = json.Unmarshal(cfg.EngineSpec, &spec); err != nil {
						t.Fatal(err)
					}
					if err = spec.Validate(); err != nil {
						t.Fatalf("deployed proxy cannot accept its initial configuration: %v", err)
					}
				}
				b.apply(t, p, rc)
				status, err := p.Status(t.Context(), rc)
				if err != nil {
					t.Fatal(err)
				}
				if status.Phase == domain.PhaseReady && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
					return
				}
			}
			t.Fatal("proxy provisioning did not converge")
		})
	}
}

type pendingProxyBackend struct {
	*fakeBackend
	ready func() bool
}

func (b pendingProxyBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "pending-proxy" && !b.ready() {
		return infra.OpState{}, nil
	}
	return b.fakeBackend.OpStatus(ctx, op)
}

func TestPendingProxyDeploymentDoesNotBlockDataJoin(t *testing.T) {
	p, rc, b := setup(t, "single")
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 1
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range s.Members {
		if member.Role == "data" {
			member.Stage = "deployed"
		} else {
			member.Stage = "deploying"
			member.DeployOp = &infra.Op{ID: "pending-proxy"}
		}
	}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Backend = pendingProxyBackend{fakeBackend: b, ready: func() bool {
		current, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		for _, member := range current.data() {
			if member.Stage == "joined" {
				return true
			}
		}
		return false
	}}
	converge(t, p, rc, b)
}
