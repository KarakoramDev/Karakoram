// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type retainedProxyFailureBackend struct {
	*proxyScaleBackend
	failure *infra.ProxyRolloutFailureError
}

func (b *retainedProxyFailureBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	service, err := b.Backend.GetService(ctx, name)
	if err != nil || name != b.proxy || b.failure == nil || service == nil {
		return service, err
	}
	observed := *service
	observed.Status, observed.MaxContainers, observed.Resources = infra.StatusFailed, b.failure.TargetReplicas, b.failure.TargetResources
	return &observed, b.failure
}
func (b *retainedProxyFailureBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	op, err := b.proxyScaleBackend.SetContainerCount(ctx, name, min, max)
	if err == nil && name == b.proxy {
		b.failure = nil
	}
	return op, err
}
func (b *retainedProxyFailureBackend) SetResources(ctx context.Context, name string, target infra.Resources) (infra.Op, error) {
	failure := b.failure
	op, err := b.proxyScaleBackend.SetResources(ctx, name, target)
	if err == nil && name == b.proxy {
		if failure != nil {
			// Dokploy resources reload keeps the failed specification's count.
			b.count = failure.TargetReplicas
		}
		b.failure = nil
	}
	return op, err
}

func TestOrdinaryProxyTypedFailureChangedTargetRecovery(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		for _, action := range []string{"count", "resources"} {
			t.Run(provider+"/"+action, func(t *testing.T) {
				var p *Core
				var rc *engine.Context
				var base *fakeBackend
				if provider == "pxc" {
					p, rc, base = setupPXC(t)
				} else {
					p, rc, base = setup(t, "group")
				}
				converge(t, p, rc, base)
				s, _ := load(rc.Cluster)
				scale, stale := proxyScaleFixture(t, rc, s)
				scale.servingResources = true
				m := s.find(scale.proxy)
				good := desiredComponent(rc, m)
				failed := good
				stage := "scale-failed"
				if action == "count" {
					failed.Replicas++
				} else {
					failed.Resources.MinCPU++
					failed.Resources.MaxCPU++
					stage = "resource-failed"
				}
				b := &retainedProxyFailureBackend{proxyScaleBackend: scale, failure: &infra.ProxyRolloutFailureError{Service: m.Name, Action: "scale", Issued: time.Unix(1, 0), ForceUpdate: 2, Reason: "paused", TargetReplicas: failed.Replicas, TargetResources: failed.Resources}}
				rc.Backend = b
				s.Rolling = &rollingState{From: m.Version, To: m.Version, Order: []string{m.Name}, Stage: stage, Targets: map[string]domain.ComponentSpec{m.Name: failed}}
				s = replayAdmissionState(t, rc, s)
				// A different target is the existing desired good spec. No generation
				// fence is installed or advanced by this component-level checkpoint test.
				if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if s.Rolling.Stage != "scale-recovery" && s.Rolling.Stage != "resource-recovery-scale" {
					t.Fatalf("correction lost serving path: %+v", s.Rolling)
				}
				s = replayAdmissionState(t, rc, s)
				base.apply(t, p, rc)
				stale[0] = true
				for range 2 {
					if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil {
						t.Fatal(err)
					}
				}
				if b.scales != 0 || b.resources != 0 {
					t.Fatal("recovery bypassed current incumbent configuration acknowledgement")
				}
				stale[0] = false
				for range 12 {
					if _, err := p.reconcileRolling(t.Context(), rc, s); err != nil {
						t.Fatal(err)
					}
					s = replayAdmissionState(t, rc, s)
					base.apply(t, p, rc)
					if s.Rolling == nil {
						break
					}
				}
				if s.Rolling != nil || b.failure != nil {
					t.Fatalf("corrected target failed to settle: %+v", s.Rolling)
				}
				if b.stops[b.proxy] != 0 || b.deploys != 0 {
					t.Fatal("corrective serving update stopped or redeployed whole proxy")
				}
				if action == "count" && b.scales != 1 || action == "resources" && b.resources != 1 {
					t.Fatalf("replacement submissions: scales=%d resources=%d", b.scales, b.resources)
				}
			})
		}
	}
}
