// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package mysql

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type correctionObservationBackend struct {
	*retainedProxyFailureBackend
	unavailable bool
	submissions []string
}

func (b *correctionObservationBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	if name == b.proxy && b.unavailable {
		b.unavailable = false
		return nil, infra.ErrUnavailable
	}
	return b.retainedProxyFailureBackend.GetService(ctx, name)
}
func (b *correctionObservationBackend) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	if name == b.proxy {
		b.submissions = append(b.submissions, "resources")
	}
	return b.retainedProxyFailureBackend.SetResources(ctx, name, resources)
}
func (b *correctionObservationBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	if name == b.proxy {
		b.submissions = append(b.submissions, "count")
	}
	return b.retainedProxyFailureBackend.SetContainerCount(ctx, name, min, max)
}

func TestProxyServingCorrectionDimensionsAndTransientLookup(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		for _, scenario := range []string{"transient", "count-after-resources", "combined-after-resources", "combined-after-count", "legacy-planned"} {
			t.Run(provider+"/"+scenario, func(t *testing.T) {
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
				failed := desiredComponent(rc, m)
				failed.Resources.MinCPU++
				failed.Resources.MaxCPU++
				desired := failed
				stage, action := "resource-failed", "restart"
				if scenario == "transient" {
					desired.Resources = m.Resources
				} else {
					desired.Replicas++
					if scenario != "count-after-resources" {
						desired.Resources = m.Resources
					}
				}
				if scenario == "combined-after-count" {
					stage, action = "scale-failed", "scale"
					failed.Resources = m.Resources
					failed.Replicas++
					desired.Replicas = m.Replicas
					desired.Resources.MinCPU++
					desired.Resources.MaxCPU++
				}
				b := &correctionObservationBackend{retainedProxyFailureBackend: &retainedProxyFailureBackend{proxyScaleBackend: scale, failure: &infra.ProxyRolloutFailureError{Service: m.Name, Action: action, Issued: time.Unix(1, 0), ForceUpdate: 2, Reason: "paused", TargetReplicas: failed.Replicas, TargetResources: failed.Resources}}}
				rc.Backend = b
				// Keep the failed full target blocked through durable public Sync passes.
				rc.Cluster.Spec.Components.Extra["proxy"] = failed
				s.Rolling = &rollingState{From: m.Version, To: m.Version, Order: []string{m.Name}, Stage: stage, Targets: map[string]domain.ComponentSpec{m.Name: failed}}
				replayAdmissionState(t, rc, s)
				for range 2 {
					_, _ = p.Sync(t.Context(), rc)
					base.apply(t, p, rc)
				}
				if len(b.submissions) != 0 || b.stops[b.proxy] != 0 {
					t.Fatal("identical failed target replayed")
				}
				// Use the real store update rather than manually advancing generation.
				st := rc.Store.(*store.Store)
				if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
					t.Fatal(err)
				}
				if err := st.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
					t.Fatal(err)
				}
				spec := rc.Cluster.Spec
				spec.Components.Extra = map[string]domain.ComponentSpec{}
				for name, component := range rc.Cluster.Spec.Components.Extra {
					spec.Components.Extra[name] = component
				}
				spec.Components.Extra["proxy"] = desired
				if err := st.UpdateClusterSpec(t.Context(), rc.Cluster.Ref(), spec); err != nil {
					t.Fatal(err)
				}
				current, err := st.GetCluster(t.Context(), rc.Cluster.Ref())
				if err != nil {
					t.Fatal(err)
				}
				rc.Cluster = current
				beforeGeneration := current.Status.ObservedGeneration
				s, _ = load(rc.Cluster)
				if scenario == "legacy-planned" {
					s.Rolling.Targets[m.Name] = desired
					s.Rolling.Stage = "resource-recovery-scale"
					s = replayAdmissionState(t, rc, s)
				}
				if scenario == "transient" {
					b.unavailable = true
					// Isolate the late observation after earlier Sync lookups may succeed.
					_, err := p.reconcileRolling(t.Context(), rc, s)
					if !errors.Is(err, infra.ErrUnavailable) || s.Rolling.Stage != stage || s.find(m.Name).Offline || len(b.submissions) != 0 || b.stops[b.proxy] != 0 || b.deploys != 0 {
						t.Fatalf("transient observation changed checkpoint or authorized mutation: stage=%s err=%v stops=%d", s.Rolling.Stage, err, b.stops[b.proxy])
					}
					replayAdmissionState(t, rc, s)
				}
				stale[0] = true
				for range 3 {
					p = New(Options{Provider: provider})
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					base.apply(t, p, rc)
				}
				if len(b.submissions) != 0 || rc.Cluster.Status.ObservedGeneration != beforeGeneration {
					t.Fatal("correction bypassed latest incumbent acknowledgement")
				}
				stale[0] = false
				countBarrierChecked := false
				for range 40 {
					p = New(Options{Provider: provider})
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					base.apply(t, p, rc)
					s, _ = load(rc.Cluster)
					if s.Rolling != nil && s.Rolling.Stage == "scale-recovery" && b.resources == 1 && b.scales == 0 {
						stale[0] = true
						for range 2 {
							if _, err := p.Sync(t.Context(), rc); err != nil {
								t.Fatal(err)
							}
							base.apply(t, p, rc)
						}
						if b.scales != 0 || rc.Cluster.Status.ObservedGeneration != beforeGeneration {
							t.Fatal("count correction bypassed its newly published configuration")
						}
						stale[0] = false
						countBarrierChecked = true
					}
					if s.Rolling == nil && rc.Cluster.Status.ObservedGeneration == rc.Cluster.Generation {
						break
					}
				}
				if scenario != "transient" && !countBarrierChecked {
					t.Fatal("compound correction did not pass a separate count acknowledgement barrier")
				}
				s, _ = load(rc.Cluster)
				if s.Rolling != nil || rc.Cluster.Status.ObservedGeneration != rc.Cluster.Generation {
					t.Fatalf("different target did not converge: rolling=%+v wait=%s", s.Rolling, rc.SyncWait)
				}
				want := 2
				if scenario == "transient" {
					want = 1
				}
				if len(b.submissions) != want || b.submissions[0] != "resources" || want == 2 && b.submissions[1] != "count" || b.stops[b.proxy] != 0 || b.deploys != 0 {
					t.Fatalf("unsafe or unbounded correction sequence: %v stops=%d deploys=%d", b.submissions, b.stops[b.proxy], b.deploys)
				}
			})
		}
	}
}
