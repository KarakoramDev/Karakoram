// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestResourcePlacementRejectionAllowsRevert(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom1")
	original := rc.Cluster.Spec.Components.Engine.Resources
	applied := m.Resources
	oversized := applied
	oversized.MaxCPU = 8
	rc.Cluster.Spec.Components.Engine.Resources.MaxCPU = 8
	m.Change = &memberChange{Kind: "resources", Step: "runtime-resources", Version: rc.Cluster.Spec.Version, Resources: oversized, Disk: m.Disk}
	cause := &infra.ResourceUpdateRejectedError{Message: "node node-a: dimension cpu exhausted; running allocation unchanged"}
	f.failures["resources:"+m.Name] = cause
	err := p.changeStep(t.Context(), rc, s, m, true)
	var rejected *infra.ResourceUpdateRejectedError
	if !errors.As(err, &rejected) || rejected != cause {
		t.Fatalf("provider erased definitive placement type: %v", err)
	}
	if s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 || m.Change != nil || !reflect.DeepEqual(m.Resources, applied) {
		t.Fatalf("rejection retained obsolete target or applied resources: %+v member=%+v", s, m)
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	// A controller restart followed by corrective intent must not replay the
	// rejected target first, even while that same backend rejection persists.
	rc.Cluster.Spec.Components.Engine.Resources = original
	rc.Cluster.Generation++
	mutations := f.mutations
	ready(t, p, rc)
	reloaded := state(t, rc)
	if reloaded.Retry != nil || reloaded.Pending != nil || reloaded.find(m.Name).Change != nil || !reflect.DeepEqual(reloaded.find(m.Name).Resources, applied) {
		t.Fatalf("revert stayed behind rejected resources: %+v", reloaded)
	}
	if f.mutations != mutations {
		t.Fatal("revert retried an unapplied rejected target")
	}
}

type resourceRejectedReceiptBackend struct {
	infra.Backend
	cause *infra.ResourceUpdateRejectedError
}

func (b *resourceRejectedReceiptBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return infra.OpState{Failed: true, Message: b.cause.Error()}, b.cause
}

func TestResourcePlacementRejectionDuringReceiptReleasesChange(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	s := state(t, rc)
	m := s.find("demom1")
	m.Change = &memberChange{Kind: "resources", Step: "runtime-resources", Resources: m.Resources}
	s.Pending = &pending{Action: "resources", Member: m.Name, Resources: m.Resources, Op: infra.Op{ID: "accepted-stop"}}
	cause := &infra.ResourceUpdateRejectedError{Message: "node node-a: dimension cpu exhausted"}
	rc.Backend = &resourceRejectedReceiptBackend{Backend: rc.Backend, cause: cause}
	if _, err := p.observePending(t.Context(), rc, s); !errors.Is(err, cause) || s.Pending != nil || s.Retry != nil || m.Change != nil {
		t.Fatalf("terminal receipt retained rejected change: %v %+v", err, s)
	}
}

type resizeBackend struct {
	infra.Backend
	name string
}

func (b resizeBackend) Name() string { return b.name }

func TestRuntimeOnlyResizeWithoutDiskGrowth(t *testing.T) {
	for _, backend := range []string{"nomad", "dokploy"} {
		for _, dimension := range []string{"cpu", "ram"} {
			t.Run(backend+"/"+dimension, func(t *testing.T) {
				p, rc, f, _ := setup(t)
				ready(t, p, rc)
				caps := f.Capabilities()
				caps.DiskGrowth = false
				f.capabilities = &caps
				rc.Backend = resizeBackend{Backend: f, name: backend}
				target := &rc.Cluster.Spec.Components.Engine.Resources
				if dimension == "cpu" {
					target.MaxCPU++
				} else {
					target.MaxRAM += 0.5
				}
				rc.Cluster.Generation++
				f.log.clear()
				ready(t, p, rc)
				for _, m := range state(t, rc).Members {
					if m.Change != nil || m.Resources != runtimeResources(*target) || m.ProcessGeneration != 2 {
						t.Fatalf("runtime resize did not converge once: %+v", m)
					}
					volumeCall := "infra:" + m.Volume + ":resources"
					for _, call := range f.log.snapshot() {
						if call == volumeCall {
							t.Fatal("runtime-only resize invoked volume growth")
						}
					}
				}
				// Disk caches still have the original CPU/RAM. Another sync must
				// not start a second resource roll because of those legacy fields.
				mutations := f.mutations
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if f.mutations != mutations {
					t.Fatal("converged runtime resize repeated platform work")
				}
				for _, m := range state(t, rc).Members {
					if m.Change != nil {
						t.Fatal("legacy CPU/RAM in disk checkpoint triggered another roll")
					}
				}
			})
		}
	}
}

func TestRuntimeResizeResumesLegacyDiskCheckpoint(t *testing.T) {
	p, rc, f, _ := setup(t)
	ready(t, p, rc)
	caps := f.Capabilities()
	caps.DiskGrowth = false
	f.capabilities = &caps
	s := state(t, rc)
	m := s.find("demom1")
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM += 0.5
	rc.Cluster.Generation++
	target := rc.Cluster.Spec.Components.Engine.Resources
	fp, err := p.restartFingerprint(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce the durable boundary left by the failed pre-fix operation:
	// runtime resize acknowledged, disk step not submitted.
	m.Resources = runtimeResources(target)
	f.services[m.Name].Resources = m.Resources
	m.Change = &memberChange{Kind: "resources", Step: "disk", Started: true, Version: rc.Cluster.Spec.Version, Fingerprint: fp, Resources: m.Resources, Disk: target}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	f.log.clear()
	p = New(p.options)
	ready(t, p, rc)
	for _, call := range f.log.snapshot() {
		if strings.Contains(call, "v:resources") {
			t.Fatalf("resuming legacy disk step invoked volume growth: %s", call)
		}
	}
}

func TestUnsupportedDiskGrowthPrecedesDisruption(t *testing.T) {
	for _, backend := range []string{"nomad", "dokploy"} {
		t.Run(backend, func(t *testing.T) {
			p, rc, f, a := setup(t)
			ready(t, p, rc)
			caps := f.Capabilities()
			caps.DiskGrowth = false
			f.capabilities = &caps
			rc.Backend = resizeBackend{Backend: f, name: backend}
			before := state(t, rc)
			rc.Cluster.Spec.Components.Engine.Resources.MaxDisk++
			rc.Cluster.Generation++
			mutations := f.mutations
			f.log.clear()
			if _, err := p.Sync(t.Context(), rc); !errors.Is(err, engine.ErrNotSupported) {
				t.Fatalf("unsupported growth accepted: %v", err)
			}
			if f.mutations != mutations || len(f.log.snapshot()) != 0 {
				t.Fatal("growth rejection happened after disruption")
			}
			after := state(t, rc)
			for _, m := range after.Members {
				if m.Change != nil || m.ProcessGeneration != before.find(m.Name).ProcessGeneration || a.member(m.Name).sealed {
					t.Fatal("growth rejection changed serving member")
				}
			}
			// Admission uses the same backend volume identity and sizes.
			target := rc.Cluster.Spec
			if _, err := engine.VolumeAdmissionBaseline(t.Context(), p, rc.Cluster, target, rc.Backend); !errors.Is(err, engine.ErrNotSupported) {
				t.Fatalf("admission accepted actual growth: %v", err)
			}
		})
	}
}
