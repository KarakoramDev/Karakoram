// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"testing"
)

func TestRecoveryFencesCleanupAndImperativeOperations(t *testing.T) {
	for _, keep := range []bool{false, true} {
		t.Run(map[bool]string{false: "delete", true: "suspend"}[keep], func(t *testing.T) {
			p, rc, _, _ := renewalCluster(t, 1)
			backend := rc.Backend.(*fakeInfra)
			s, _ := load(rc.Cluster)
			s.Members[0].RuntimeRecovery.Op = &infra.Op{ID: "accepted-start"}
			s.Members[0].RuntimeRecovery.Action = "start"
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			p = New(p.options)
			backend.done = false
			before := backend.mutations
			if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
				t.Fatal("recovery did not fence claim", allowed, err)
			}
			op := &domain.Operation{Kind: domain.OperationRestart}
			out, err := p.RunOperation(t.Context(), rc, op)
			if err != nil || out.Done || backend.mutations != before {
				t.Fatal("operation raced recovery", out, err)
			}
			rc.Cluster.Deleting = true
			done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep, KeepBackups: true})
			if err != nil || done || backend.mutations != before {
				t.Fatal("cleanup raced recovery", done, err)
			}
			got, _ := load(rc.Cluster)
			if got.Members[0].RuntimeRecovery.Op == nil || got.Suspended {
				t.Fatal("accepted journal lost or suspension finalized")
			}
			backend.done = true
			done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep, KeepBackups: true})
			if err != nil || done || backend.mutations != before {
				t.Fatal("terminal observation combined lifecycle mutation", done, err)
			}
			got, _ = load(rc.Cluster)
			if got.Members[0].RuntimeRecovery.Op != nil {
				t.Fatal("terminal start not released")
			}
		})
	}
}

func TestLegacyActiveStateMigratesBeforePhysicalLossVerification(t *testing.T) {
	p, rc := healingCluster(t, 3)
	s, _ := load(rc.Cluster)
	s.TLSMode = "disabled"
	nativeHealingFixture(t, rc, s)
	for _, m := range s.Members {
		m.RuntimeRecovery = engine.RuntimeRecovery{}
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	// Remove every new journal key to reproduce the pre-recovery serialized shape.
	var old map[string]any
	if err := json.Unmarshal(rc.Cluster.Internal, &old); err != nil {
		t.Fatal(err)
	}
	for _, raw := range old["members"].([]any) {
		delete(raw.(map[string]any), "runtimeRecovery")
	}
	rc.Cluster.Internal, _ = json.Marshal(old)
	lost := s.Members[0]
	backend := rc.Backend.(*fakeInfra)
	rc.Backend = &confirmedLossBackend{fakeInfra: backend, volume: lost.Volume, service: backend.services[lost.Volume], err: &infra.VolumeLostError{Volume: lost.Volume, ServiceID: lost.VolumeID}}
	before := len(backend.imports)
	for range 2 {
		p = New(p.options)
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		current, _ := load(rc.Cluster)
		if !current.find(lost.Name).RuntimeRecovery.Initialized || !current.find(lost.Name).RuntimeRecovery.VolumeLost {
			t.Fatal("legacy loss migration did not persist", string(rc.Cluster.Internal))
		}
	}
	operations, err := rc.Store.(*store.Store).ListOperations(t.Context(), rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	count := 0
	for _, op := range operations {
		if op.Kind == domain.OperationReplaceMember {
			count++
		}
	}
	if count != 1 || len(backend.imports) != before {
		t.Fatal("replacement admission duplicated or lost volume recreated", count)
	}
}
