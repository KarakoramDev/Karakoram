// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package psmdb

import (
	"context"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type uncertainScaleBackend struct {
	*unplacedScaleBackend
	proof bool
	err   error
}

func (b *uncertainScaleBackend) RuntimeNeverPlaced(context.Context, string) (bool, error) {
	return b.proof, b.err
}

func TestScaleBackPreservesInitializedAndUncertainMembers(t *testing.T) {
	for _, mode := range []string{"initialized", "deployed-history", "unresolved-import", "unknown-history", "observation-error"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, f, _ := setup(t)
			s, _ := load(rc.Cluster)
			s.add(rc.Cluster.Name, "rs0", agentapi.RoleReplicaSet, -1).Stage = "active"
			m := s.add(rc.Cluster.Name, "rs0", agentapi.RoleReplicaSet, -1)
			m.Stage = "imported"
			pending := &pending{Member: m.Name, Action: "deploy", Op: infra.Op{ID: "blocked", Kind: "deploy"}}
			s.Pending = pending
			b := &uncertainScaleBackend{unplacedScaleBackend: &unplacedScaleBackend{&supersedingCleanupBackend{fakeInfra: f}}, proof: true}
			rc.Backend = b
			switch mode {
			case "initialized":
				m.RuntimeRecovery.Initialized = true
			case "deployed-history":
				m.Version = rc.Cluster.Spec.Version
			case "unresolved-import":
				m.ImportPlan = &engine.ImportPlan{}
			case "unknown-history":
				b.proof = false
			case "observation-error":
				b.err = infra.ErrUnavailable
			}
			busy, err := p.retireUnplacedScaleDown(t.Context(), rc, s)
			if mode == "observation-error" {
				if err == nil || !busy {
					t.Fatal("backend uncertainty was hidden")
				}
			} else if busy || err != nil {
				t.Fatalf("unsafe retirement admitted: busy=%v err=%v", busy, err)
			}
			if m.UnplacedRetirement != nil || s.Pending != pending || f.mutations != 0 {
				t.Fatal("uncertain/native/importing member was discarded")
			}
		})
	}
}

func TestUnplacedRetirementFencesOperationsAndCleanupTakesOver(t *testing.T) {
	p, rc, f, _ := setup(t)
	s, _ := load(rc.Cluster)
	s.UID = "existing"
	s.add(rc.Cluster.Name, "rs0", agentapi.RoleReplicaSet, -1).Stage = "active"
	m := s.add(rc.Cluster.Name, "rs0", agentapi.RoleReplicaSet, -1)
	m.Stage = "imported"
	m.ServiceID = m.Name
	m.VolumeID = m.Volume
	f.services[m.Name] = &infra.Service{ID: m.Name, Hostname: m.Name, Kind: infra.KindRuntime}
	f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume, Kind: infra.KindVolume}
	s.Pending = &pending{Member: m.Name, Action: "deploy", Op: infra.Op{ID: "blocked", Kind: "deploy"}}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Backend = &unplacedScaleBackend{&supersedingCleanupBackend{fakeInfra: f}}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
		t.Fatal("accepted retirement did not own operation admission", err)
	}
	for range 30 {
		done, err := p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true, KeepBackups: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			current, _ := load(rc.Cluster)
			for _, member := range current.Members {
				if member.UnplacedRetirement != nil {
					t.Fatal("queued volume removal survived suspension")
				}
			}
			if f.services[m.Name] != nil || f.services[m.Volume] == nil {
				t.Fatal("keep-volumes takeover did not preserve unissued volume deletion")
			}
			return
		}
	}
	t.Fatal("cleanup takeover did not finish")
}
