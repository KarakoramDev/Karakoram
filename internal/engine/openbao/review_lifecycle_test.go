// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestLostVolumeReplacementReconcilesBeforeRetirement(t *testing.T) {
	p, rc, f, a := setup(t)
	ready(t, p, rc)
	lost := state(t, rc).find("demom1")
	delete(f.services, lost.Volume)
	a.member(lost.Name).unreachable = true
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	st := rc.Store.(*store.Store)
	ops, err := st.ListOperations(t.Context(), rc.Cluster.Ref())
	if err != nil || len(ops) != 1 {
		t.Fatalf("recovery operations: %v %v", ops, err)
	}
	op := ops[0]
	op.Phase = domain.PhaseRunning
	if err = st.UpdateOperation(t.Context(), &op); err != nil {
		t.Fatal(err)
	}
	for range 250 {
		result, err := p.RunOperation(t.Context(), rc, &op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		if err = st.UpdateOperation(t.Context(), &op); err != nil {
			t.Fatal(err)
		}
		if result.Done {
			calls := f.log.snapshot()
			joined, removed := slices.Index(calls, "unsealed:demom3"), slices.Index(calls, "remove-peer:demom1")
			if joined < 0 || removed <= joined || state(t, rc).find("demom1") != nil {
				t.Fatalf("unsafe replacement: %v", calls)
			}
			return
		}
	}
	t.Fatalf("replacement stuck: %s %s", op.Checkpoint, rc.Cluster.Internal)
}

func TestCloneWaitsForInitialHealthyVoters(t *testing.T) {
	for _, n := range []int{3, 5} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			p, rc, _, a := setup(t)
			rc.Cluster.Spec.Components.Engine.Replicas = n
			rc.Cluster.Spec.DataSource = &domain.DataSource{Cluster: "deleted", Backup: "snap"}
			rc.Store = &snapshotTestStore{ClusterStore: rc.Store, backup: &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: "deleted", Name: "snap", Provider: "openbao", Type: "raft-snapshot", Phase: domain.PhaseSucceeded, Location: "retained/source/raft/snap.snap"}}
			for range 200 {
				s := state(t, rc)
				if s.Initialized {
					material, err := readUnseal(t.Context(), rc)
					if err != nil {
						t.Fatal(err)
					}
					raw, _ := json.Marshal(RestoreUnsealShares{Shares: material.Shares, Threshold: material.Threshold})
					if err = putSecret(t.Context(), rc, RestoreUnsealSecret("clone"), raw); err != nil {
						t.Fatal(err)
					}
				}
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s = state(t, rc)
				if s.CloneRestoreStep != "" {
					for _, m := range s.Members {
						if !m.Healthy || !m.Voter || m.Sealed {
							t.Fatalf("clone started before native formation: %+v", m)
						}
					}
				}
				if s.CloneRestoreStep == "post" {
					return
				}
				if s.CloneRestoreStep == "prepare" {
					p = New(p.options)
					a.p = p
				}
			}
			t.Fatalf("clone stuck: %s", rc.Cluster.Internal)
		})
	}
}

func TestRollingRechecksDisruptionBoundaries(t *testing.T) {
	for _, step := range []string{"apply", "deploy", "runtime-resources", "disk", "resource-config"} {
		for _, failure := range []string{"quorum", "leadership"} {
			t.Run(step+"/"+failure, func(t *testing.T) {
				p, rc, f, a := setup(t)
				ready(t, p, rc)
				s := state(t, rc)
				m := s.find("demom1")
				kind := "resources"
				if step == "apply" {
					kind = "restart"
				}
				if step == "deploy" {
					kind = "deploy"
				}
				m.Change = &memberChange{Kind: kind, Step: step, Version: rc.Cluster.Spec.Version, Resources: m.Resources, Disk: m.Disk}
				generation, config, mutations := m.ProcessGeneration, s.ConfigVersion, f.mutations
				if failure == "quorum" {
					a.member("demom2").unreachable = true
				} else {
					a.member("demom0").active = false
					a.member(m.Name).active = true
				}
				// Persisted observations deliberately remain healthy and standby.
				if err := p.changeStep(t.Context(), rc, s, m, true); err != nil {
					t.Fatal(err)
				}
				if m.ProcessGeneration != generation || s.ConfigVersion != config || f.mutations != mutations || m.Change.Step != step {
					t.Fatalf("disrupted after %s changed: %+v", failure, m.Change)
				}
				if s.QuorumBlocked == "" {
					t.Fatal("missing live disruption fence")
				}
				if failure == "leadership" && !slices.Contains(f.log.snapshot(), "step-down:"+m.Name) {
					t.Fatal("did not step down re-elected target")
				}
			})
		}
	}
}

func TestManagementRecoveryWithPendingTargetReceiptAndSuppliedShares(t *testing.T) {
	p, rc, _, a := setup(t)
	rc.Cluster.Spec.Components.Engine.Replicas = 1
	ready(t, p, rc)
	material, err := readUnseal(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(RestoreUnsealShares{Shares: material.Shares, Threshold: material.Threshold})
	if err = putSecret(t.Context(), rc, RestoreUnsealSecret("clone"), raw); err != nil {
		t.Fatal(err)
	}
	if err = putSecret(t.Context(), rc, SecretInitKey, []byte(`{"receiptId":"pending-target-receipt"}`)); err != nil {
		t.Fatal(err)
	}
	if err = p.prepareSnapshotMaterial(t.Context(), rc, domain.ClusterRef{Namespace: rc.Cluster.Ref().Namespace, Name: "deleted"}, "clone", true, &domain.Backup{}); err != nil {
		t.Fatal(err)
	}
	a.tokenValid = false
	token, err := p.ensureManagementToken(t.Context(), rc, state(t, rc))
	if err != nil || token == "" || a.generated != 1 || a.revoked != 1 {
		t.Fatalf("recovery token=%q generated=%d revoked=%d err=%v", token, a.generated, a.revoked, err)
	}
	if _, err = secret(t.Context(), rc, SecretInitKey); err != nil {
		t.Fatal("recovery prematurely discarded initialization receipt", err)
	}
}

func TestPostgresStatusDoesNotRequireRaftVoters(t *testing.T) {
	p, rc, _, _ := setup(t)
	ready(t, p, rc)
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
	s := state(t, rc)
	for _, m := range s.Members {
		m.Voter = false
	}
	if err := s.save(rc.Cluster); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != domain.PhaseReady {
		t.Fatalf("PostgreSQL health phase=%s", status.Phase)
	}
	for _, m := range status.Members {
		if !m.Health {
			t.Fatalf("healthy non-Raft peer degraded: %+v", m)
		}
	}
	for _, c := range status.Conditions {
		if strings.HasPrefix(c.Type, "RaftVoter/") {
			t.Fatalf("Raft condition for PostgreSQL: %+v", c)
		}
	}
}
