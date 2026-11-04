// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"bytes"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"testing"
	"time"
)

// Every tick reconstructs Core; no lifecycle progress may depend on local memory.
func day2Ticks(t *testing.T, rc *engine.Context, b *fakeBackend, done func(*state) bool) {
	t.Helper()
	for i := range 240 {
		p := New(Options{Provider: rc.Cluster.Spec.Provider})
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatalf("tick %d: %v", i, err)
		}
		b.apply(t, p, rc)
		s, err := load(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if done(s) {
			return
		}
	}
	t.Fatal("day-2 reconciliation did not finish")
}

func TestScaleGroupThreeFiveThreeReplay(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	day2Ticks(t, rc, b, func(s *state) bool {
		if len(s.data()) != 5 {
			return false
		}
		for _, m := range s.data() {
			if m.Stage != "joined" {
				return false
			}
		}
		return true
	})
	s, _ := load(rc.Cluster)
	if s.NextIndex != 5 || b.clones["demom3"] == "" || b.clones["demom4"] == "" {
		t.Fatalf("scale-out did not clone monotonic identities: %+v", s)
	}
	// Force the highest identity to be primary so scale-in must hand off first.
	for name, st := range b.native {
		st.MemberRole = "SECONDARY"
		st.ReadOnly = true
		st.SuperReadOnly = true
		if name == "demom4" {
			st.MemberRole = "PRIMARY"
			st.ReadOnly = false
			st.SuperReadOnly = false
			b.leader = name
		}
		b.native[name] = st
	}
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	day2Ticks(t, rc, b, func(s *state) bool { return len(s.data()) == 3 && s.Pending == nil })
	s, _ = load(rc.Cluster)
	if s.NextIndex != 5 || b.services["demom4"] != nil || b.services["demom4v"] != nil || b.services["demom3"] != nil || b.leader == "demom4" {
		t.Fatalf("unsafe/incomplete scale-in: %+v", s)
	}
}

func TestAdoptSingletonVolumeReplay(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	seed := s.data()[0]
	volumeID := seed.VolumeID
	rc.Cluster.Generation++
	rc.Cluster.Spec.Topology.Type = "group"
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	proxy := rc.Cluster.Spec.Components.Extra["proxy"]
	proxy.Replicas = 1
	rc.Cluster.Spec.Components.Extra["proxy"] = proxy
	day2Ticks(t, rc, b, func(s *state) bool {
		return s.Formation.State == "formed" && len(s.data()) == 3 && s.data()[2].Stage == "joined"
	})
	s, _ = load(rc.Cluster)
	if s.data()[0].VolumeID != volumeID || b.clones[seed.Name] != "" || b.bootstrap[s.Formation.FormationEpoch] != 1 {
		t.Fatalf("singleton volume was not adopted once: %+v", s)
	}
}

func runDay2Operation(t *testing.T, rc *engine.Context, b *fakeBackend, op *domain.Operation) {
	t.Helper()
	for i := range 240 {
		p := New(Options{Provider: rc.Cluster.Spec.Provider})
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatalf("operation tick %d: %v", i, err)
		}
		if result.Done {
			// Simulate losing the final operation response: cluster tombstone wins.
			replay, err := New(Options{Provider: rc.Cluster.Spec.Provider}).RunOperation(t.Context(), rc, op)
			if err != nil || !replay.Done {
				t.Fatalf("completion replay: %+v %v", replay, err)
			}
			return
		}
		op.Checkpoint = result.Checkpoint
		if p.SyncDuringOperation(op) {
			if _, err = p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
		}
		b.apply(t, p, rc)
	}
	t.Fatal("operation did not finish")
}

func TestReplacementReplayAndRetirement(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	old := b.leader
	op := &domain.Operation{ID: "replace-one", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + old + `"}`)}
	runDay2Operation(t, rc, b, op)
	s, _ := load(rc.Cluster)
	if len(s.data()) != 3 || s.NextIndex != 4 || s.find(old) != nil || b.services[old] != nil || b.services[old+"v"] != nil || b.clones["demom3"] == "" {
		t.Fatalf("replacement did not complete add-before-remove: %+v", s)
	}
}

func TestReplacementCancellationReconcilesRollback(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	op := &domain.Operation{ID: "cancel-replacement", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"demom0"}`)}
	result, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = result.Checkpoint
	if _, err = p.CancelOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	day2Ticks(t, rc, b, func(s *state) bool { return s.Replacement == nil })
	s, _ := load(rc.Cluster)
	if len(s.data()) != 3 || s.find("demom0") == nil || s.find("demom3") != nil {
		t.Fatalf("rollback retired predecessor: %+v", s)
	}
}

func TestRotateTLSReplay(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	before, err := secret(t.Context(), rc, "tls/servers/demom0.crt")
	if err != nil {
		t.Fatal(err)
	}
	runDay2Operation(t, rc, b, &domain.Operation{ID: "rotate-one", Kind: domain.OperationRotateTLS})
	after, err := secret(t.Context(), rc, "tls/servers/demom0.crt")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(before, after) {
		t.Fatal("rotation did not issue a new leaf")
	}
}

func TestResourceRollPrimaryLastReplay(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	primary := b.leader
	rc.Cluster.Generation++
	rc.Cluster.Spec.Components.Engine.Resources.MinRAM = 3
	rc.Cluster.Spec.Components.Engine.Resources.MaxRAM = 3
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	if s.Rolling == nil || s.Rolling.Order[len(s.Rolling.Order)-1] != primary {
		t.Fatalf("roll not primary-last: %+v", s.Rolling)
	}
	day2Ticks(t, rc, b, func(s *state) bool { return s.Rolling == nil })
	s, _ = load(rc.Cluster)
	for _, m := range s.data() {
		if m.Resources.MinRAM != 3 || m.Offline {
			t.Fatalf("resource roll unfinished: %+v", m)
		}
	}
}

func TestMinorUpgradeRollingReplay(t *testing.T) {
	p, rc, b := setup(t, "group")
	rc.Cluster.Spec.Version = "8.4.10-10"
	converge(t, p, rc, b)
	primary := b.leader
	rc.Cluster.Generation++
	rc.Cluster.Spec.Version = "8.4.11-11"
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	if s.Rolling == nil || s.Rolling.Major || s.Rolling.Order[len(s.Rolling.Order)-1] != primary {
		t.Fatal("minor upgrade order")
	}
	day2Ticks(t, rc, b, func(s *state) bool { return s.Rolling == nil })
	s, _ = load(rc.Cluster)
	for _, m := range s.Members {
		if m.Version != rc.Cluster.Spec.Version {
			t.Fatalf("old pinned artifact: %+v", m)
		}
	}
}

func TestMajorUpgradeBackupGateAndSlowShutdownReplay(t *testing.T) {
	for _, topology := range []string{"single", "group"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, b := setup(t, topology)
			rc.Cluster.Spec.Version = "8.0.46-37"
			converge(t, p, rc, b)
			rc.Cluster.Generation++
			rc.Cluster.Spec.Version = "8.4.11-11"
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ := load(rc.Cluster)
			u := s.Rolling
			if u == nil || !u.Major || u.Stage != "backup" {
				t.Fatal("major upgrade journal absent")
			}
			if _, err := p.Sync(t.Context(), rc); err == nil {
				t.Fatal("major upgrade accepted without fresh backup")
			}
			// Exercise the exact gate without requiring physical backup storage in this fixture.
			rc.Cluster.Spec.Backup.Storage = "fixture"
			started := u.RequestedAt.Add(time.Second)
			completed := started.Add(time.Second)
			if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "upgrade-gate", Provider: p.Name(), Storage: "fixture", Type: "full", Phase: domain.PhaseSucceeded, SourceVersion: u.From, MySQL: &domain.MySQLBackup{Label: "full-upgrade"}, StartedAt: &started, CompletedAt: &completed}); err != nil {
				t.Fatal(err)
			}
			if err := requireUpgradeBackup(t.Context(), rc, u); err != nil {
				t.Fatal(err)
			}
			stale := *u
			stale.RequestedAt = completed.Add(time.Second)
			if err := requireUpgradeBackup(t.Context(), rc, &stale); err == nil {
				t.Fatal("stale backup passed")
			}
			// Keep the repository-free fixture; the durable gate is already accepted.
			rc.Cluster.Spec.Backup.Storage = ""
			u.Stage = "prepare"
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			sawSlow := false
			day2Ticks(t, rc, b, func(s *state) bool {
				if s.Rolling != nil {
					for _, m := range s.data() {
						if m.SlowShutdown {
							sawSlow = true
						}
					}
				}
				return s.Rolling == nil
			})
			if !sawSlow {
				t.Fatal("major upgrade never published slow clean shutdown")
			}
			s, _ = load(rc.Cluster)
			for _, m := range s.data() {
				if m.Version != "8.4.11-11" || m.SlowShutdown || m.Offline {
					t.Fatalf("major upgrade unfinished: %+v", m)
				}
			}
		})
	}
}

func TestDay2RefusesFormationAndRestoreFences(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	s.Formation.Reformation = &reformation{State: "observing"}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := maintenanceAllowed(t.Context(), rc, s); err == nil {
		t.Fatal("formation fence ignored")
	}
	if _, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "blocked", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"demom0"}`)}); err == nil {
		t.Fatal("replacement crossed formation fence")
	}
	s.Formation.Reformation = nil
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := saveBackupState(rc.Cluster, backupState{RestoreOperation: &domain.Operation{ID: "restore-owner", Kind: domain.OperationRestore}}); err != nil {
		t.Fatal(err)
	}
	if err := maintenanceAllowed(t.Context(), rc, s); err == nil {
		t.Fatal("restore fence ignored")
	}
}

func TestParameterClassificationAndPinnedRollReplay(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	rc.Cluster.Generation++
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"parameters":{"max_connections":"300"}}`)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	if s.Rolling != nil {
		t.Fatal("dynamic variable caused a restart")
	}
	primary := b.leader
	rc.Cluster.Generation++
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"parameters":{"max_connections":"300","table_open_cache_instances":"4"}}`)
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if s.Rolling == nil || s.Rolling.Order[len(s.Rolling.Order)-1] != primary {
		t.Fatal("startup-only parameter did not create primary-last roll")
	}
	for _, m := range s.data() {
		if values := memberParameters(m, map[string]string{"table_open_cache_instances": "4"}); values["table_open_cache_instances"] != "" {
			t.Fatal("unselected member received new startup-only setting")
		}
	}
	day2Ticks(t, rc, b, func(s *state) bool { return s.Rolling == nil })
	s, _ = load(rc.Cluster)
	for _, m := range s.data() {
		if m.Parameters["table_open_cache_instances"] != "4" {
			t.Fatalf("startup-only parameter not pinned: %+v", m)
		}
	}
}

func TestMembershipRefusesActiveBackup(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	if err := rc.Store.CreateBackup(t.Context(), &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "active", Type: "full", Phase: domain.PhaseRunning}); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("scale crossed physical backup fence")
	}
	s, _ := load(rc.Cluster)
	if len(s.data()) != 3 {
		t.Fatal("scale allocated a member while backup was active")
	}
}
