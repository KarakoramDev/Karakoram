// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestRestoreReformReleasesOwnedStopHolds(t *testing.T) {
	for _, topology := range []string{"single", "group", "cluster"} {
		t.Run(topology, func(t *testing.T) {
			var p *Core
			var rc *engine.Context
			var backend *fakeBackend
			if topology == "cluster" {
				p, rc, backend = setupPXC(t)
			} else {
				p, rc, backend = setup(t, topology)
			}
			converge(t, p, rc, backend)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			held, starts := map[string]bool{}, map[string]int{}
			s.Formation.GTIDCertification = nil // Restore, not the prior formation, owns this synthetic new epoch.
			s.Formation.FormationEpoch++
			if clustered(topology) {
				s.Formation.State = "bootstrapping"
			}
			for _, m := range s.data() {
				held[m.Name] = true // Durable stop holds created by restore fence.
				m.Offline = clustered(topology) && m.Name != s.Formation.Seed
				st := backend.native[m.Name]
				st.MemberState = "OFFLINE"
				st.DataIdentity.FormationEpoch = 0
				backend.native[m.Name] = st
			}
			publishRevision(s)
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			previous := backend.action
			backend.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
				if r.URL.Path == "/v1/status" {
					pid := 1
					if held[member] {
						pid = 0
					}
					json.NewEncoder(w).Encode(agentapi.Status{MySQLRestoreCertification: true, ConfigVersion: backend.applied[member], Processes: map[string]agentapi.ProcessStatus{"mysqld": {PID: pid}}})
					return true
				}
				if strings.HasPrefix(r.URL.Path, "/v1/processes/") && strings.HasSuffix(r.URL.Path, "/start") {
					current, err := load(rc.Cluster)
					if err != nil {
						t.Error(err)
						return true
					}
					m := current.find(member)
					if m.Offline || backend.applied[member] < m.ConfigVersion {
						t.Error("restore started a fenced member or stale configuration", member)
					}
					if clustered(topology) && member != current.Formation.Seed && current.Formation.State != "formed" {
						t.Error("restore started follower before seed formation", member)
					}
					held[member] = false
					starts[member]++
					json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
					return true
				}
				if held[member] && (r.URL.Path == "/v1/mysql/status" || r.URL.Path == "/v1/mysql/bootstrap" || r.URL.Path == "/v1/mysql/galera-bootstrap") {
					http.Error(w, "mysqld remains intentionally stopped", http.StatusServiceUnavailable)
					return true
				}
				if r.URL.Path == "/v1/mysql/galera-prepare-join" {
					json.NewEncoder(w).Encode(struct{}{})
					return true
				}
				return previous != nil && previous(w, r, member)
			}
			cp := backupCheckpoint{Step: "reform", Member: s.Formation.Seed, Request: &myspec.Restore{NewGroupName: s.Formation.GroupName}}
			op := &domain.Operation{ID: "restore-held-processes", Kind: domain.OperationRestore}
			// Config acknowledgement must precede hold release. Reload alone must
			// not be modeled as an explicit start, as it was in older fixtures.
			if _, err := p.runRestore(t.Context(), rc, op, cp); err != nil {
				t.Fatal(err)
			}
			if len(starts) != 0 {
				t.Fatal("started before restored config was acknowledged", starts)
			}
			for range 30 {
				backend.apply(t, p, rc) // Deliberately does not clear held.
				result, err := p.runRestore(t.Context(), rc, op, cp)
				if err != nil {
					t.Fatal(err)
				}
				if result.Step == "activate" {
					for _, m := range s.data() {
						if held[m.Name] || starts[m.Name] != 1 {
							t.Fatalf("member %s hold=%v starts=%d", m.Name, held[m.Name], starts[m.Name])
						}
					}
					return
				}
				p = New(Options{Provider: p.Name()}) // CP reconstruction retains holds.
			}
			t.Fatal("restore never reformed after durable process stop")
		})
	}
}

func TestPXCRestoreReformSerializesNonSyncedFollowers(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		name := "capable"
		if legacy {
			name = "legacy"
		}
		t.Run(name, func(t *testing.T) {
			p, rc, backend := setupPXC(t)
			converge(t, p, rc, backend)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			s.Formation.GTIDCertification = nil // The restore owns certification.
			s.Formation.State = "formed"
			data := s.data()
			seed, first, second := s.find(s.Formation.Seed), data[1], data[2]
			held := map[string]bool{first.Name: true, second.Name: true}
			starts := map[string]int{}
			firstSynced, proxyHeld := false, false
			op := &domain.Operation{ID: "restore-serial-" + name, Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhaseRunning}
			cp := backupCheckpoint{Step: "reform", Member: seed.Name, Request: &myspec.Restore{NewGroupName: s.Formation.GroupName},
				GTIDCertification: &pxcRestoreCertification{Negotiated: true, Legacy: legacy, Phase: "negotiated", FencesOwned: !legacy}}
			if !legacy {
				s.RestoreGTIDFence = op.ID
			}
			for _, m := range data {
				m.Offline, m.GaleraJoinFence = false, false
				if m.Name != seed.Name {
					m.Stage, m.Initialized = "deployed", false
				}
				native := backend.native[m.Name]
				if held[m.Name] {
					native.MemberState = "OFFLINE"
				}
				backend.native[m.Name] = native
			}
			publishRevision(s)
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if err := persistRestore(rc, op, cp); err != nil {
				t.Fatal(err)
			}
			previous := backend.action
			backend.action = func(w http.ResponseWriter, r *http.Request, member string) bool {
				if r.URL.Path == "/v1/status" {
					process, pid := "mysqld", 1
					if member == rc.Cluster.Name+"mx" {
						process = "proxysql"
						if proxyHeld {
							pid = 0
						}
					} else if held[member] {
						pid = 0
					}
					_ = json.NewEncoder(w).Encode(agentapi.Status{MySQLRestoreCertification: !legacy, ConfigVersion: backend.applied[member], Processes: map[string]agentapi.ProcessStatus{process: {PID: pid}}})
					return true
				}
				if strings.HasPrefix(r.URL.Path, "/v1/processes/") {
					if member == rc.Cluster.Name+"mx" && strings.HasSuffix(r.URL.Path, "/stop") {
						proxyHeld = true
						_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
						return true
					}
					if strings.HasSuffix(r.URL.Path, "/start") {
						if member == second.Name && !firstSynced {
							t.Error("second restore follower started before first became Synced")
						}
						if held[member] {
							starts[member]++
						}
						held[member] = false
						_ = json.NewEncoder(w).Encode(agentapi.ActionResponse{Success: true})
						return true
					}
				}
				if r.URL.Path == "/v1/mysql/status" {
					if held[member] {
						http.Error(w, "mysqld remains intentionally stopped", http.StatusServiceUnavailable)
						return true
					}
					if member == first.Name && !firstSynced {
						native := backend.native[member]
						native.MemberState = "Joining"
						_ = json.NewEncoder(w).Encode(native)
						return true
					}
				}
				return previous != nil && previous(w, r, member)
			}
			pass := func() engine.OpResult {
				t.Helper()
				backend.apply(t, p, rc)
				backend.mu.Lock()
				for _, m := range data {
					native := backend.native[m.Name]
					if held[m.Name] {
						native.MemberState = "OFFLINE"
					} else if m.Name == first.Name && !firstSynced {
						native.MemberState = "Joining"
					} else {
						native.MemberState = "ONLINE"
					}
					backend.native[m.Name] = native
				}
				backend.mu.Unlock()
				result, err := p.runRestore(t.Context(), rc, op, backupCheckpoint{})
				if err != nil {
					t.Fatal(err)
				}
				p = New(Options{Provider: "pxc"})
				return result
			}
			for range 4 {
				if result := pass(); result.Step != "reform" {
					t.Fatalf("restore advanced without a Synced first follower: %+v", result)
				}
			}
			backend.mu.Lock()
			if held[first.Name] || starts[first.Name] != 1 || !held[second.Name] || starts[second.Name] != 0 {
				t.Errorf("restored followers were not serialized: holds=%v starts=%v", held, starts)
			}
			firstSynced = true
			backend.mu.Unlock()
			for range 16 {
				result := pass()
				if result.Step == "certify" || result.Step == "activate" {
					backend.mu.Lock()
					defer backend.mu.Unlock()
					if held[second.Name] || starts[second.Name] != 1 {
						t.Fatalf("second follower never joined after first Synced: holds=%v starts=%v", held, starts)
					}
					return
				}
			}
			t.Fatal("restored formation did not advance after both serialized joins")
		})
	}
}
