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
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: backend.applied[member], Processes: map[string]agentapi.ProcessStatus{"mysqld": {PID: pid}}})
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
