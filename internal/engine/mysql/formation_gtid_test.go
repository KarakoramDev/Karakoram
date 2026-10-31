// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPXCFormationGTIDCertification(t *testing.T) {
	const direct = "fc8b3a31-d49d-11f1-a52e-2a8aa2dd75f9"
	const inverse = "0374c5ce-2b62-ee0e-5ad1-d5755d228a06"
	const historical = "bac91544-d464-11f1-a1f6-8b04c1d7e37c:1-711"
	for _, scenario := range []string{"direct", "inverse", "remedy", "persistent", "unique", "legacy", "empty-join", "live-join", "resume"} {
		t.Run(scenario, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			if scenario == "empty-join" || scenario == "live-join" {
				converge(t, p, rc, b)
				rc.Cluster.Spec.Components.Engine.Replicas = 5
			}
			mapping, histories := map[string]string{}, map[string]string{}
			sequence, probes, resets := int64(0), 0, 0
			proxyStops := 0
			receipts := map[string]myspec.GaleraStatusResponse{}
			bad := rc.Cluster.Name + "m2"
			if scenario == "empty-join" {
				bad = rc.Cluster.Name + "m3"
			}
			prior := b.action
			native := func(name string) myspec.GaleraStatusResponse {
				current, _ := load(rc.Cluster)
				if histories[name] == "" {
					histories[name] = historical
				}
				member := current.find(name)
				fenced := current.RestoreGTIDFence != "" || member != nil && member.GaleraJoinFence
				out := myspec.GaleraStatusResponse{ClusterStatus: "non-Primary", StateUUID: direct, LastCommitted: sequence, GTIDExecuted: histories[name], ServerUUID: b.native[name].ServerUUID, OfflineMode: fenced}
				for _, m := range current.data() {
					if b.native[m.Name].MemberState == "ONLINE" {
						out.ClusterSize++
					}
				}
				if b.native[name].MemberState == "ONLINE" {
					out.ClusterStatus, out.LocalStateComment, out.Ready, out.Connected = "Primary", "Synced", true, true
				}
				if scenario == "live-join" && member != nil {
					out.LastCommitted = sequence + int64(member.Index)
				}
				return out
			}
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if mapping[name] == "" {
					mapping[name] = direct
					if scenario == "inverse" || name == bad && (scenario == "remedy" || scenario == "persistent" || scenario == "unique" || scenario == "empty-join" || scenario == "resume") {
						mapping[name] = inverse
					}
					histories[name] = historical
					if scenario == "unique" && name == bad {
						histories[name] += ",aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:1"
					}
				}
				if strings.Contains(r.URL.Path, "/processes/proxysql/stop") {
					proxyStops++
				}
				switch r.URL.Path {
				case "/v1/status":
					pid := 1
					if b.native[name].MemberState != "ONLINE" {
						pid = 0
					}
					process := "mysqld"
					if strings.HasSuffix(name, "mx") {
						process = "proxysql"
					}
					_ = json.NewEncoder(w).Encode(agentapi.Status{Agent: b.agentBuild, MySQLRestoreCertification: scenario != "legacy", ConfigVersion: b.applied[name], Processes: map[string]agentapi.ProcessStatus{process: {PID: pid}}})
					return true
				case "/v1/mysql/galera-status":
					_ = json.NewEncoder(w).Encode(native(name))
					return true
				case "/v1/mysql/galera-history-probe":
					var req myspec.GaleraHistoryProbeRequest
					if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
						t.Error(err)
					}
					current, _ := load(rc.Cluster)
					if current.RestoreGTIDFence == "" && !current.find(name).GaleraJoinFence {
						t.Error("formation probe lacked a privileged member fence")
					}
					if saved, ok := receipts[req.Receipt]; ok {
						_ = json.NewEncoder(w).Encode(saved)
						return true
					}
					sequence++
					probes++
					for _, m := range current.data() {
						if histories[m.Name] == "" {
							histories[m.Name] = historical
						}
						sid := mapping[m.Name]
						if sid == "" {
							sid = direct
							mapping[m.Name] = sid
						}
						histories[m.Name] += fmt.Sprintf(",%s:%d", sid, sequence)
					}
					out := native(name)
					if scenario == "live-join" {
						// The controlled DDL crossed every member, then ordinary
						// writers advance the seed/old members while new joiners
						// legitimately lag by one replicated transaction.
						out.LastCommitted = sequence
						sequence++
						for _, m := range current.data() {
							if !m.GaleraJoinFence {
								histories[m.Name] += fmt.Sprintf(",%s:%d", direct, sequence)
							}
						}
					}
					receipts[req.Receipt] = out
					_ = json.NewEncoder(w).Encode(out)
					return true
				case "/v1/mysql/galera-prepare-join":
					current, _ := load(rc.Cluster)
					target := current.find(name)
					if name == current.Formation.Seed || target == nil || !target.Offline || target.GaleraSSTDonor != current.Formation.Seed || current.RestoreGTIDFence == "" && !target.GaleraJoinFence {
						t.Error("formation SST lacked stopped empty target/seed donor authority")
					}
					resets++
					histories[name] = histories[current.Formation.Seed]
					if scenario != "persistent" {
						mapping[name] = mapping[current.Formation.Seed]
					}
					_ = json.NewEncoder(w).Encode(struct{}{})
					return true
				}
				return prior != nil && prior(w, r, name)
			}
			completed, blocked, resumed := false, false, false
			for i := 0; i < 160; i++ {
				b.apply(t, p, rc)
				_, err := p.Sync(t.Context(), rc)
				current, _ := load(rc.Cluster)
				c := current.Formation.GTIDCertification
				if err != nil {
					if scenario != "persistent" && scenario != "unique" {
						t.Fatalf("iteration%d: %v", i, err)
					}
					if c == nil || c.Blocked == "" {
						t.Fatalf("unrelated failure: %v", err)
					}
					blocked = true
					break
				}
				if c == nil {
					continue
				}
				if scenario == "resume" && !resumed && c.Checkpoint.GTIDCertification != nil && c.Checkpoint.GTIDCertification.Phase == "reset" {
					p = New(Options{Provider: "pxc"})
					resumed = true
				}
				if len(current.data()) == rc.Cluster.Spec.Components.Engine.Replicas && (c.Legacy || c.Checkpoint.GTIDCertification != nil && c.Checkpoint.GTIDCertification.Certified && !c.Checkpoint.GTIDCertification.FencesOwned && len(c.Members) == len(current.data())) {
					completed = true
					break
				}
			}
			current, _ := load(rc.Cluster)
			c := current.Formation.GTIDCertification
			if current.RestoreGTIDFence != "" {
				t.Fatal("terminal formation retained client fence")
			}
			if scenario == "empty-join" && proxyStops != 0 {
				t.Fatal("empty scale join stopped existing client proxy")
			}
			if scenario == "persistent" || scenario == "unique" {
				if !blocked || c == nil || c.Blocked == "" {
					t.Fatal("unsafe formation did not fail closed")
				}
				if scenario == "unique" && resets != 0 {
					t.Fatal("unique data was automatically discarded")
				}
				if scenario == "persistent" && resets != 2 {
					t.Fatalf("SST attempts=%d want2", resets)
				}
				condition, ready := pxcFormationMappingCondition(current)
				if ready || condition.Status != "False" {
					t.Fatal("blocked mapping advertised Ready")
				}
			} else {
				if !completed || c == nil {
					t.Fatalf("formation stalled: %+v", c)
				}
				if scenario == "legacy" {
					condition, _ := pxcFormationMappingCondition(current)
					if !c.Legacy || probes != 0 || resets != 0 || condition.Reason != "CertificationUnavailable" {
						t.Fatal("legacy path acquired certification controls")
					}
				} else {
					if !c.Checkpoint.GTIDCertification.Certified || len(c.EmptyMembers) != 0 {
						t.Fatal("certificate missing or erase authority survived activation")
					}
					if scenario == "remedy" || scenario == "empty-join" || scenario == "resume" {
						if resets != 1 || probes != 2 || activeGTIDIncident(current) == nil {
							t.Fatalf("remedy lost evidence/probe: resets=%d probes=%d", resets, probes)
						}
					} else if resets != 0 || probes != 1 {
						t.Fatalf("unexpected probe/remedy: %d/%d", probes, resets)
					}
					if scenario == "live-join" && (!c.JoinOnly || len(c.Checkpoint.GTIDCertification.Evidence) == 0 || len(c.Checkpoint.GTIDCertification.Evidence[0].CertifiedCut) == 0 || proxyStops != 0) {
						t.Fatal("live-write join did not preserve a seed cut without stopping clients")
					}
				}
			}
		})
	}
}

func TestPXCExistingLatchedFormationHasExplicitRecoveryAction(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	s.Formation.GTIDCertification = nil
	s.GTIDIncidents = append(s.GTIDIncidents, gtidIncident{DetectedAt: now(rc), Audited: true})
	if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatalf("unowned diagnostic blocked ordinary native observation: %v", err)
	}
	current, _ := load(rc.Cluster)
	condition, ready := pxcFormationMappingCondition(current)
	if ready || condition.Status != "False" || !strings.Contains(condition.Message, "restore") || current.RestoreGTIDFence != "" {
		t.Fatal("existing latched data was silently erased, unfenced without guidance or claimed Ready")
	}
	if activeGTIDIncident(current) == nil {
		t.Fatal("existing incident silently cleared")
	}
}

func TestPXCScaleJoinStallBlocksWithoutStoppingExistingClients(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	rc.Cluster.Spec.Components.Engine.Replicas = 5
	base := time.Now().UTC()
	rc.Clock = trafficRenewalClock{at: base}
	first, second := rc.Cluster.Name+"m3", rc.Cluster.Name+"m4"
	proxyStops, secondStarts := 0, 0
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if strings.Contains(r.URL.Path, "/processes/proxysql/stop") {
			proxyStops++
		}
		if name == second && strings.Contains(r.URL.Path, "/processes/mysqld/start") {
			secondStarts++
		}
		if name == first && r.URL.Path == "/v1/mysql/"+myspec.ActionStatus {
			st := b.native[name]
			st.MemberState, st.MemberRole = "JOINER", "SECONDARY"
			_ = json.NewEncoder(w).Encode(st)
			return true
		}
		if name == first && r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraStatus {
			_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Joining", Connected: true})
			return true
		}
		if name == second && r.URL.Path == "/v1/mysql/"+myspec.ActionGaleraStatus {
			http.Error(w, "held mysqld is stopped; SQL unavailable", http.StatusServiceUnavailable)
			return true
		}
		return prior != nil && prior(w, r, name)
	}
	observedWait := false
	for range 100 {
		b.apply(t, p, rc)
		_, err := p.Sync(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		s, _ := load(rc.Cluster)
		c := s.Formation.GTIDCertification
		if c != nil && c.JoinOnly && c.Checkpoint.GTIDCertification != nil && c.Checkpoint.GTIDCertification.FencesOwned {
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			observedWait = true
			break
		}
	}
	if !observedWait {
		t.Fatal("fixture did not reach first empty join wait")
	}
	rc.Clock = trafficRenewalClock{at: base.Add(pxcFormationAdmissionTimeout + time.Second)}
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("stalled join admitted unbounded waiting")
	}
	s, _ := load(rc.Cluster)
	c := s.Formation.GTIDCertification
	if c == nil || !strings.Contains(c.Blocked, "five minutes") || !c.Checkpoint.GTIDCertification.CleanupComplete {
		t.Fatalf("stall did not settle explicit retryable cleanup: %+v", c)
	}
	if proxyStops != 0 || secondStarts != 0 || s.RestoreGTIDFence != "" {
		t.Fatal("stalled scale join stopped clients or released the later joiner")
	}
	for _, m := range s.data() {
		if m.GaleraJoinFence {
			t.Fatal("terminal scale failure retained member fence")
		}
	}
	condition, ready := pxcFormationMappingCondition(s)
	if ready || condition.Status != "False" || !strings.Contains(condition.Message, "restore") {
		t.Fatal("stalled admission lacks fail-closed operator diagnostic")
	}
}
