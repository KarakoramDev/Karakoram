// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Port of the v0.4.7 impact reproduction: counts were sufficient to release
// retirement with an extra UNREACHABLE predecessor or the wrong ONLINE UUIDs.
func TestExplicitLostReplacementIdentityBarrier(t *testing.T) {
	for _, mode := range []string{"old-unreachable-extra-row", "wrong-online-identities", "survivor-recovering", "older-agent", "different-view-id", "different-primary", "unknown-retiree-uuid"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b := setup(t, "group")
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			old := s.data()[2]
			oldName, oldVolume, oldUUID := old.Name, old.Volume, old.ServerUUID
			old.LastGTIDExecuted = recoverySID + ":1-10"
			for _, m := range s.data() {
				st := b.native[m.Name]
				st.GroupName, st.GTIDExecuted = s.Formation.GroupName, recoverySID+":1-20"
				b.native[m.Name] = st
			}
			delete(b.native, oldName)
			delete(b.deployed, oldName)
			old.ConfirmedVolumeLost, old.RuntimeRecovery.VolumeLost = true, true
			rc.Backend = physicallyLostBackend{fakeBackend: b, volume: oldVolume, volumeID: old.VolumeID}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if name == oldName && strings.HasPrefix(r.URL.Path, "/v1/mysql/") {
					// Shared status sweeps may probe unavailable members, but no
					// native mutation may target the confirmed-lost predecessor.
					if r.URL.Path != "/v1/mysql/status" {
						t.Errorf("native action on lost member: %s", r.URL.Path)
					}
					http.Error(w, "lost", http.StatusServiceUnavailable)
					return true
				}
				if r.URL.Path == "/v1/mysql/stop-group" {
					t.Errorf("unexpected native stop on %s", name)
				}
				return false
			}
			payload, _ := json.Marshal(map[string]string{"member": oldName})
			op := domain.Operation{ID: "identity-barrier", Kind: domain.OperationReplaceMember, Payload: payload, Actor: "operator"}
			rc.Operation = &op
			injected, blocked, restarted, converged := false, false, false, false
			nextName := ""
			blockedTicks := 0
			done := false
			for tick := range 240 {
				current, _ := load(rc.Cluster)
				if current.Replacement != nil && current.Replacement.Stage == "retire" && !injected {
					nextName = current.Replacement.New
					b.membership = map[string]*myspec.GroupView{}
					for _, observer := range current.data() {
						if observer.Name == oldName {
							continue
						}
						view := &myspec.GroupView{ViewID: "view:2"}
						for _, peer := range current.data() {
							if peer.Name == oldName {
								continue
							}
							native := b.native[peer.Name]
							view.Members = append(view.Members, myspec.GroupViewMember{ID: native.ServerUUID, Host: peer.Name, Port: 3306, State: "ONLINE", Role: native.MemberRole})
						}
						switch mode {
						case "old-unreachable-extra-row":
							view.Members = append(view.Members, myspec.GroupViewMember{ID: oldUUID, State: "UNREACHABLE"})
						case "wrong-online-identities":
							for i := range view.Members {
								if view.Members[i].ID != b.native[observer.Name].ServerUUID {
									view.Members[i].ID = oldUUID
									break
								}
							}
						case "older-agent":
							view = nil
						case "different-view-id":
							if observer.Name == nextName {
								view.ViewID = "view:3"
							}
						case "different-primary":
							if observer.Name == nextName {
								for i := range view.Members {
									if view.Members[i].Role == "PRIMARY" {
										view.Members[i].Role = "SECONDARY"
									} else {
										view.Members[i].Role = "PRIMARY"
									}
								}
							}
						}
						b.membership[observer.Name] = view
					}
					if mode == "survivor-recovering" {
						native := b.native[nextName]
						native.MemberState = "RECOVERING"
						b.native[nextName] = native
					}
					if mode == "unknown-retiree-uuid" {
						current.find(oldName).ServerUUID = ""
						if err := save(rc.Cluster, current); err != nil {
							t.Fatal(err)
						}
					}
					injected = true
				}
				result, err := p.RunOperation(t.Context(), rc, &op)
				if err != nil && !errors.Is(err, engine.ErrUnavailable) {
					t.Fatalf("tick %d: %v", tick, err)
				}
				if err != nil && injected && !converged && (strings.Contains(err.Error(), "identity convergence") || mode == "survivor-recovering") {
					blocked = true
					blockedTicks++
					if mode == "older-agent" && !strings.Contains(err.Error(), "upgrade agents") {
						t.Fatalf("missing upgrade diagnostic: %v", err)
					}
					if result.Done || len(b.deleted) != 0 || b.services[oldName] == nil || b.services[oldVolume] == nil {
						t.Fatal("unconverged views released destructive retirement")
					}
					assertRetirementSeedPresence(t, p, rc, oldName, true)
					if !restarted {
						// Reconstruct both CP provider and cluster snapshot from persisted JSON.
						recovered := *rc.Cluster
						recovered.Internal = append(json.RawMessage(nil), rc.Cluster.Internal...)
						rc.Cluster = &recovered
						p = New(Options{})
						restarted = true
					}
					if blockedTicks >= 3 {
						clear(b.membership)
						native := b.native[nextName]
						native.MemberState = "ONLINE"
						b.native[nextName] = native
						current, _ := load(rc.Cluster)
						if mode == "unknown-retiree-uuid" {
							current.find(oldName).ServerUUID = oldUUID
							if err := save(rc.Cluster, current); err != nil {
								t.Fatal(err)
							}
						}
						converged = true
					}
				}
				if err == nil {
					op.Checkpoint = result.Checkpoint
				}
				if result.Done {
					done = true
					break
				}
				if _, syncErr := p.Sync(t.Context(), rc); syncErr != nil && !errors.Is(syncErr, engine.ErrUnavailable) {
					t.Fatalf("sync %d: %v", tick, syncErr)
				}
				b.apply(t, p, rc)
			}
			if !done || !blocked || !restarted || !converged {
				t.Fatalf("done=%v blocked=%v restarted=%v converged=%v", done, blocked, restarted, converged)
			}
			current, _ := load(rc.Cluster)
			if current.find(oldName) != nil || b.services[oldName] != nil || b.services[oldVolume] != nil {
				t.Fatal("converged retirement retained predecessor")
			}
			for _, name := range b.stopped {
				if name != oldName {
					t.Fatalf("stopped survivor %s", name)
				}
			}
			for _, name := range b.deleted {
				if name != oldName && name != oldVolume {
					t.Fatalf("deleted survivor %s", name)
				}
			}
			for _, m := range current.data() {
				if b.services[m.Name] == nil || b.services[m.Volume] == nil {
					t.Fatal("survivor resource missing")
				}
			}
			assertRetirementSeedPresence(t, p, rc, oldName, false)
			if b.clones[nextName] == "" || b.clones[nextName] == oldName {
				t.Fatal("invalid clone donor")
			}
			t.Logf("blocked divergent view across CP reconstruction; converged; stopped=%v deleted=%v", b.stopped, b.deleted)
		})
	}
}

func assertRetirementSeedPresence(t *testing.T, p *Core, rc *engine.Context, old string, want bool) {
	t.Helper()
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for _, member := range s.data() {
		if member.Name == old {
			continue
		}
		config, err := p.MemberConfig(t.Context(), rc, member.Name)
		if err != nil {
			t.Fatal(err)
		}
		var spec myspec.Spec
		if err := json.Unmarshal(config.EngineSpec, &spec); err != nil {
			t.Fatal(err)
		}
		present := false
		for _, seed := range spec.GroupReplication.Seeds {
			present = present || seed.Name == old
		}
		if present != want {
			t.Fatalf("predecessor seed on %s present=%v want=%v", member.Name, present, want)
		}
	}
}
