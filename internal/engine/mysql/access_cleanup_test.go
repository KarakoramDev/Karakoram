// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestCleanupResolvesOwnedAccessIntentReplay(t *testing.T) {
	for _, keep := range []bool{false, true} {
		for _, outcome := range []string{"applied", "ambiguous", "rejected"} {
			t.Run(outcome+map[bool]string{false: "-delete", true: "-suspend"}[keep], func(t *testing.T) {
				p, rc, b := setup(t, "group")
				converge(t, p, rc, b)
				connected := false
				calls := 0
				var hash string
				installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
					switch r.URL.Path {
					case "/v1/mysql/status":
						json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
					case "/v1/mysql/" + myspec.ActionAccessUsers:
						var req myspec.AccessRequest
						if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
							t.Error(err)
						}
						calls++
						if calls == 1 {
							hash = accessRequestHash(req)
						} else if hash != accessRequestHash(req) {
							t.Error("cleanup changed sealed request identity")
						}
						if !connected && outcome != "applied" {
							w.WriteHeader(503)
							return
						}
						result := myspec.AccessResult{Outcome: "applied"}
						if outcome == "rejected" {
							result = myspec.AccessResult{Outcome: "rejected", Status: http.StatusConflict}
						}
						json.NewEncoder(w).Encode(result)
					case "/v1/status":
						if !connected {
							w.WriteHeader(503)
							return
						}
						cfg, err := New(Options{}).MemberConfig(r.Context(), rc, "demomx")
						if err != nil {
							t.Error(err)
							w.WriteHeader(503)
							return
						}
						json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: cfg.ConfigVersion})
					case "/v1/mysql/proxy-status":
						json.NewEncoder(w).Encode(myspec.ProxyStatusResponse{Backends: []myspec.ProxyBackend{{Hostgroup: 10, Status: "ONLINE"}}})
					default:
						t.Error("unexpected route", r.URL.Path)
						w.WriteHeader(404)
					}
				})
				ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
				_, err := p.CreateUser(ctx, rc, UserWrite{Name: "app"})
				cancel()
				if err == nil {
					t.Fatal("expected interrupted access publication")
				}
				intent, err := loadAccessIntent(t.Context(), rc)
				if err != nil || intent == nil || intent.Applied != (outcome == "applied") {
					t.Fatal("missing durable access stage", err)
				}
				rc.Cluster.Deleting = true
				rc.Cluster.Status.Phase = domain.PhaseDeleting
				if _, err := New(Options{}).CreateUser(t.Context(), rc, UserWrite{Name: "other"}); !errors.Is(err, engine.ErrUnavailable) {
					t.Fatal("new access admitted during cleanup", err)
				}
				services := len(b.services)
				state, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				state.Formation.Reformation = &reformation{State: "observing"}
				if err := save(rc.Cluster, state); err != nil {
					t.Fatal(err)
				}
				before := calls
				if done, err := New(Options{}).Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep}); done || !errors.Is(err, engine.ErrUnavailable) || calls != before || len(b.services) != services {
					t.Fatal("cleanup crossed active native recovery fence", done, err)
				}
				state.Formation.Reformation = &reformation{State: "formed"}
				if err := save(rc.Cluster, state); err != nil {
					t.Fatal(err)
				}
				ctx, cancel = context.WithTimeout(t.Context(), 100*time.Millisecond)
				done, err := New(Options{}).Cleanup(ctx, rc, domain.DeleteOptions{KeepVolumes: keep})
				cancel()
				if done || err == nil || len(b.services) != services {
					t.Fatal("cleanup discarded unresolved access or removed runtime", done, err)
				}
				connected = true
				for range 80 {
					done, err = New(Options{}).Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep})
					if err != nil {
						t.Fatal(err)
					}
					if done {
						break
					}
				}
				if !done {
					t.Fatal("cleanup did not converge after connectivity returned")
				}
				if pending, err := loadAccessIntent(t.Context(), rc); err != nil || pending != nil {
					t.Fatal("resolved access intent retained", err)
				}
				if outcome == "applied" && calls != 1 {
					t.Fatal("applied SQL repeated", calls)
				}
				if keep {
					s, _ := load(rc.Cluster)
					if !s.Suspended {
						t.Fatal("suspension receipt missing")
					}
					users, err := managedProxyUsers(t.Context(), rc)
					if err != nil || (len(users) == 0) != (outcome == "rejected") {
						t.Fatal("incorrect credential finish/rollback", err)
					}
				} else if len(b.services) != 0 {
					t.Fatal("delete retained services")
				}
			})
		}
	}
}

func TestAccessIntentPreservesGaleraTLSOwner(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.GaleraTLS = &galeraTLSRotation{}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	request := myspec.AccessRequest{RequestID: "older-access", Operation: "delete", Name: "app"}
	intent := &accessIntent{Request: request, RequestHash: accessRequestHash(request), Revision: s.ConfigVersion + 1, Applied: true}
	if err := sealAccessIntent(t.Context(), rc, intent); err != nil {
		t.Fatal(err)
	}
	if _, err := New(Options{}).ListUsers(t.Context(), rc); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("public access crossed shared TLS rotation fence", err)
	}
	if err := New(Options{}).finishCleanupAccess(t.Context(), rc); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatal("owned access intent crossed shared TLS rotation fence", err)
	}
	if pending, err := loadAccessIntent(t.Context(), rc); err != nil || pending == nil {
		t.Fatal("shared TLS fence discarded access intent", err)
	}
}

func TestCleanupSupersedesGaleraTLSAfterRuntimeRemoval(t *testing.T) {
	for _, keep := range []bool{false, true} {
		for _, stage := range []string{"fence", "cleanup"} {
			t.Run(stage+map[bool]string{false: "-delete", true: "-suspend"}[keep], func(t *testing.T) {
				p, rc, b := setup(t, "group")
				converge(t, p, rc, b)
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				s.GaleraTLS = &galeraTLSRotation{Stage: stage}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				for _, file := range galeraTrafficFiles {
					active := "old" + file.suffix
					if stage == "cleanup" {
						active = "new" + file.suffix
					}
					if err := putSecret(t.Context(), rc, file.active, []byte(active)); err != nil {
						t.Fatal(err)
					}
					// Simulate a crash after removing the first pending file.
					if stage == "cleanup" && file.suffix == ".ca.key" {
						continue
					}
					if err := putSecret(t.Context(), rc, galeraTrafficPending+file.suffix, []byte("new"+file.suffix)); err != nil {
						t.Fatal(err)
					}
				}
				done := false
				for range 80 {
					done, err = New(Options{}).Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: keep})
					if err != nil {
						t.Fatal(err)
					}
					if stage == "fence" {
						for _, m := range s.Members {
							if b.services[m.Name] != nil {
								for _, file := range galeraTrafficFiles {
									raw, err := secret(t.Context(), rc, file.active)
									if err != nil || string(raw) != "old"+file.suffix {
										t.Fatal("traffic bundle changed before all runtimes disappeared", err)
									}
								}
							}
						}
					}
					if done {
						break
					}
				}
				s, err = load(rc.Cluster)
				if err != nil || !done || s.GaleraTLS != nil {
					t.Fatal("TLS cleanup journal did not converge", err)
				}
				for _, file := range galeraTrafficFiles {
					if _, err := secret(t.Context(), rc, galeraTrafficPending+file.suffix); !missing(err) {
						t.Fatal("pending traffic secret retained", err)
					}
					raw, err := secret(t.Context(), rc, file.active)
					if keep {
						if err != nil || string(raw) != "new"+file.suffix {
							t.Fatal("suspension lost completed common traffic bundle", err)
						}
					} else if !missing(err) {
						t.Fatal("deletion retained traffic secret", err)
					}
				}
			})
		}
	}
}
