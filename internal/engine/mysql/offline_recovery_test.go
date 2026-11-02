// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestOfflineRecoverySlowShutdown(t *testing.T) {
	for _, topology := range []string{"group", "cluster"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, _ := setup(t, topology)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ := load(rc.Cluster)
			s.Formation.State = "formed"
			s.Formation.Reformation = &reformation{State: "observing"}
			for _, m := range s.data() {
				m.Stage = "deployed"
				m.Offline = true
				m.ServerUUID = sid
			}
			seen := make(chan string, 3)
			release := make(chan struct{})
			installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				if r.URL.Path == "/v1/status" {
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.find(name).ConfigVersion})
					return
				}
				seen <- name
				if name == "demom0" {
					select {
					case <-release:
					case <-r.Context().Done():
						return
					}
				}
				if topology == "group" {
					json.NewEncoder(w).Encode(myspec.OfflineGTIDResponse{Stopped: true, ServerUUID: sid, GTIDExecuted: sid + ":1-9", DataIdentity: myspec.DataIdentity{Initialized: true}})
				} else {
					json.NewEncoder(w).Encode(myspec.WSRepRecoverResponse{UUID: sid, Seqno: 9, Stopped: true})
				}
			})
			ctx := engine.WithObservationPass(t.Context())
			// Start the read-only pass budget before recovery; it must not cap the actor.
			read, cancel := mysqlReadContext(ctx, rc)
			defer cancel()
			_ = read
			done := make(chan error, 1)
			go func() { _, err := p.reconcileFormation(ctx, rc, s); done <- err }()
			for range 3 {
				select {
				case <-seen:
				case <-time.After(5 * time.Second):
					close(release)
					t.Fatal("slow member starved another probe")
				}
			}
			timer := time.NewTimer(10 * time.Second)
			defer timer.Stop()
			<-timer.C
			close(release)
			if err := <-done; err != nil {
				t.Fatal(err)
			}
			if s.Formation.Reformation.State != "bootstrapping" || s.Formation.Seed != "demom0" {
				t.Fatalf("unsafe/blocked formation: %+v", s.Formation)
			}
		})
	}
}

func TestOfflineRecoveryFailureObservesAllAndRetries(t *testing.T) {
	for _, topology := range []string{"group", "cluster"} {
		t.Run(topology, func(t *testing.T) {
			p, rc, _ := setup(t, topology)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			s, _ := load(rc.Cluster)
			s.Formation.State = "formed"
			s.Formation.Reformation = &reformation{State: "observing"}
			for _, m := range s.data() {
				m.Stage = "deployed"
				m.Offline = true
				m.ServerUUID = sid
			}
			var calls atomic.Int32
			var fail atomic.Bool
			fail.Store(true)
			installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
				name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				if r.URL.Path == "/v1/status" {
					json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: s.find(name).ConfigVersion})
					return
				}
				calls.Add(1)
				if name == "demom0" && fail.Load() {
					http.Error(w, "unavailable", 503)
					return
				}
				if topology == "group" {
					json.NewEncoder(w).Encode(myspec.OfflineGTIDResponse{Stopped: true, ServerUUID: sid, GTIDExecuted: sid + ":1-9", DataIdentity: myspec.DataIdentity{Initialized: true}})
				} else {
					json.NewEncoder(w).Encode(myspec.WSRepRecoverResponse{UUID: sid, Seqno: 9, Stopped: true})
				}
			})
			if _, err := p.reconcileFormation(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 3 || s.Formation.Reformation.State != "observing" || !strings.Contains(s.Formation.Reformation.Blocked, "demom0") {
				t.Fatalf("calls=%d formation=%+v", calls.Load(), s.Formation)
			}
			fail.Store(false)
			if _, err := p.reconcileFormation(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 3 {
				t.Fatal("retry bypassed provider backoff")
			}
			rc.Clock = trafficRenewalClock{at: time.Now().Add(16 * time.Second)}
			if _, err := p.reconcileFormation(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 6 || s.Formation.Reformation.State != "bootstrapping" {
				t.Fatalf("retry failed: %+v", s.Formation)
			}
		})
	}
}

func TestOfflineRecoveryIdentityMismatchBlocks(t *testing.T) {
	p, rc, b := setup(t, "group")
	converge(t, p, rc, b)
	suspendGroup(t, p, rc, b)
	out := b.offline["demom1"]
	out.ServerUUID = sid2
	b.offline["demom1"] = out
	for range 30 {
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
	}
	s, _ := load(rc.Cluster)
	if s.Formation.Reformation.State != "observing" || !strings.Contains(s.Formation.Reformation.Blocked, "demom1") || b.bootstrap[2] != 0 {
		t.Fatalf("identity mismatch admitted: %+v", s.Formation)
	}
}

func TestOfflineRecoveryBudgetAndCancellation(t *testing.T) {
	_, rc, _ := setup(t, "group")
	m := &member{Name: "m0"}
	if got := offlineRecoveryTimeout(rc, m); got != time.Minute {
		t.Fatal(got)
	}
	for _, ram := range []float64{2, 2.5, 8, 10, 100} {
		m.Resources.MinRAM = ram
		if got := offlineRecoveryTimeout(rc, m); got != time.Minute {
			t.Fatalf("GR RAM=%v budget=%v", ram, got)
		}
		rc.Cluster.Spec.Topology.Type = "cluster"
		if got := offlineRecoveryTimeout(rc, m); got != 3*time.Minute {
			t.Fatalf("Galera RAM=%v budget=%v", ram, got)
		}
		rc.Cluster.Spec.Topology.Type = "group"
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, blocked := collectOfflineRecovery(ctx, rc, []*member{m}, func(ctx context.Context, _ *member) (bool, error) { <-ctx.Done(); return false, ctx.Err() })
	if !strings.Contains(blocked, "m0: offline recovery canceled") {
		t.Fatal(blocked)
	}
	_, blocked = collectOfflineRecovery(t.Context(), rc, []*member{m}, func(context.Context, *member) (bool, error) { return false, context.DeadlineExceeded })
	if !strings.Contains(blocked, "m0: offline recovery timed out") {
		t.Fatal(blocked)
	}
}

func TestOfflineRecoveryPrecheckAvoidsNativeWork(t *testing.T) {
	for _, topology := range []string{"group", "cluster"} {
		for _, cause := range []string{"not deployed", "not stopped"} {
			t.Run(topology+"/"+cause, func(t *testing.T) {
				p, rc, _ := setup(t, topology)
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				s, _ := load(rc.Cluster)
				s.Formation.State = "formed"
				s.Formation.Reformation = &reformation{State: "observing"}
				for _, m := range s.data() {
					m.Stage = "deployed"
					m.Offline = true
				}
				if cause == "not deployed" {
					s.find("demom0").Stage = "pending"
				}
				var calls atomic.Int32
				installAccessAgent(t, rc, func(w http.ResponseWriter, r *http.Request) {
					name := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
					if r.URL.Path != "/v1/status" {
						calls.Add(1)
						http.Error(w, "unexpected native work", 503)
						return
					}
					status := agentapi.Status{ConfigVersion: s.find(name).ConfigVersion}
					if cause == "not stopped" && name == "demom0" {
						status.Processes = map[string]agentapi.ProcessStatus{"mysqld": {PID: 1}}
					}
					json.NewEncoder(w).Encode(status)
				})
				if _, err := p.reconcileFormation(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				if calls.Load() != 0 || !strings.Contains(s.Formation.Reformation.Blocked, cause) {
					t.Fatalf("calls=%d blocked=%s", calls.Load(), s.Formation.Reformation.Blocked)
				}
			})
		}
	}
}

func TestOfflineRecoveryBackoffCapAndReset(t *testing.T) {
	p, rc, _ := setup(t, "group")
	s := &state{UID: "recovery-owner"}
	s.Formation.Reformation = &reformation{State: "observing", Blocked: "failed"}
	at := time.Now()
	rc.Clock = trafficRenewalClock{at: at}
	for _, delay := range []time.Duration{15 * time.Second, 30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 5 * time.Minute, 5 * time.Minute} {
		p.finishRecoverySweep(rc, s)
		if p.recoveryReady(rc, s) {
			t.Fatal("blocked retry admitted")
		}
		rc.Clock = trafficRenewalClock{at: at.Add(delay - time.Nanosecond)}
		if p.recoveryReady(rc, s) {
			t.Fatal("retry admitted early")
		}
		at = at.Add(delay)
		rc.Clock = trafficRenewalClock{at: at}
		if !p.recoveryReady(rc, s) {
			t.Fatal("retry not admitted at deadline")
		}
	}
	s.Formation.FormationEpoch++
	if !p.recoveryReady(rc, s) {
		t.Fatal("new epoch inherited backoff")
	}
	p.finishRecoverySweep(rc, s)
	s.Formation.Reformation.State = "bootstrapping"
	if !p.recoveryReady(rc, s) {
		t.Fatal("state change did not reset")
	}
	s.Formation.Reformation.State = "observing"
	if !p.recoveryReady(rc, s) {
		t.Fatal("state reset retained backoff")
	}
}
