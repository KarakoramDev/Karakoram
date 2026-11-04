// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPXCRestartedMixedAccountingCannotBecomePermanentLag(t *testing.T) {
	const direct = "d5aeb882-d447-11f1-96e2-f732d65208d9"
	const inverse = "2a51477d-2bb8-ee0e-691d-08cd29adf726"
	const historical = "e419e04e-d43f-11f1-835a-d377c0fbcfd8:1-227"
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	bad := s.data()[2].Name
	positionsEqual, healed := true, false
	prior := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path != "/v1/mysql/galera-status" {
			return prior != nil && prior(w, r, name)
		}
		set, position := direct+":1-1134,"+historical, int64(1143)
		if name == bad && !healed {
			set = inverse + ":1-534," + direct + ":1-600," + historical
			if !positionsEqual {
				position--
			}
		}
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, ClusterSize: 3, StateUUID: direct, GTIDExecuted: set, ServerUUID: b.native[name].ServerUUID, LastCommitted: position})
		return true
	}
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err == nil {
		t.Fatal("native frozen inverse divergence was not latched")
	}
	start := activeGTIDIncident(s).DetectedAt.Add(time.Second)
	full := &domain.Backup{Name: "ack", Type: "full", Phase: domain.PhaseSucceeded, StartedAt: &start, MySQL: &domain.MySQLBackup{EndTime: start.Add(time.Second)}}
	if err := establishGTIDBaseline(t.Context(), rc, s, full); err == nil {
		t.Fatal("fresh full acknowledged the native mixed accounting")
	}
	if activeGTIDIncident(s) == nil {
		t.Fatal("rejected acknowledgement erased the incident")
	}
	// Older controllers could already have persisted this unsafe allowance.
	// Load that historical state explicitly: the current acknowledgement path
	// must reject it, while legacy records still require the prefix-offset guard.
	observations := observePXCQuorumGTIDs(t.Context(), rc, s)
	incident := activeGTIDIncident(s)
	incident.BaselineAt, incident.Backup = full.MySQL.EndTime, "legacy-ack"
	incident.Acknowledged = observations
	incident.Allowance = &gtidAllowance{Epoch: s.Formation.FormationEpoch, Group: s.Formation.GroupName, Members: gtidMemberIdentities(s, observations), Histories: observations}
	if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	rc.Clock = trafficRenewalClock{at: start.Add(2 * time.Second)}
	pass := engine.WithObservationPass(t.Context())
	if err, ok := checkPXCQuorumGTIDs(pass, rc, s).(*myspec.GTIDQuorumError); !ok || err.Diverged {
		t.Fatalf("first in-flight sample did not remain Unknown: %v", err)
	}
	safe := s.LastGTIDSafeAt
	rc.Clock = trafficRenewalClock{at: start.Add(20 * time.Second)}
	if err, ok := checkPXCQuorumGTIDs(pass, rc, s).(*myspec.GTIDQuorumError); !ok || err.Diverged {
		t.Fatalf("one cached sweep did not remain Unknown: %v", err)
	}
	s, _ = load(rc.Cluster)
	if err := loadGTIDJournal(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if s.GTIDPrefixLag == nil || s.GTIDPrefixLag.Samples != 1 || !s.LastGTIDSafeAt.Equal(safe) {
		t.Fatal("suspicion did not survive reload or advanced safe watermark")
	}
	if err := checkPXCQuorumGTIDs(engine.WithObservationPass(t.Context()), rc, s); err == nil {
		t.Fatal("persistent equal-position active prefix mismatch was accepted as lag")
	}
	s, _ = load(rc.Cluster)
	if err := loadGTIDJournal(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if activeGTIDIncident(s) == nil || len(s.GTIDIncidents) != 2 {
		t.Fatal("persistent native accounting mismatch did not latch durably")
	}
	// Later real equality cannot erase the incident.
	healed = true
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if activeGTIDIncident(s) == nil {
		t.Fatal("healing silently cleared the durable incident")
	}
}

func TestPXCEqualPositionLagRequiresIndependentPersistentEvidence(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	observed := map[string]myspec.GaleraStatusResponse{}
	for _, m := range s.data() {
		observed[m.Name] = myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: sid, LastCommitted: 20, GTIDExecuted: sid + ":1-10", ServerUUID: b.native[m.Name].ServerUUID}
	}
	bad := s.data()[2].Name
	lag := observed[bad]
	lag.GTIDExecuted = sid + ":1-9"
	observed[bad] = lag
	at := time.Unix(100, 0)
	if conflict, suspected, _ := observePXCEqualPositionLag(s, observed, at, true); conflict != nil || !suspected {
		t.Fatal("initial lag not held as in-flight")
	}
	lag.GTIDExecuted = sid + ":1-10"
	observed[bad] = lag
	if conflict, suspected, _ := observePXCEqualPositionLag(s, observed, at.Add(time.Second), true); conflict != nil || suspected || s.GTIDPrefixLag != nil {
		t.Fatal("caught-up lag remained suspected")
	}
	lag.GTIDExecuted = sid + ":1-9"
	lag.LastCommitted = 19
	observed[bad] = lag
	if conflict, suspected, _ := observePXCEqualPositionLag(s, observed, at.Add(time.Minute), true); conflict != nil || suspected {
		t.Fatal("different wsrep positions were treated as permanent lag")
	}
}

// Model the restart/continuous-writer sequence: the same member is one
// transaction behind in repeated torn equal-wsrep-position samples, but both
// the writer and the restarting member are making progress between sweeps.
func TestPXCRolloutMovingPrefixLagDoesNotLatch(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	for _, change := range []string{"position-and-history", "history-only", "position-only"} {
		t.Run(change, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			lagging := s.data()[0].Name
			position, head, tail := int64(900), 893, 892
			restarting, healed := false, false
			prior := b.action
			b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
				if r.URL.Path != "/v1/mysql/galera-status" {
					return prior != nil && prior(w, r, name)
				}
				end := head
				if name == lagging && !healed {
					end = tail
				}
				status := myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, ClusterSize: 3, StateUUID: sid, LastCommitted: position, GTIDExecuted: fmt.Sprintf("%s:1-%d", sid, end), ServerUUID: b.native[name].ServerUUID}
				if name == lagging && restarting {
					status.Ready = false
					status.LocalStateComment = "Joining"
				}
				_ = json.NewEncoder(w).Encode(status)
				return true
			}
			at := now(rc).Add(time.Minute)
			// Start with a complete healthy boundary before the restart.
			healed = true
			rc.Clock = trafficRenewalClock{at: at}
			if err := checkPXCQuorumGTIDs(engine.WithObservationPass(t.Context()), rc, s); err != nil {
				t.Fatal(err)
			}
			safe := s.LastGTIDSafeAt
			restarting, healed = true, false
			rc.Clock = trafficRenewalClock{at: at.Add(time.Second)}
			_ = checkPXCQuorumGTIDs(engine.WithObservationPass(t.Context()), rc, s)
			restarting = false
			for i := range 3 {
				rc.Clock = trafficRenewalClock{at: at.Add(time.Duration(2+i*10) * time.Second)}
				pass := engine.WithObservationPass(t.Context())
				if err, ok := checkPXCQuorumGTIDs(pass, rc, s).(*myspec.GTIDQuorumError); !ok || err.Diverged || !err.PrefixInFlight {
					t.Fatalf("moving prefix lag did not remain Unknown: %v", err)
				}
				if activeGTIDIncident(s) != nil || !s.LastGTIDSafeAt.Equal(safe) {
					t.Fatal("moving prefix lag latched or advanced continuity")
				}
				if err := agentUpgradeGate(pass, rc, s, s.data()[2]); err != nil {
					t.Fatalf("compatible catch-up blocked rollout: %v", err)
				}
				// Reload the journal too: progress must invalidate persisted suspicion.
				s, _ = load(rc.Cluster)
				if change != "history-only" {
					position++
				}
				if change != "position-only" {
					head++
					tail++
				}
			}
			healed = true
			rc.Clock = trafficRenewalClock{at: at.Add(time.Minute)}
			if err := checkPXCQuorumGTIDs(engine.WithObservationPass(t.Context()), rc, s); err != nil {
				t.Fatal(err)
			}
			if activeGTIDIncident(s) != nil || !s.LastGTIDSafeAt.After(safe) || s.GTIDPrefixLag != nil {
				t.Fatal("caught-up member failed to restore continuity")
			}
		})
	}
}

func TestPXCRolloutStalePrefixSuspicionCannotBypassObservationFailure(t *testing.T) {
	for _, failure := range []string{"observation-lock", "journal-cancellation"} {
		t.Run(failure, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			s, _ := load(rc.Cluster)
			observed := observePXCQuorumGTIDs(t.Context(), rc, s)
			lagging := s.data()[0].Name
			lag := observed[lagging]
			lag.GTIDExecuted = lag.StateUUID + ":1-9"
			for name, status := range observed {
				status.GTIDExecuted = lag.StateUUID + ":1-10"
				status.LastCommitted = 20
				observed[name] = status
			}
			lag.LastCommitted = 20
			observed[lagging] = lag
			if _, suspected, _ := observePXCEqualPositionLag(s, observed, now(rc), true); !suspected {
				t.Fatal("failed to establish earlier prefix suspicion")
			}
			if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
				t.Fatal(err)
			}
			s, _ = load(rc.Cluster)
			if s.GTIDPrefixLag == nil {
				t.Fatal("prefix suspicion did not survive reload")
			}
			ctx, cancel := context.WithCancel(engine.WithObservationPass(t.Context()))
			defer cancel()
			// Keep native readiness healthy and cached, even if this invocation
			// later loses the journal-backed GTID comparison.
			for _, m := range s.data() {
				status, err := galeraStatus(ctx, rc, m.Name)
				if err != nil || !status.Eligible() || status.ClusterSize != len(s.data()) {
					t.Fatalf("native readiness unavailable: %v", err)
				}
			}
			if failure == "observation-lock" {
				lock := gtidObservationMutex(rc)
				lock.Lock()
				defer lock.Unlock()
			} else {
				rc.Secrets = quorumFailingHistoryStore{SecretStore: rc.Secrets, mode: "cancel", onHistoryRead: cancel}
			}
			err := agentUpgradeGate(ctx, rc, s, s.data()[2])
			var comparison *myspec.GTIDQuorumError
			if !errors.As(err, &comparison) || comparison.Diverged || comparison.PrefixInFlight {
				t.Fatalf("stale suspicion bypassed unavailable comparison: %v", err)
			}
			if failure == "observation-lock" && !strings.Contains(comparison.Detail, "observation-lock") {
				t.Fatalf("wrong observation failure: %v", err)
			}
			if failure == "journal-cancellation" && !errors.Is(err, context.Canceled) {
				t.Fatalf("lost pre-authoritative cancellation: %v", err)
			}
		})
	}
}
