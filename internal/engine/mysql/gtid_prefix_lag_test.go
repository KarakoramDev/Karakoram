// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"net/http"
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
	if err := establishGTIDBaseline(t.Context(), rc, s, full); err != nil {
		t.Fatal(err)
	}
	rc.Clock = trafficRenewalClock{at: start.Add(2 * time.Second)}
	pass := engine.WithObservationPass(t.Context())
	if err := checkPXCQuorumGTIDs(pass, rc, s); err != nil {
		t.Fatalf("first in-flight sample immediately latched: %v", err)
	}
	safe := s.LastGTIDSafeAt
	rc.Clock = trafficRenewalClock{at: start.Add(20 * time.Second)}
	if err := checkPXCQuorumGTIDs(pass, rc, s); err != nil {
		t.Fatalf("one cached sweep counted as multiple passes: %v", err)
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
