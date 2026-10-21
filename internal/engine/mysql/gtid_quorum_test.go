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

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type gtidEvents struct{ events []domain.Event }

func (e *gtidEvents) Record(_ context.Context, event domain.Event) error {
	e.events = append(e.events, event)
	return nil
}

func TestPXCQuorumGTIDDurableLatchAndBaseline(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	const inverse = "eeeeeeee-dddd-cccc-bbbb-aaaaaaaaaaaa"
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	events := &gtidEvents{}
	rc.Events = events
	s, _ := load(rc.Cluster)
	for _, m := range s.data() {
		st := b.native[m.Name]
		st.GTIDExecuted = sid + ":1-7"
		b.native[m.Name] = st
	}
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if s.LastGTIDSafeAt.IsZero() {
		t.Fatal("safe watermark missing")
	}
	lastSafe := s.LastGTIDSafeAt
	bad := s.data()[2].Name
	st := b.native[bad]
	st.GTIDExecuted = sid + ":1-6," + inverse + ":1"
	b.native[bad] = st
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err == nil {
		t.Fatal("divergence accepted")
	}
	incident := activeGTIDIncident(s)
	if incident == nil || !incident.SafeThrough.Equal(lastSafe) {
		t.Fatal("durable discontinuity missing")
	}
	// Restarts and later equal observations must not erase the incident.
	p = New(Options{Provider: "pxc"})
	s, _ = load(rc.Cluster)
	st.GTIDExecuted = sid + ":1-7"
	b.native[bad] = st
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if activeGTIDIncident(s) == nil {
		t.Fatal("equal histories erased incident")
	}
	if err := p.ValidateBackup(t.Context(), rc, "full"); err != nil {
		t.Fatalf("full backup blocked: %v", err)
	}
	if err := p.ValidateBackup(t.Context(), rc, "incremental"); err == nil {
		t.Fatal("incremental crossed incident")
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, c := range status.Conditions {
		if c.Type == "GTIDHistorySafe" {
			found = c.Status == "False" && strings.Contains(c.Message, bad) && strings.Contains(c.Message, "safe through")
		}
	}
	if !found {
		t.Fatal("persistent actionable condition missing")
	}
	// A full that started before detection cannot acknowledge the new boundary.
	oldStart := incident.DetectedAt.Add(-time.Second)
	full := &domain.Backup{Name: "new-base", Type: "full", Phase: domain.PhaseSucceeded, StartedAt: &oldStart, MySQL: &domain.MySQLBackup{EndTime: incident.DetectedAt.Add(time.Second)}}
	if err := establishGTIDBaseline(t.Context(), rc, s, full); err != nil {
		t.Fatal(err)
	}
	if activeGTIDIncident(s) == nil {
		t.Fatal("in-flight old full cleared incident")
	}
	start := incident.DetectedAt.Add(time.Second)
	full.StartedAt = &start
	full.MySQL.EndTime = start.Add(time.Second)
	if err := establishGTIDBaseline(t.Context(), rc, s, full); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if activeGTIDIncident(s) != nil || len(s.GTIDIncidents) != 1 {
		t.Fatal("fresh full did not clear while preserving boundary")
	}
	if len(events.events) != 2 || events.events[0].Reason != "GTIDHistoryDiverged" || events.events[1].Reason != "GTIDHistoryBaselineEstablished" {
		t.Fatalf("audit transitions: %v", events.events)
	}
	if !gtidContinuitySafe(s, full.MySQL.EndTime, full.MySQL.EndTime.Add(time.Second)) {
		t.Fatal("fresh baseline cannot continue")
	}
	if gtidContinuitySafe(s, lastSafe.Add(-time.Second), full.MySQL.EndTime) {
		t.Fatal("cleared latch admitted old crossing chain")
	}
}

func TestPXCContinuityWatermarkNotDetectionTime(t *testing.T) {
	safe, detected, baseline := time.Unix(10, 0), time.Unix(30, 0), time.Unix(40, 0)
	s := &state{GTIDIncidents: []gtidIncident{{SafeThrough: safe, DetectedAt: detected}}}
	base := time.Unix(5, 0)
	if !gtidContinuitySafe(s, base, safe) {
		t.Fatal("verified prepoint target rejected")
	}
	if gtidContinuitySafe(s, base, time.Unix(20, 0)) {
		t.Fatal("transaction/detection gap accepted")
	}
	if end := gtidWindowEnd(s, base, time.Unix(50, 0)); !end.Equal(safe) {
		t.Fatal(end)
	}
	s.GTIDIncidents[0].BaselineAt = baseline
	if gtidContinuitySafe(s, base, time.Unix(50, 0)) {
		t.Fatal("old chain crossed acknowledged discontinuity")
	}
	if !gtidContinuitySafe(s, baseline, time.Unix(50, 0)) {
		t.Fatal("new baseline rejected")
	}
	s.GTIDIncidents[0].SafeThrough = time.Time{}
	if !gtidWindowEnd(s, base, detected).IsZero() {
		t.Fatal("unknown unsafe interval became restorable")
	}
}

func TestMySQLDoesNotObservePXCQuorum(t *testing.T) {
	p, rc, b := setup(t, "single")
	converge(t, p, rc, b)
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/mysql/galera-status" {
			t.Error("MySQL requested PXC GTIDs")
		}
		return false
	}
	if err := p.ValidateBackup(t.Context(), rc, "incremental"); err != nil {
		t.Fatal(err)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range status.Conditions {
		if c.Type == "GTIDHistorySafe" {
			t.Fatal("PXC condition leaked into MySQL")
		}
	}
}

func TestPXCResumedIncrementalCannotBypassLatch(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	s.GTIDIncidents = []gtidIncident{{DetectedAt: time.Now(), Members: []string{s.data()[0].Name}}}
	if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	backup := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "guarded", Type: "incremental", Phase: domain.PhasePending}
	if err := rc.Store.CreateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Payload: json.RawMessage(`{"backup":"guarded"}`)}
	if _, err := p.runBackup(t.Context(), rc, op, backupCheckpoint{Step: "poll", Member: s.data()[0].Name}); err == nil || !strings.Contains(err.Error(), "GTID") {
		t.Fatalf("resumed incremental: %v", err)
	}
}

func TestPXCPITRPreservesSafeOldWindowAndNewBaseline(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	storage := &domain.BackupStorage{Namespace: "default", Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", Prefix: "prefix", VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage, rc.Cluster.Spec.Backup.PITR.Enabled = storage.Name, true
	s, _ := load(rc.Cluster)
	s.GTIDIncidents = []gtidIncident{{DetectedAt: time.Unix(30, 0), SafeThrough: time.Unix(20, 0), Members: []string{s.data()[2].Name}}}
	s.GTIDWindows = []domain.TimeWindow{{Start: time.Unix(10, 0), End: time.Unix(20, 0)}}
	if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	base := inventoryBackup(t, rc, "old-base", "full", "", 10, time.Unix(10, 0))
	base.Provider = "pxc"
	base.MySQL.Repository = repoPrefix(storage, rc.Cluster, s, 0)
	base.MySQL.ObjectPaths = []string{base.MySQL.Repository + "/base/old-base"}
	if err := rc.Store.UpdateBackup(t.Context(), &base); err != nil {
		t.Fatal(err)
	}
	oldAction := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/mysql/pitr-coverage" {
			var req myspec.PITRCoverageRequest
			if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
				t.Error(err)
				http.Error(w, "invalid coverage request", http.StatusBadRequest)
				return true
			}
			start := time.Unix(10, 0)
			if len(req.ObjectPaths) == 1 && strings.HasSuffix(req.ObjectPaths[0], "/fresh-base") {
				start = time.Unix(40, 0)
			}
			if err := json.NewEncoder(w).Encode(myspec.PITRCoverageResponse{Windows: []myspec.PITRWindow{{Start: start, End: time.Unix(50, 0)}}}); err != nil {
				t.Error(err)
			}
			return true
		}
		return oldAction(w, r, name)
	}
	if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(15, 0)); err != nil {
		t.Fatalf("safe historical target: %v", err)
	}
	if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(25, 0)); err == nil {
		t.Fatal("detection-gap target admitted")
	}
	newBase := inventoryBackup(t, rc, "fresh-base", "full", "", 20, time.Unix(40, 0))
	newBase.Provider = "pxc"
	newBase.MySQL.Repository = base.MySQL.Repository
	newBase.MySQL.ObjectPaths = []string{newBase.MySQL.Repository + "/base/fresh-base"}
	started := time.Unix(35, 0)
	newBase.StartedAt = &started
	if err := rc.Store.UpdateBackup(t.Context(), &newBase); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	if err := establishGTIDBaseline(t.Context(), rc, s, &newBase); err != nil {
		t.Fatal(err)
	}
	if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(45, 0)); err != nil {
		t.Fatalf("new baseline: %v", err)
	}
	if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(25, 0)); err == nil {
		t.Fatal("fresh full reopened unsafe historical gap")
	}
	if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(15, 0)); err != nil {
		t.Fatalf("old safe window disappeared: %v", err)
	}
	// Named immutable backups remain a recovery escape hatch without a live
	// source, even when that source has a latched GTID discontinuity.
	base.RecoveryIdentity = "sealed-old-base"
	if err := captureFixtureBackupAccess(t.Context(), rc, &base); err != nil {
		t.Fatal(err)
	}
	if err := rc.Store.UpdateBackup(t.Context(), &base); err != nil {
		t.Fatal(err)
	}
	rc.Agents = nil
	if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(15, 0)); err != nil {
		t.Fatalf("verified offline prepoint target blocked: %v", err)
	}
	if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(25, 0)); err == nil {
		t.Fatal("offline cache reopened unsafe gap")
	}
	if _, err := p.ValidatePITRRestore(t.Context(), rc, time.Unix(55, 0)); err == nil {
		t.Fatal("offline source invented new continuity")
	}
	if _, _, err := p.resolveRestore(t.Context(), rc, restoreSource{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Backup: base.Name}); err != nil {
		t.Fatalf("immutable offline backup restore blocked: %v", err)
	}
}

func TestPXCUnavailableMemberCannotAdvanceSafeWatermark(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	for _, m := range s.data() {
		status := b.native[m.Name]
		status.GTIDExecuted = sid + ":1-7"
		b.native[m.Name] = status
	}
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	safe := s.LastGTIDSafeAt
	rc.Clock = trafficRenewalClock{at: safe.Add(time.Hour)}
	bad := s.data()[2].Name
	priorAction := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path == "/v1/mysql/galera-status" && name == bad {
			http.Error(w, "observation unavailable", http.StatusServiceUnavailable)
			return true
		}
		return priorAction != nil && priorAction(w, r, name)
	}
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err != nil {
		t.Fatalf("healthy majority unavailable: %v", err)
	}
	if !s.LastGTIDSafeAt.Equal(safe) {
		t.Fatal("partial observations advanced the all-member safety watermark")
	}
	b.action = priorAction
	status := b.native[bad]
	status.GTIDExecuted = sid + ":1-6,eeeeeeee-dddd-cccc-bbbb-aaaaaaaaaaaa:1"
	b.native[bad] = status
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err == nil {
		t.Fatal("later observed conflict accepted")
	}
	if incident := activeGTIDIncident(s); incident == nil || !incident.SafeThrough.Equal(safe) {
		t.Fatal("unavailable interval became a permitted historical restore range")
	}
}

func TestPXCObservationsCannotReorderConflictBoundary(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	for _, m := range s.data() {
		status := b.native[m.Name]
		status.GTIDExecuted = sid + ":1-7"
		b.native[m.Name] = status
	}
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	safe := s.LastGTIDSafeAt
	rc.Clock = trafficRenewalClock{at: safe.Add(time.Minute)}
	newer := *rc
	newerCluster := *rc.Cluster
	newerCluster.Internal = append(json.RawMessage(nil), rc.Cluster.Internal...)
	newer.Cluster = &newerCluster
	newer.Clock = trafficRenewalClock{at: safe.Add(2 * time.Minute)}
	newerState, _ := load(newer.Cluster)
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	var calls atomic.Int32
	priorAction := b.action
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if r.URL.Path != "/v1/mysql/galera-status" {
			return priorAction != nil && priorAction(w, r, name)
		}
		call := calls.Add(1)
		status := myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, ClusterSize: 3, StateUUID: sid, GTIDExecuted: b.native[name].GTIDExecuted}
		if call == 1 {
			status.GTIDExecuted = sid + ":1-6,eeeeeeee-dddd-cccc-bbbb-aaaaaaaaaaaa:1"
		}
		if call == 2 {
			close(entered)
			<-release
		}
		if err := json.NewEncoder(w).Encode(status); err != nil {
			t.Error(err)
		}
		return true
	}
	olderDone := make(chan error, 1)
	go func() { olderDone <- checkPXCQuorumGTIDs(t.Context(), rc, s) }()
	<-entered
	mu := gtidObservationMutex(&newer)
	if mu.TryLock() {
		mu.Unlock()
		t.Fatal("an in-flight cluster observation is not serialized")
	}
	newerDone := make(chan error, 1)
	go func() { newerDone <- checkPXCQuorumGTIDs(t.Context(), &newer, newerState) }()
	close(release)
	if err := <-olderDone; err == nil {
		t.Fatal("older conflicting observation accepted")
	}
	if err := <-newerDone; err != nil {
		t.Fatalf("later equal observation: %v", err)
	}
	if incident := activeGTIDIncident(newerState); incident == nil || !incident.SafeThrough.Equal(safe) {
		t.Fatal("newer equal observation moved an older conflict's safe boundary")
	}
}

func TestPXCUnversionedWindowCacheIsNotReplayProof(t *testing.T) {
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, _ := load(rc.Cluster)
	windows := []domain.TimeWindow{{Start: time.Unix(10, 0), End: time.Unix(20, 0)}}
	raw, err := json.Marshal(gtidJournal{Windows: windows, Generation: 1})
	if err != nil {
		t.Fatal(err)
	}
	if err := putSecret(t.Context(), rc, "mysql/gtid/history", raw); err != nil {
		t.Fatal(err)
	}
	s.GTIDWindows, s.GTIDWindowGeneration = windows, 1
	if err := loadGTIDJournal(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.GTIDWindows) != 0 || s.GTIDWindowGeneration != 0 {
		t.Fatal("raw-union archive cache authorized offline replay")
	}
	s.GTIDWindows, s.GTIDWindowGeneration = windows, 1
	if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	s.GTIDWindows = nil
	if err := loadGTIDJournal(t.Context(), rc, s); err != nil {
		t.Fatal(err)
	}
	if len(s.GTIDWindows) != 1 || s.GTIDWindowGeneration != 1 {
		t.Fatal("current exact-lineage proof cache was not retained")
	}
}
