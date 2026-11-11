// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"encoding/pem"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

type quorumObservationContextKey struct{}

func quorumObservationFixture(t *testing.T, handler http.HandlerFunc) (*engine.Context, *state) {
	t.Helper()
	server := httptest.NewTLSServer(handler)
	t.Cleanup(server.Close)
	rc := &engine.Context{
		Cluster: &domain.Cluster{Namespace: "default", Name: "quorum", Generation: 1},
		Agents:  fakeAgents{url: server.URL, ca: pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: server.Certificate().Raw})},
	}
	s := &state{}
	// Deliberately unsorted: scheduling must not influence member/result mapping.
	for _, name := range []string{"mysql2", "mysql0", "mysql1"} {
		s.Members = append(s.Members, &member{Name: name, Role: "data", ServiceID: name + "-id", Stage: "joined"})
	}
	return rc, s
}

func lockedQuorumSample(ctx context.Context, rc *engine.Context, s *state, fresh bool) (map[string]myspec.GaleraStatusResponse, time.Time) {
	mu := gtidObservationMutex(rc)
	mu.Lock()
	defer mu.Unlock()
	return samplePXCQuorumGTIDs(ctx, rc, s, fresh, nil)
}

func TestPXCQuorumObservationPassAndFresh(t *testing.T) {
	var calls, version atomic.Int32
	version.Store(1)
	rc, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/v1/mysql/galera-status" {
			t.Errorf("unexpected action: %s", r.URL.Path)
		}
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterSize: int(version.Load())})
	})
	ctx := engine.WithObservationPass(t.Context())
	first, at := lockedQuorumSample(ctx, rc, s, false)
	version.Store(2)
	// Sync, Status, PITRWindow and the Degraded retry all use this entry point.
	for range 9 {
		got, sampledAt := lockedQuorumSample(context.WithValue(ctx, quorumObservationContextKey{}, true), rc, s, false)
		if got["mysql0"].ClusterSize != 1 || !sampledAt.Equal(at) {
			t.Fatal("cache hit changed sample or timestamp")
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("calls per pass = %d, want one triplet", calls.Load())
	}
	fresh, _ := lockedQuorumSample(ctx, rc, s, true)
	if fresh["mysql0"].ClusterSize != 2 || first["mysql0"].ClusterSize != 1 || calls.Load() != 6 {
		t.Fatal("fresh bypass did not take an independent sample")
	}
	got, _ := lockedQuorumSample(ctx, rc, s, false)
	if got["mysql0"].ClusterSize != 2 || calls.Load() != 6 {
		t.Fatal("fresh sample did not replace cache")
	}
	lockedQuorumSample(engine.WithObservationPass(ctx), rc, s, false)
	if calls.Load() != 9 {
		t.Fatal("new pass inherited the previous pass")
	}
	for _, change := range []func(){
		func() { rc.Cluster.Name = "other" },
		func() { rc.Cluster.Namespace = "other" },
		func() { rc.Cluster.Generation++ },
		func() { s.Formation.FormationEpoch++ },
		func() { s.Formation.GroupName = "new-group" },
		func() { s.Members[0].ServiceID = "replacement" },
		func() { s.Members[0].VolumeID = "replacement" },
		func() { s.Members[0].ServerUUID = "replacement" },
		func() { s.RestoreGTIDFence = "restore" },
		func() {
			rc.Operation = &domain.Operation{TargetGeneration: 2, Checkpoint: json.RawMessage(`{"step":"activate"}`)}
		},
		func() { rc.Operation.Checkpoint = json.RawMessage(`{"step":"done"}`) },
	} {
		before := calls.Load()
		change()
		lockedQuorumSample(ctx, rc, s, false)
		if calls.Load() != before+3 {
			t.Fatal("changed cluster/formation/member/restore identity reused sample")
		}
	}
}

func TestPXCQuorumObservationConcurrentCancellation(t *testing.T) {
	var active, peak, calls atomic.Int32
	release := make(chan struct{})
	rc, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		n := active.Add(1)
		defer active.Add(-1)
		for old := peak.Load(); n > old; old = peak.Load() {
			if peak.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	// Cleanup is LIFO: release blocked handlers before fixture server.Close.
	t.Cleanup(func() { close(release) })
	// A shared deadline exercises the real cancellation-aware HTTP agent path
	// without waiting four seconds for each built-in per-action timeout.
	ctx, cancel := context.WithTimeout(engine.WithObservationPass(t.Context()), 300*time.Millisecond)
	defer cancel()
	started := time.Now()
	got, _ := lockedQuorumSample(ctx, rc, s, false)
	if elapsed := time.Since(started); elapsed > 800*time.Millisecond {
		t.Fatalf("canceled quorum took %s", elapsed)
	}
	if calls.Load() != 3 || peak.Load() != 3 || len(got) != 3 {
		t.Fatalf("calls=%d peak=%d results=%d, want three concurrent members", calls.Load(), peak.Load(), len(got))
	}
	for name, status := range got {
		if status != (myspec.GaleraStatusResponse{}) {
			t.Fatalf("failed member %s returned a successful status", name)
		}
	}
	lockedQuorumSample(ctx, rc, s, false)
	if calls.Load() != 3 {
		t.Fatal("already canceled context issued agent actions")
	}
}

func TestPXCQuorumObservationConcurrentTimeout(t *testing.T) {
	var calls atomic.Int32
	var respond atomic.Bool
	release := make(chan struct{})
	rc, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if respond.Load() {
			_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterSize: 3})
			return
		}
		select {
		case <-r.Context().Done():
		case <-release:
		}
	})
	t.Cleanup(func() { close(release) })
	ctx := engine.WithObservationPass(t.Context())
	started := time.Now()
	got, _ := lockedQuorumSample(ctx, rc, s, false)
	elapsed := time.Since(started)
	if elapsed < engine.ObservationTimeout || elapsed >= 2*engine.ObservationTimeout {
		t.Fatalf("quorum timeout took %s, want approximately one %s timeout, not three", elapsed, engine.ObservationTimeout)
	}
	if calls.Load() != 3 || len(got) != 3 {
		t.Fatalf("calls=%d results=%d, want three members", calls.Load(), len(got))
	}
	started = time.Now()
	for range 9 {
		lockedQuorumSample(ctx, rc, s, false)
	}
	if calls.Load() != 3 || time.Since(started) > 300*time.Millisecond {
		t.Fatal("exhausted read budget issued more actions or waited")
	}
	respond.Store(true)
	got, _ = lockedQuorumSample(ctx, rc, s, true)
	if calls.Load() != 6 || got["mysql0"].ClusterSize != 3 {
		t.Fatal("exhausted read budget suppressed fresh write-relevant probe")
	}
	lockedQuorumSample(ctx, rc, s, false)
	if calls.Load() != 6 {
		t.Fatal("fresh successful sample was not reusable after budget exhaustion")
	}
	lockedQuorumSample(engine.WithObservationPass(ctx), rc, s, false)
	if calls.Load() != 9 {
		t.Fatal("read budget leaked into a new pass")
	}
}

func TestPXCQuorumObservationTransportFailureNotCached(t *testing.T) {
	var calls atomic.Int32
	var fail atomic.Bool
	rc, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if fail.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterSize: 3})
	})
	ctx := engine.WithObservationPass(t.Context())
	lockedQuorumSample(ctx, rc, s, false)
	fail.Store(true)
	lockedQuorumSample(ctx, rc, s, true)
	fail.Store(false)
	got, _ := lockedQuorumSample(ctx, rc, s, false)
	if calls.Load() != 9 || got["mysql0"].ClusterSize != 3 {
		t.Fatal("failed fresh sample retained stale cache or cached transport failure")
	}
}

func TestPXCQuorumObservationRestoreCheckpointIsolation(t *testing.T) {
	var calls atomic.Int32
	rc, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterSize: 3})
	})
	ctx := engine.WithObservationPass(t.Context())
	lockedQuorumSample(ctx, rc, s, false)
	// Recovery Status/Sync may have no rc.Operation: bind the durable owner too.
	for i, backup := range []backupState{
		{Generation: 1},
		{Generation: 1, RestoreOperation: &domain.Operation{ID: "restore", TargetGeneration: 1, Checkpoint: json.RawMessage(`{"step":"certify"}`)}},
		{Generation: 1, RestoreOperation: &domain.Operation{ID: "restore", TargetGeneration: 1, Checkpoint: json.RawMessage(`{"step":"activate"}`)}},
		{Generation: 1, RestoreOperation: &domain.Operation{ID: "restore", TargetGeneration: 2, Checkpoint: json.RawMessage(`{"step":"activate"}`)}},
		{Generation: 1, RestoreOperation: &domain.Operation{ID: "next-restore", TargetGeneration: 2, Checkpoint: json.RawMessage(`{"step":"activate"}`)}},
	} {
		if err := saveBackupState(rc.Cluster, backup); err != nil {
			t.Fatal(err)
		}
		lockedQuorumSample(ctx, rc, s, false)
		if calls.Load() != int32(3*(i+2)) {
			t.Fatal("durable restore owner/checkpoint/generation reused old sample")
		}
	}
}

func TestPXCQuorumObservationTimestampAtSweepStart(t *testing.T) {
	requestTimes := make(chan time.Time, 3)
	rc, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		requestTimes <- time.Now()
		time.Sleep(20 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterSize: 3})
	})
	_, at := lockedQuorumSample(engine.WithObservationPass(t.Context()), rc, s, false)
	for range 3 {
		if at.After(<-requestTimes) {
			t.Fatal("safe watermark used completion time instead of sweep start")
		}
	}
}

func TestPXCQuorumObservationFastFailureCanRetry(t *testing.T) {
	var calls atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	rc, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if fail.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterSize: 3})
	})
	ctx := engine.WithObservationPass(t.Context())
	lockedQuorumSample(ctx, rc, s, false)
	fail.Store(false)
	got, _ := lockedQuorumSample(ctx, rc, s, false)
	if calls.Load() != 6 || got["mysql0"].ClusterSize != 3 {
		t.Fatal("fast incomplete sweep consumed entire read budget or cached failure")
	}
	lockedQuorumSample(ctx, rc, s, false)
	if calls.Load() != 6 {
		t.Fatal("recovered sample was not memoized")
	}
}

func TestPXCQuorumObservationRetryRetainsEarliestSafeBoundary(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	const extra = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	var calls atomic.Int32
	var recovered, diverged atomic.Bool
	probe, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		minority := r.Header.Get("Authorization") == "Bearer mysql2"
		if minority && !recovered.Load() {
			http.Error(w, "temporarily unavailable", http.StatusServiceUnavailable)
			return
		}
		status := myspec.GaleraStatusResponse{
			ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true,
			ClusterSize: 3, StateUUID: sid, GTIDExecuted: sid + ":1-7",
		}
		if minority && diverged.Load() {
			status.GTIDExecuted += "," + extra + ":1"
		}
		_ = json.NewEncoder(w).Encode(status)
	})
	_, rc, _ := setup(t, "cluster")
	rc.Cluster.Spec.Provider, rc.Agents = "pxc", probe.Agents
	// Start with the actual persisted member/formation identity. Saving the
	// first journal must not itself change identity and force all reads fresh.
	s.UID, s.ConfigVersion, s.Topology = "quorum", 1, myspec.TopologyCluster
	s.Formation = formation{State: "formed", FormationEpoch: 1, GroupName: sid}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	base := time.Now().UTC()
	rc.Clock = trafficRenewalClock{at: base}
	ctx := engine.WithObservationPass(t.Context())
	_ = checkPXCQuorumGTIDs(ctx, rc, s)
	if calls.Load() != 3 || !s.LastGTIDSafeAt.IsZero() {
		t.Fatal("incomplete initial sweep established a safety boundary")
	}

	// Only the failed member is retried. Two successful payloads still prove
	// the old instant, not the newer clock at the start of this retry.
	recovered.Store(true)
	rc.Clock = trafficRenewalClock{at: base.Add(time.Minute)}
	if err := checkPXCQuorumGTIDs(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 4 || !s.LastGTIDSafeAt.Equal(base) {
		t.Fatalf("mixed-age quorum advanced safety: calls=%d safe=%s want=%s", calls.Load(), s.LastGTIDSafeAt, base)
	}

	// A subsequent real incident must inherit that conservative boundary; PITR
	// must not gain the minute between the old responses and the retry.
	diverged.Store(true)
	rc.Clock = trafficRenewalClock{at: base.Add(2 * time.Minute)}
	if err := checkPXCQuorumGTIDs(engine.WithObservationPass(ctx), rc, s); err == nil {
		t.Fatal("new unshared SID did not latch divergence")
	}
	if incident := activeGTIDIncident(s); incident == nil || !incident.SafeThrough.Equal(base) {
		t.Fatalf("incident lost original sampling boundary: %+v", incident)
	}
}

func TestPXCStoppedRetirementSharedObservationPass(t *testing.T) {
	for _, checkpoint := range []string{"stop", "delete", "credentials"} {
		t.Run(checkpoint, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			s, err := load(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			retiree := s.data()[2]
			receipt, err := b.Stop(t.Context(), retiree.Name)
			if err != nil {
				t.Fatal(err)
			}
			retiree.Removing, retiree.Offline = true, true
			retiree.Retirement, retiree.RetirementOp = checkpoint, &receipt
			delete(b.native, retiree.Name)
			delete(b.deployed, retiree.Name)
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			rc.Backend = rollbackContextBackend{Backend: rc.Backend}
			rc.Agents = stoppedRollbackAgents{AgentDialer: rc.Agents, stopped: retiree.Name}
			pass := engine.WithObservationPass(t.Context())
			_, _ = p.Status(pass, rc)
			if _, err := p.Sync(pass, rc); err != nil {
				t.Fatalf("quorum pre-observation starved retirement: %v", err)
			}
			current, _ := load(rc.Cluster)
			if m := current.find(retiree.Name); m != nil && m.Retirement == checkpoint && current.Pending == nil {
				t.Fatal("retirement made no progress")
			}
			// Unavailable retirees remain in the comparison's membership, not
			// an invented eligible sample or a smaller quorum denominator.
			outcomes := make(map[string]string)
			mu := gtidObservationMutex(rc)
			mu.Lock()
			observations, _ := samplePXCQuorumGTIDs(engine.WithObservationPass(t.Context()), rc, s, false, outcomes)
			mu.Unlock()
			if len(observations) != 3 || observations[retiree.Name].Eligible() || outcomes[retiree.Name] != "no-observation" {
				t.Fatalf("retiree changed quorum semantics: observations=%d eligible=%v outcome=%s", len(observations), observations[retiree.Name].Eligible(), outcomes[retiree.Name])
			}
			observations[s.data()[0].Name] = myspec.GaleraStatusResponse{}
			if err := myspec.CheckGTIDQuorum(observations); err == nil {
				t.Fatal("unknown retired member reduced the quorum denominator")
			}
		})
	}
}

func TestPXCStoppedRetirementFreshQuorumReads(t *testing.T) {
	var calls atomic.Int32
	rc, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: recoverySID, GTIDExecuted: recoverySID + ":1-10"})
	})
	retiree := s.data()[0]
	retiree.Removing, retiree.Retirement = true, "stop"
	observations, _ := lockedQuorumSample(t.Context(), rc, s, false)
	if len(observations) != 3 || observations[retiree.Name].Eligible() || calls.Load() != 2 {
		t.Fatal("read-only quorum sampling contacted the stopped retiree")
	}
	observations, _ = lockedQuorumSample(t.Context(), rc, s, true)
	if !observations[retiree.Name].Eligible() || calls.Load() != 5 {
		t.Fatal("read-only skip changed authoritative fresh quorum sampling")
	}
}
