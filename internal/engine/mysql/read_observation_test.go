// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestMySQLReadObservationSingleFlight(t *testing.T) {
	var calls atomic.Int32
	rc, _ := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		time.Sleep(20 * time.Millisecond)
		_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 11})
	})
	ctx := engine.WithObservationPass(t.Context())
	var wg sync.WaitGroup
	times := make(chan time.Time, 12)
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			status, at, err := mysqlAgentStatus(ctx, rc, "mysql0")
			if err != nil || status.ConfigVersion != 11 {
				t.Errorf("metadata: %+v %v", status, err)
			}
			times <- at
		}()
	}
	wg.Wait()
	close(times)
	var first time.Time
	for at := range times {
		if first.IsZero() {
			first = at
		}
		if at.IsZero() || !at.Equal(first) {
			t.Fatal("single-flight consumers received different sample timestamps")
		}
	}
	if calls.Load() != 1 {
		t.Fatalf("single-flight issued %d agent calls", calls.Load())
	}
	scope := mysqlObservationScope(ctx, rc)
	scope.mu.Lock()
	scope.deadline = time.Now().Add(-time.Second)
	scope.mu.Unlock()
	status, at, err := mysqlAgentStatus(ctx, rc, "mysql0")
	if err != nil || status.ConfigVersion != 11 || !at.Equal(first) || calls.Load() != 1 {
		t.Fatal("expired network budget invalidated/relabelled successful same-pass sample")
	}
}

func TestMySQLReadObservationIsolationAndInvalidation(t *testing.T) {
	var calls, version atomic.Int32
	version.Store(1)
	rc, _ := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: int64(version.Load())})
	})
	ctx := engine.WithObservationPass(t.Context())
	check := func(ctx context.Context, wantVersion int64, wantCalls int32) {
		t.Helper()
		status, _, err := mysqlAgentStatus(ctx, rc, "mysql0")
		if err != nil || status.ConfigVersion != wantVersion || calls.Load() != wantCalls {
			t.Fatalf("version=%d calls=%d error=%v; want %d/%d", status.ConfigVersion, calls.Load(), err, wantVersion, wantCalls)
		}
	}
	check(ctx, 1, 1)
	version.Store(2)
	check(ctx, 1, 1)
	check(freshMySQLReads(ctx), 2, 2)
	check(normalMySQLReads(freshMySQLReads(ctx)), 1, 2)
	scope := mysqlObservationScope(ctx, rc)
	scope.inventory.Store("old-platform-fact", true)
	probe, stopProbe := mysqlReadContext(ctx, rc)
	defer stopProbe()
	actor, stopActor := mysqlReadContext(freshMySQLReads(probe), rc)
	defer stopActor()
	if infra.ObservationCache(probe) != &scope.inventory || infra.ObservationCache(actor) != nil {
		t.Fatal("fresh actor inherited the read-only platform snapshot")
	}
	invalidateMySQLReadObservation(ctx, rc)
	if _, ok := scope.inventory.Load("old-platform-fact"); ok {
		t.Fatal("mutation invalidation retained platform snapshot")
	}
	check(ctx, 2, 3)
	rc.Cluster.Generation++
	check(ctx, 2, 4)
	rc.Cluster.Name = "other"
	check(ctx, 2, 5)
	rc.Cluster.Namespace = "other"
	check(ctx, 2, 6)
	check(engine.WithObservationPass(ctx), 2, 7)
	client, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), "mysql0")
	if err != nil {
		t.Fatal(err)
	}
	for i, host := range []string{"instance-a", "instance-b"} {
		pinned, err := client.ForInstance(host)
		if err != nil {
			t.Fatal(err)
		}
		for range 2 {
			_, _, err := mysqlReadClientStatus(ctx, rc, "mysql0", pinned)
			if err != nil || calls.Load() != int32(8+i) {
				t.Fatalf("instance %s shared/repeated another instance's sample: %d %v", host, calls.Load(), err)
			}
		}
		pinned.Close()
	}
}

func TestMySQLReadObservationFailureCanRetry(t *testing.T) {
	var calls atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	rc, _ := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if fail.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 7})
	})
	ctx := engine.WithObservationPass(t.Context())
	if _, _, err := mysqlAgentStatus(ctx, rc, "mysql0"); err == nil {
		t.Fatal("failed agent read accepted")
	}
	fail.Store(false)
	status, _, err := mysqlAgentStatus(ctx, rc, "mysql0")
	if err != nil || status.ConfigVersion != 7 || calls.Load() != 2 {
		t.Fatal("fast transport failure was cached or consumed the complete budget")
	}
}

func TestMySQLReadObservationWaitCancellation(t *testing.T) {
	started, release := make(chan struct{}), make(chan struct{})
	var once sync.Once
	rc, _ := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		once.Do(func() { close(started) })
		<-release
		_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 9})
	})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	ctx := engine.WithObservationPass(t.Context())
	done := make(chan error, 1)
	go func() {
		_, _, err := mysqlAgentStatus(ctx, rc, "mysql0")
		done <- err
	}()
	<-started
	waiter, cancel := context.WithCancel(ctx)
	cancel()
	before := time.Now()
	if _, _, err := mysqlAgentStatus(waiter, rc, "mysql0"); err != context.Canceled || time.Since(before) > 300*time.Millisecond {
		t.Fatalf("single-flight wait ignored cancellation: %v", err)
	}
	unblock()
	if err := <-done; err != nil {
		t.Fatal("cancelled waiter cancelled the independent owner", err)
	}
}

func TestMySQLReadObservationAdmissionAndAbsoluteDeadline(t *testing.T) {
	rc := &engine.Context{Cluster: &domain.Cluster{Name: "reads", Namespace: "default"}}
	ctx := engine.WithObservationPass(t.Context())
	first, cancel := mysqlReadContext(ctx, rc)
	defer cancel()
	second, cancelSecond := mysqlReadContext(ctx, rc)
	defer cancelSecond()
	a, okA := first.Deadline()
	b, okB := second.Deadline()
	if !okA || !okB || !a.Equal(b) {
		t.Fatal("repeated reads renewed the absolute pass deadline")
	}
	fresh, cancelFresh := mysqlReadContext(freshMySQLReads(ctx), rc)
	defer cancelFresh()
	if !infra.IsObservation(first) || !infra.IsObservation(second) || !infra.IsObservation(fresh) || infra.IsObservation(ctx) {
		t.Fatal("local read wrapper lost observation admission or contaminated its mutation parent")
	}
}

func TestMySQLReadObservationOptionalFailureBound(t *testing.T) {
	var calls atomic.Int32
	rc, _ := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.NotFound(w, r)
	})
	ctx := engine.WithObservationPass(t.Context())
	for range 9 {
		var status myspec.GaleraStatusResponse
		if err := mysqlReadAction(ctx, rc, "mysql0", myspec.ActionGaleraStatus, struct{}{}, &status); err == nil {
			t.Fatal("unsupported optional endpoint accepted")
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("optional endpoint issued %d calls, want one sweep and one retry", calls.Load())
	}
	var status myspec.GaleraStatusResponse
	_ = mysqlReadAction(freshMySQLReads(ctx), rc, "mysql0", myspec.ActionGaleraStatus, struct{}{}, &status)
	if calls.Load() != 3 {
		t.Fatal("optional attempt limit suppressed a mandatory fresh actor read")
	}
}

func TestMySQLReadObservationGTIDLockCancellation(t *testing.T) {
	var calls atomic.Int32
	rc, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.NotFound(w, r)
	})
	rc.Cluster.Spec.Provider = "pxc"
	lock := gtidObservationMutex(rc)
	lock.Lock()
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(engine.WithObservationPass(t.Context()), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	err := checkPXCQuorumGTIDs(ctx, rc, s)
	var unavailable *myspec.GTIDQuorumError
	if !errors.As(err, &unavailable) || unavailable.Diverged || time.Since(started) > 300*time.Millisecond || calls.Load() != 0 {
		t.Fatalf("optional lock contention escaped cancellation or claimed divergence: %v calls=%d", err, calls.Load())
	}
	actor, stop := context.WithTimeout(engine.WithObservationPass(t.Context()), 30*time.Millisecond)
	defer stop()
	if err := establishGTIDBaseline(actor, rc, s, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("mandatory actor lock wait did not fail closed: %v", err)
	}
}

func TestMySQLReadObservationInvalidatedBudgetIsDeferredWithoutReuse(t *testing.T) {
	var calls atomic.Int32
	rc, _ := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 17})
	})
	ctx := engine.WithObservationPass(t.Context())
	if _, _, err := mysqlAgentStatus(ctx, rc, "mysql0"); err != nil {
		t.Fatal(err)
	}
	invalidateMySQLReadObservation(ctx, rc)
	scope := mysqlObservationScope(ctx, rc)
	expired := time.Now().Add(-time.Millisecond)
	scope.mu.Lock()
	scope.deadline = expired
	scope.mu.Unlock()
	status, _, err := mysqlAgentStatus(ctx, rc, "mysql0")
	if !errors.Is(err, engine.ErrObservationBudgetExhausted) || status.ConfigVersion != 0 || calls.Load() != 1 {
		t.Fatalf("invalidated sample was reused or unsampled work was treated as failure: %+v %v calls=%d", status, err, calls.Load())
	}
	scope.mu.Lock()
	deadline := scope.deadline
	scope.mu.Unlock()
	if !deadline.Equal(expired) {
		t.Fatal("budget deferral replenished the absolute pass deadline")
	}
	if _, _, err := mysqlAgentStatus(freshMySQLReads(ctx), rc, "mysql0"); err != nil || calls.Load() != 2 {
		t.Fatal("deferred read budget suppressed an explicit mandatory fresh read")
	}
	initial := &mysqlReadScope{deadline: expired, hasSample: true}
	if got := initial.readError(context.DeadlineExceeded); got != context.DeadlineExceeded {
		t.Fatal("initial failed full-length probe was masked as a scheduling deferral")
	}
}

func TestMySQLReadObservationDeferredSnapshotKeepsGenuineFailures(t *testing.T) {
	at := time.Now().UTC().Add(-time.Second)
	for _, metadataFailure := range []bool{false, true} {
		rc := &engine.Context{Cluster: &domain.Cluster{Status: domain.ClusterStatus{
			Phase: domain.PhaseReady, ObservedAt: at, ObservationEpoch: "original",
			Members: []domain.MemberStatus{
				{Name: "mysql0", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}},
				{Name: "mysql1", Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at}},
			},
		}}}
		s := &state{Members: []*member{{Name: "mysql0", Role: "data"}, {Name: "mysql1", Role: "data"}}}
		actual := &agentapi.HTTPError{StatusCode: http.StatusServiceUnavailable, Message: "native unavailable"}
		joined := errors.Join(engine.ErrObservationBudgetExhausted, actual)
		observed := []memberObservation{
			{err: joined, metadataErr: engine.ErrObservationBudgetExhausted, nativeErr: joined},
			{infrastructureErr: engine.ErrObservationBudgetExhausted},
		}
		if metadataFailure {
			observed[0].metadataErr, observed[0].nativeErr = joined, engine.ErrObservationBudgetExhausted
		}
		status, err, ok := deferredMySQLObservation(rc, s, observed)
		var deferred *engine.ObservationBudgetError
		if !ok || !errors.As(err, &deferred) || !errors.Is(err, actual) || len(deferred.Failures) != 1 {
			t.Fatalf("concurrent real failure disappeared in budget deferral: %v", err)
		}
		if status.Members[0].Health || status.Members[0].Agent.Healthy == metadataFailure ||
			!status.Members[1].Health || !status.Members[1].Agent.Healthy || !status.ObservedAt.Equal(at) {
			t.Fatal("deferred snapshot lost fresh negative facts or withdrew unsampled peers")
		}
	}
}

func TestMySQLReadObservationOuterAggregatePreservesHTTPFailure(t *testing.T) {
	for _, wrapped := range []bool{false, true} {
		t.Run(fmt.Sprintf("wrapped=%t", wrapped), func(t *testing.T) {
			var block atomic.Bool
			release := make(chan struct{})
			rc, _ := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/mysql/proxy-status" {
					http.Error(w, "proxy unavailable", http.StatusServiceUnavailable)
					return
				}
				if block.Load() {
					select {
					case <-r.Context().Done():
					case <-release:
					}
					return
				}
				_ = json.NewEncoder(w).Encode(agentapi.Status{ConfigVersion: 1})
			})
			t.Cleanup(func() { close(release) })
			ctx := engine.WithObservationPass(t.Context())
			if _, _, err := mysqlAgentStatus(ctx, rc, "mysql0"); err != nil {
				t.Fatal(err)
			}
			block.Store(true)
			invalidateMySQLReadObservation(ctx, rc)
			scope := mysqlObservationScope(ctx, rc)
			scope.mu.Lock()
			scope.deadline = time.Now().Add(500 * time.Millisecond)
			scope.mu.Unlock()
			_, _, err := mysqlReadValue(ctx, rc, "proxy", "all-replicas", func(probe context.Context) (bool, error) {
				var failures [2]error
				var batch sync.WaitGroup
				batch.Add(2)
				go func() {
					defer batch.Done()
					var status myspec.ProxyStatusResponse
					failures[0] = mysqlReadAction(probe, rc, "proxy", myspec.ActionProxyStatus, struct{}{}, &status)
				}()
				go func() {
					defer batch.Done()
					_, _, failures[1] = mysqlAgentStatus(probe, rc, "mysql1")
				}()
				batch.Wait()
				combined := errors.Join(failures[0], failures[1], probe.Err())
				if wrapped {
					combined = fmt.Errorf("proxy replica observation: %w", combined)
				}
				return false, combined
			})
			var remote *agentapi.HTTPError
			if !errors.Is(err, engine.ErrObservationBudgetExhausted) || !errors.As(err, &remote) || remote.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("outer read aggregate erased an actual HTTP503 beside its exhausted sibling: %v", err)
			}
			remote = nil
			if actual := observedFailureCause(err); !errors.As(actual, &remote) || remote.StatusCode != http.StatusServiceUnavailable {
				t.Fatalf("status classification discarded the retained HTTP failure: %v", actual)
			}
		})
	}
}

type failedInfrastructureObservationBackend struct {
	infra.Backend
	serviceErr, containersErr error
}

func (b failedInfrastructureObservationBackend) GetService(context.Context, string) (*infra.Service, error) {
	return nil, b.serviceErr
}

func (b failedInfrastructureObservationBackend) Containers(context.Context, string) ([]infra.Container, error) {
	return nil, b.containersErr
}

func TestMySQLParallelInfrastructurePreservesFailureBesideServiceDeferral(t *testing.T) {
	for _, deferred := range []bool{false, true} {
		t.Run(fmt.Sprintf("serviceDeferred=%t", deferred), func(t *testing.T) {
			rc, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Path == "/v1/status" {
					_ = json.NewEncoder(w).Encode(agentapi.Status{})
				} else {
					_ = json.NewEncoder(w).Encode(myspec.StatusResponse{MemberState: "ONLINE", MemberRole: "PRIMARY"})
				}
			})
			actual := errors.New("task inventory unavailable")
			backend := failedInfrastructureObservationBackend{containersErr: actual}
			if deferred {
				backend.serviceErr = engine.ErrObservationBudgetExhausted
			}
			rc.Backend = backend
			at := time.Now().UTC().Add(-time.Second)
			rc.Cluster.Status = domain.ClusterStatus{Phase: domain.PhaseReady, ObservedAt: at, ObservationEpoch: "previous"}
			for _, m := range s.Members {
				rc.Cluster.Status.Members = append(rc.Cluster.Status.Members, domain.MemberStatus{
					Name: m.Name, Health: true, Agent: domain.AgentStatus{Healthy: true, LastSeen: at},
				})
			}
			observed := observeStatusMembers(engine.WithObservationPass(t.Context()), rc, s.Members)
			for i, sample := range observed {
				if !errors.Is(sample.infrastructureErr, actual) {
					t.Fatalf("member %s dropped independent task failure: %v", s.Members[i].Name, sample.infrastructureErr)
				}
			}
			if deferred {
				status, err, ok := deferredMySQLObservation(rc, s, observed)
				if !ok || !errors.Is(err, actual) || !errors.Is(err, engine.ErrObservationBudgetExhausted) {
					t.Fatal("deferral erased genuine infrastructure failure", err)
				}
				for _, m := range status.Members {
					if m.Health {
						t.Fatal("genuine task failure retained old positive health", m.Name)
					}
				}
			}
		})
	}
}
