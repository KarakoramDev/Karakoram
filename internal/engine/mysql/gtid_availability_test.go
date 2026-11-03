// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestPXCQuorumReservedBudget(t *testing.T) {
	for _, size := range []int{3, 5} {
		t.Run(fmt.Sprint(size), func(t *testing.T) {
			const sid = "11111111-2222-3333-4444-555555555555"
			probe, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
				_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: sid, GTIDExecuted: sid + ":1-7", ClusterSize: size})
			})
			_, rc, _ := setup(t, "cluster")
			rc.Cluster.Spec.Provider, rc.Agents = "pxc", probe.Agents
			for len(s.Members) < size {
				name := fmt.Sprintf("mysql%d", len(s.Members))
				s.Members = append(s.Members, &member{Name: name, Role: "data", ServiceID: name + "-id", Stage: "joined"})
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			ctx := engine.WithObservationPass(t.Context())
			scope := mysqlObservationScope(ctx, rc)
			// Required reads consumed their absolute pass budget, on an idle cluster
			// or after scale-out. Invalidation must not renew that required budget.
			scope.deadline = time.Now().Add(-time.Second)
			invalidateMySQLReadObservation(ctx, rc)
			if err := checkPXCQuorumGTIDs(ctx, rc, s); err != nil {
				t.Fatalf("healthy majority starved by required reads: %v", err)
			}
			if s.LastGTIDSafeAt.IsZero() {
				t.Fatal("complete sweep did not advance watermark")
			}
			if scope.deadline.After(time.Now()) {
				t.Fatal("renewed required-read budget")
			}
		})
	}
}

func TestPXCQuorumUnavailableDiscriminators(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	probe, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer mysql2" {
			http.Error(w, "private upstream detail", http.StatusServiceUnavailable)
			return
		}
		st := myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: sid, GTIDExecuted: sid + ":1-7", ClusterSize: 5}
		if r.Header.Get("Authorization") == "Bearer mysql1" {
			st.Ready = false
		}
		_ = json.NewEncoder(w).Encode(st)
	})
	_, rc, _ := setup(t, "cluster")
	rc.Cluster.Spec.Provider, rc.Agents = "pxc", probe.Agents
	s.Members = append(s.Members, &member{Name: "mysql3", Role: "data", Stage: "planned"}, &member{Name: "mysql4", Role: "data", Stage: "imported"})
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	err := checkPXCQuorumGTIDs(engine.WithObservationPass(t.Context()), rc, s)
	if _, ok := err.(*myspec.GTIDQuorumError); !ok {
		t.Fatalf("want typed majority failure, got %v", err)
	}
	for _, want := range []string{"majority-short", "mysql2=rpc-error", "mysql1=not-eligible", "mysql3=no-observation", "mysql4=no-observation"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("missing %s in %v", want, err)
		}
	}
	if strings.Contains(err.Error(), "private upstream") {
		t.Fatal("upstream error leaked")
	}
	if !s.LastGTIDSafeAt.IsZero() {
		t.Fatal("incomplete observation advanced watermark")
	}
	lock := gtidObservationMutex(rc)
	lock.Lock()
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	err = checkPXCQuorumGTIDs(ctx, rc, s)
	lock.Unlock()
	if err == nil || !strings.Contains(err.Error(), "observation-lock") {
		t.Fatalf("missing lock discriminator: %v", err)
	}
	// Recovering one non-eligible report and one RPC is not necessary: joining
	// two members supplies a compatible 3/5 majority despite the remaining faults.
	for _, m := range s.Members {
		if m.Name == "mysql3" || m.Name == "mysql4" {
			m.Stage = "joined"
			m.ServiceID = m.Name + "-id"
		}
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := checkPXCQuorumGTIDs(engine.WithObservationPass(t.Context()), rc, s); err != nil {
		t.Fatalf("observable 3/5 majority did not converge: %v", err)
	}
	if !s.LastGTIDSafeAt.IsZero() {
		t.Fatal("partial majority advanced watermark")
	}
}

func TestPXCQuorumPersistenceCondition(t *testing.T) {
	for _, mode := range []string{"read", "decode", "write", "audit"} {
		t.Run(mode, func(t *testing.T) {
			p, rc, b := setupPXC(t)
			converge(t, p, rc, b)
			if mode == "audit" {
				s, err := load(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				s.GTIDIncidents = append(s.GTIDIncidents, gtidIncident{DetectedAt: now(rc), BaselineAt: now(rc), BaselineAuditPending: true})
				if err := persistGTIDHistory(t.Context(), rc, s); err != nil {
					t.Fatal(err)
				}
				rc.Events = gtidEventFunc(func(context.Context, domain.Event) error { return fmt.Errorf("private journal payload") })
			}
			rc.Secrets = quorumFailingHistoryStore{SecretStore: rc.Secrets, mode: mode}
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			for _, c := range status.Conditions {
				if c.Type != "GTIDHistorySafe" {
					continue
				}
				if c.Status != "Unknown" || c.Reason != "GTIDHistoryPersistenceUnavailable" {
					t.Fatalf("persistence misclassified: %+v", c)
				}
				if !strings.Contains(c.Message, "persistence-error") || !strings.Contains(c.Message, "=eligible") || strings.Contains(c.Message, "private journal payload") {
					t.Fatalf("unsafe or missing persistence discriminator: %s", c.Message)
				}
				if err := p.ValidateBackup(t.Context(), rc, "full"); err == nil {
					t.Fatal("unavailable durable incident state must fail closed")
				}
				return
			}
			t.Fatal("missing GTID history condition")
		})
	}
}

func TestPXCQuorumObservationBudgetDiscriminator(t *testing.T) {
	probe, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("expired injected sampling reservation issued an RPC")
	})
	_, rc, _ := setup(t, "cluster")
	rc.Cluster.Spec.Provider, rc.Agents = "pxc", probe.Agents
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	ctx := engine.WithObservationPass(t.Context())
	mysqlObservationScope(ctx, rc).quorumDeadline = time.Now().Add(-time.Second)
	err := checkPXCQuorumGTIDs(ctx, rc, s)
	if err == nil || !strings.Contains(err.Error(), "observation-budget") || !strings.Contains(err.Error(), "mysql0=no-observation") {
		t.Fatalf("missing injected budget discriminator: %v", err)
	}
	started := time.Now()
	invalidateMySQLReadObservation(ctx, rc)
	err = checkPXCQuorumGTIDs(ctx, rc, s)
	if time.Since(started) > 300*time.Millisecond || err == nil || !strings.Contains(err.Error(), "observation-budget") {
		t.Fatalf("invalidation renewed continuity reservation: %v", err)
	}
}

type quorumFailingHistoryStore struct {
	engine.SecretStore
	mode          string
	onHistoryRead func()
}

func (s quorumFailingHistoryStore) GetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string) ([]byte, error) {
	if key == "mysql/gtid/history" {
		if s.onHistoryRead != nil {
			s.onHistoryRead()
			if s.mode == "cancel" {
				return nil, ctx.Err()
			}
			ctx = context.WithoutCancel(ctx)
		}
		switch s.mode {
		case "read":
			return nil, fmt.Errorf("private journal payload")
		case "decode":
			return []byte("{"), nil
		}
	}
	return s.SecretStore.GetClusterSecret(ctx, ref, key)
}

func (s quorumFailingHistoryStore) SetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string, value []byte) error {
	if key == "mysql/gtid/history" && s.mode == "write" {
		return fmt.Errorf("private journal payload")
	}
	return s.SecretStore.SetClusterSecret(ctx, ref, key, value)
}

func TestPXCQuorumIncompleteComponentCannotAdvanceWatermark(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	probe, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: sid, GTIDExecuted: sid + ":1-7", ClusterSize: 3})
	})
	_, rc, _ := setup(t, "cluster")
	rc.Cluster.Spec.Provider, rc.Agents = "pxc", probe.Agents
	for len(s.Members) < 5 {
		name := fmt.Sprintf("mysql%d", len(s.Members))
		s.Members = append(s.Members, &member{Name: name, Role: "data", Stage: "joined", ServiceID: name + "-id"})
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err != nil {
		t.Fatalf("compatible majority with stale component size unavailable: %v", err)
	}
	if !s.LastGTIDSafeAt.IsZero() {
		t.Fatal("stale expected component size advanced watermark")
	}
}

func TestPXCQuorumFullAdmissionDuringLockContention(t *testing.T) {
	p, rc, _ := setupPXC(t)
	s := &state{Members: []*member{{Name: "mysql0", Role: "data", Stage: "joined"}}}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	lock := gtidObservationMutex(rc)
	lock.Lock()
	defer lock.Unlock()
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if err := p.ValidateBackup(freshMySQLReads(ctx), rc, "full"); err != nil {
		t.Fatalf("transient comparison lock blocked full: %v", err)
	}
	if err := p.ValidateBackup(freshMySQLReads(ctx), rc, "incremental"); err == nil {
		t.Fatal("incremental admitted without majority proof")
	}
}

type quorumDeadlineObserver struct {
	context.Context
	onDeadline func()
}

func (c quorumDeadlineObserver) Deadline() (time.Time, bool) {
	c.onDeadline()
	return c.Context.Deadline()
}

func TestPXCQuorumLockPrecedesSamplingReservation(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	probe, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: sid, GTIDExecuted: sid + ":1-7", ClusterSize: 3})
	})
	_, rc, _ := setup(t, "cluster")
	rc.Cluster.Spec.Provider, rc.Agents = "pxc", probe.Agents
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	ctx := engine.WithObservationPass(t.Context())
	scope := mysqlObservationScope(ctx, rc)
	var first sync.Once
	ctx = quorumDeadlineObserver{Context: ctx, onDeadline: func() {
		first.Do(func() {
			scope.mu.Lock()
			started := !scope.quorumDeadline.IsZero()
			scope.mu.Unlock()
			if started {
				t.Error("sampling reservation started before separately bounded observation-lock wait")
			}
		})
	}}
	started := time.Now()
	if err := checkPXCQuorumGTIDs(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	if scope.quorumDeadline.Before(started.Add(engine.ObservationTimeout)) {
		t.Fatal("sampling reservation shorter than the existing agent read timeout")
	}
}

func TestPXCQuorumJournalCancellationDoesNotRelabelSampling(t *testing.T) {
	probe, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{})
	})
	_, rc, _ := setup(t, "cluster")
	rc.Cluster.Spec.Provider, rc.Agents = "pxc", probe.Agents
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(engine.WithObservationPass(t.Context()))
	defer cancel()
	rc.Secrets = quorumFailingHistoryStore{SecretStore: rc.Secrets, onHistoryRead: cancel}
	err := checkPXCQuorumGTIDs(ctx, rc, s)
	if err == nil || !strings.Contains(err.Error(), "(comparison;") || strings.Contains(err.Error(), "observation-budget") {
		t.Fatalf("post-sample cancellation relabelled a completed majority-short comparison: %v", err)
	}
}

func TestPXCQuorumObserveStatusConcurrentReads(t *testing.T) {
	for _, shortage := range []bool{false, true} {
		t.Run(fmt.Sprintf("shortage=%t", shortage), func(t *testing.T) {
			p, rc, b := setupPXC(t)
			rc.Cluster.Spec.Components.Engine.Replicas = 5
			converge(t, p, rc, b)
			// Delay outside the backend fixture's serialized handler, so the
			// fixture models independent remote member RPCs rather than a queue.
			original := rc.Agents.(fakeAgents)
			target, err := url.Parse(original.url)
			if err != nil {
				t.Fatal(err)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(original.ca) {
				t.Fatal("invalid fixture CA")
			}
			transport := &http.Transport{TLSClientConfig: &tls.Config{RootCAs: roots}}
			t.Cleanup(transport.CloseIdleConnections)
			proxy := httputil.NewSingleHostReverseProxy(target)
			proxy.Transport = transport
			var quorumCalls atomic.Int32
			front, _ := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
				var delay time.Duration
				switch r.URL.Path {
				case "/v1/status", "/v1/mysql/status":
					delay = 3500 * time.Millisecond
				case "/v1/mysql/galera-status":
					delay = 2 * time.Second
					quorumCalls.Add(1)
				}
				timer := time.NewTimer(delay)
				defer timer.Stop()
				select {
				case <-r.Context().Done():
					return
				case <-timer.C:
				}
				if shortage && r.URL.Path == "/v1/mysql/galera-status" {
					_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Joining", Connected: true})
					return
				}
				proxy.ServeHTTP(w, r)
			})
			rc.Agents = front.Agents
			ctx := engine.WithObservationPass(t.Context())
			status, err := engine.ObserveStatus(ctx, p, rc)
			if err != nil {
				t.Fatal(err)
			}
			found := false
			for _, c := range status.Conditions {
				if c.Type != "GTIDHistorySafe" {
					continue
				}
				found = true
				if shortage {
					if c.Status != "Unknown" || c.Reason != "QuorumComparisonUnavailable" || !strings.Contains(c.Message, "majority-short (comparison;") {
						t.Fatalf("real majority shortage misclassified: %+v", c)
					}
				} else if c.Status != "True" || c.Reason != "QuorumHistoryCompatible" {
					t.Fatalf("outer status deadline starved healthy majority: %+v", c)
				}
			}
			if !found {
				t.Fatal("missing GTID safety condition")
			}
			if quorumCalls.Load() != 5 {
				t.Fatalf("comparison needed another RPC sweep after exhausting the first parent deadline: %d calls", quorumCalls.Load())
			}
			if !shortage {
				before := quorumCalls.Load()
				rc.Cluster.Status = status
				if _, err := p.Sync(ctx, rc); err != nil {
					t.Fatal(err)
				}
				if quorumCalls.Load() != before {
					t.Fatal("Sync did not reuse completed same-pass quorum sample")
				}
			}
		})
	}
}

func TestPXCQuorumParentCancellationDuringJournalIsBudget(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	probe, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{ClusterStatus: "Primary", LocalStateComment: "Synced", Ready: true, Connected: true, StateUUID: sid, GTIDExecuted: sid + ":1-7", ClusterSize: 3})
	})
	_, rc, _ := setup(t, "cluster")
	rc.Cluster.Spec.Provider, rc.Agents = "pxc", probe.Agents
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(engine.WithObservationPass(t.Context()))
	defer cancel()
	rc.Secrets = quorumFailingHistoryStore{SecretStore: rc.Secrets, mode: "cancel", onHistoryRead: cancel}
	err := checkPXCQuorumGTIDs(ctx, rc, s)
	if _, ok := err.(*myspec.GTIDQuorumError); !ok || !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "observation-budget") {
		t.Fatalf("parent cancellation misclassified as persistence failure: %v", err)
	}
}

func TestPXCQuorumCachedShortageSurvivesExpiredBudget(t *testing.T) {
	probe, s := quorumObservationFixture(t, func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(myspec.GaleraStatusResponse{})
	})
	_, rc, _ := setup(t, "cluster")
	rc.Cluster.Spec.Provider, rc.Agents = "pxc", probe.Agents
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	ctx := engine.WithObservationPass(t.Context())
	_ = checkPXCQuorumGTIDs(ctx, rc, s)
	mysqlObservationScope(ctx, rc).quorumDeadline = time.Now().Add(-time.Second)
	err := checkPXCQuorumGTIDs(ctx, rc, s)
	if err == nil || !strings.Contains(err.Error(), "majority-short (comparison;") {
		t.Fatalf("completed cached comparison relabelled as budget failure: %v", err)
	}
}

func TestPXCQuorumAcknowledgedBaselineJournalCancellation(t *testing.T) {
	const sid = "11111111-2222-3333-4444-555555555555"
	const local = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	p, rc, b := setupPXC(t)
	converge(t, p, rc, b)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	for i, m := range s.data() {
		st := b.native[m.Name]
		st.GTIDExecuted = sid + ":1-10"
		if i == 2 {
			st.GTIDExecuted += "," + local + ":1"
		}
		b.native[m.Name] = st
	}
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err == nil {
		t.Fatal("initial divergence not detected")
	}
	start := activeGTIDIncident(s).DetectedAt.Add(time.Second)
	full := &domain.Backup{Name: "baseline", Type: "full", Phase: domain.PhaseSucceeded, StartedAt: &start, MySQL: &domain.MySQLBackup{EndTime: start.Add(time.Second)}}
	if err := establishGTIDBaseline(t.Context(), rc, s, full); err != nil {
		t.Fatal(err)
	}
	if activeGTIDIncident(s) != nil || s.GTIDIncidents[0].Allowance == nil {
		t.Fatal("baseline not acknowledged")
	}
	original := rc.Secrets
	ctx, cancel := context.WithCancel(engine.WithObservationPass(t.Context()))
	defer cancel()
	rc.Secrets = quorumFailingHistoryStore{SecretStore: original, mode: "cancel", onHistoryRead: cancel}
	err = checkPXCQuorumGTIDs(ctx, rc, s)
	unavailable, ok := err.(*myspec.GTIDQuorumError)
	if !ok || unavailable.Diverged || !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "observation-incomplete") {
		t.Fatalf("unloaded acknowledged allowance became false divergence: %v", err)
	}
	if activeGTIDIncident(s) != nil {
		t.Fatal("cancelled journal load latched an acknowledged difference")
	}
	rc.Secrets = original
	if err := checkPXCQuorumGTIDs(t.Context(), rc, s); err != nil {
		t.Fatalf("authoritative baseline stopped permitting its frozen difference: %v", err)
	}
}
