// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type retryClock struct{ at time.Time }

func (c *retryClock) Now() time.Time { return c.at }

// Unlike fakeInfra, failed deletions leave the service present. Successful
// completion is applied explicitly by the test before observing its process.
type retryInfra struct {
	*fakeInfra
	deletes []string
	polls   int
}

func (f *retryInfra) DeleteService(_ context.Context, name string) (infra.Op, error) {
	f.mutations++
	f.deletes = append(f.deletes, name)
	return infra.Op{ID: "delete"}, nil
}

func (f *retryInfra) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	f.polls++
	st, err := f.fakeInfra.OpStatus(ctx, op)
	st.Message = "FAILED: platform action rejected"
	return st, err
}

func retrySetup(t *testing.T) (*Provider, *engine.Context, *retryInfra, *retryClock) {
	t.Helper()
	p, rc, f, _ := setup(t)
	backend := &retryInfra{fakeInfra: f}
	clock := &retryClock{at: time.Date(2024, 1, 6, 12, 0, 0, 0, time.UTC)}
	rc.Backend, rc.Clock = backend, clock
	return p, rc, backend, clock
}

func retryState(t *testing.T, rc *engine.Context) *internalState {
	t.Helper()
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func syncToPending(t *testing.T, p *Provider, rc *engine.Context, action string) *internalState {
	t.Helper()
	for range 8 {
		s := retryState(t, rc)
		if s.Pending != nil && s.Pending.Action == action {
			return s
		}
		if _, err := p.Sync(context.Background(), rc); err != nil {
			t.Fatal(err)
		}
	}
	t.Fatalf("did not reach pending %s", action)
	return nil
}

func TestPlatformDeployRetryBackoffProgressAndReset(t *testing.T) {
	p, rc, f, clock := retrySetup(t)
	ctx := context.Background()
	s := syncToPending(t, p, rc, "deploy")
	memberName := s.Pending.Member
	f.failed = true
	if _, err := p.Sync(ctx, rc); err == nil || !strings.Contains(err.Error(), "platform action rejected") || !strings.Contains(err.Error(), "attempt 1") {
		t.Fatalf("failure must include platform message and attempt: %v", err)
	}
	s = retryState(t, rc)
	r := s.PlatformRetries[memberName+"/deploy"]
	if s.Pending != nil || s.Retry == nil || s.Retry.Op.ID != "" || r.Failures != 1 || !r.RetryAfter.Equal(clock.at.Add(30*time.Second)) || s.find(memberName).Stage != "imported" {
		t.Fatalf("failure did not durably schedule retry: %+v %+v", s, r)
	}
	before, polls := f.mutations, f.polls
	clock.at = r.RetryAfter.Add(-time.Nanosecond)
	var waiting *engine.PlatformRetryWait
	if _, err := p.Sync(ctx, rc); !errors.As(err, &waiting) {
		t.Fatal("backoff must report scheduled wait without another failed attempt")
	}
	if f.mutations != before || f.polls != polls {
		t.Fatal("backoff reissued or polled the failed process")
	}
	clock.at = r.RetryAfter
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	if len(f.deployments) != 2 || f.mutations != before+1 {
		t.Fatal("failed deployment was not reissued exactly once")
	}
	if _, err := p.Sync(ctx, rc); err == nil || !strings.Contains(err.Error(), "attempt 2") {
		t.Fatalf("second failure lost counter: %v", err)
	}
	s = retryState(t, rc)
	r = s.PlatformRetries[memberName+"/deploy"]
	if r.Failures != 2 || !r.RetryAfter.Equal(clock.at.Add(time.Minute)) {
		t.Fatalf("second backoff: %+v", r)
	}
	clock.at = r.RetryAfter
	f.failed = false
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	if _, err := p.Sync(ctx, rc); err != nil {
		t.Fatal(err)
	}
	s = retryState(t, rc)
	if s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 || s.find(memberName).Stage != "deployed" || s.find(memberName).Version != rc.Cluster.Spec.Version {
		t.Fatalf("successful retry did not apply and reset: %+v", s)
	}
	if strings.Contains(string(rc.Cluster.Internal), "platformRetries") || strings.Contains(string(rc.Cluster.Internal), `"retry"`) {
		t.Fatal("cleared retry keys survived merged persistence")
	}
	for range 5 {
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatal(err)
		}
		if retryState(t, rc).Sets["rs0"].Stage == "users" {
			break
		}
	}
	s = retryState(t, rc)
	if s.Sets["rs0"].Stage != "users" {
		t.Fatal("cluster did not progress after retry")
	}
	// A subsequent failure of the same action starts a new consecutive series.
	s.Pending = &pending{Op: infra.Op{ID: "new-deploy"}, Action: "deploy", Member: memberName, Version: rc.Cluster.Spec.Version}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	f.failed = true
	if _, err := p.Sync(ctx, rc); err == nil || !strings.Contains(err.Error(), "attempt 1") {
		t.Fatalf("success did not reset consecutive failures: %v", err)
	}
}

func TestPlatformRetryContinuesAtCapAndReportsPersistentFailure(t *testing.T) {
	p, rc, f, clock := retrySetup(t)
	ctx := context.Background()
	syncToPending(t, p, rc, "deploy")
	f.failed = true
	for attempt := 1; attempt <= 12; attempt++ {
		failedAt := clock.at
		if _, err := p.Sync(ctx, rc); err == nil {
			t.Fatal("failure not surfaced")
		}
		s := retryState(t, rc)
		r := s.PlatformRetries[platformRetryKey(s.Retry)]
		expected := []time.Duration{30 * time.Second, time.Minute, 2 * time.Minute, 4 * time.Minute, 8 * time.Minute, 10 * time.Minute}[min(attempt-1, 5)]
		if r.Failures != attempt || r.RetryAfter.Sub(failedAt) != expected {
			t.Fatalf("attempt %d: %+v", attempt, r)
		}
		if attempt >= 10 {
			status, err := p.Status(ctx, rc)
			if err != nil || !strings.Contains(status.Message, "keeps failing") || !strings.Contains(status.Message, "platform action rejected") {
				t.Fatalf("persistent failure not in cluster status: %+v %v", status, err)
			}
		}
		clock.at = r.RetryAfter
		if _, err := p.Sync(ctx, rc); err != nil {
			t.Fatalf("retry stopped after attempt %d: %v", attempt, err)
		}
	}
	if len(f.deployments) != 13 {
		t.Fatal("capped retries stopped reissuing")
	}
}

func TestPlatformImportRetryAdoptsAndVerifiesOwnership(t *testing.T) {
	for _, remaining := range []string{"both", "runtime", "volume", "foreign"} {
		t.Run(remaining, func(t *testing.T) {
			p, rc, f, clock := retrySetup(t)
			ctx := context.Background()
			s := syncToPending(t, p, rc, "import")
			m := s.Members[0]
			switch remaining {
			case "runtime":
				delete(f.services, m.Volume)
			case "volume":
				delete(f.services, m.Name)
			case "foreign":
				f.services[m.Name].ID = "foreign-service"
				f.services[m.Name].Ownership = nil
			}
			f.failed = true
			if _, err := p.Sync(ctx, rc); err == nil {
				t.Fatal("failed import not surfaced")
			}
			s = retryState(t, rc)
			before := f.mutations
			if _, err := p.Sync(ctx, rc); err == nil || f.mutations != before {
				t.Fatal("import ignored backoff")
			}
			clock.at = s.PlatformRetries[platformRetryKey(s.Retry)].RetryAfter
			f.failed = false
			_, err := p.Sync(ctx, rc)
			if remaining == "foreign" {
				if !errors.Is(err, engine.ErrConflict) || f.mutations != before || len(f.imports) != 1 {
					t.Fatalf("foreign service adopted or mutated: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if remaining == "both" {
				if len(f.imports) != 1 || f.mutations != before {
					t.Fatal("already-created services were duplicated")
				}
			} else {
				if len(f.imports) != 2 || !reflect.DeepEqual(f.imports[0], f.imports[1]) {
					t.Fatal("retry must resume the exact original immutable batch")
				}
				if _, err := p.Sync(ctx, rc); err != nil {
					t.Fatal(err)
				}
			}
			s = retryState(t, rc)
			m = s.Members[0]
			if m.Stage != "imported" || m.ServiceID == "" || m.VolumeID == "" || s.Retry != nil || s.Pending != nil || len(s.PlatformRetries) != 0 {
				t.Fatalf("import adoption did not complete/reset: %+v", s)
			}
		})
	}
}

func TestPlatformCleanupRetry(t *testing.T) {
	for _, action := range []string{"cleanup-runtime", "cleanup-volume"} {
		t.Run(action, func(t *testing.T) {
			p, rc, f, clock := retrySetup(t)
			ctx := context.Background()
			s := retryState(t, rc)
			s.UID, s.CleanupBackups = "cleanup-retry", true
			plan(rc.Cluster, s)
			if err := p.ensureSecrets(ctx, rc, s); err != nil {
				t.Fatal(err)
			}
			m := s.Members[0]
			m.ServiceID, m.VolumeID = m.Name, m.Volume
			name := m.Name
			if action == "cleanup-volume" {
				name = m.Volume
			}
			f.services[name] = &infra.Service{ID: name, Hostname: name}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if done, err := p.Cleanup(ctx, rc, domain.DeleteOptions{}); err != nil || done {
				t.Fatal(done, err)
			}
			if retryState(t, rc).Pending.Action != action {
				t.Fatal("wrong cleanup action issued")
			}
			f.failed = true
			if _, err := p.Cleanup(ctx, rc, domain.DeleteOptions{}); err == nil || !strings.Contains(err.Error(), "attempt 1") || !strings.Contains(err.Error(), "platform action rejected") {
				t.Fatalf("cleanup failure not surfaced: %v", err)
			}
			s = retryState(t, rc)
			if s.Pending != nil || s.Retry == nil {
				t.Fatal("failed cleanup process stayed pending")
			}
			before := f.mutations
			clock.at = s.PlatformRetries[platformRetryKey(s.Retry)].RetryAfter.Add(-time.Second)
			if _, err := p.Cleanup(ctx, rc, domain.DeleteOptions{}); err == nil || f.mutations != before {
				t.Fatal("cleanup backoff not respected")
			}
			clock.at = clock.at.Add(time.Second)
			f.failed = false
			if _, err := p.Cleanup(ctx, rc, domain.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			if len(f.deletes) != 2 || f.deletes[0] != name || f.deletes[1] != name {
				t.Fatal("cleanup delete was not reissued")
			}
			delete(f.services, name)
			if _, err := p.Cleanup(ctx, rc, domain.DeleteOptions{}); err != nil {
				t.Fatal(err)
			}
			s = retryState(t, rc)
			if s.Pending != nil || s.Retry != nil || len(s.PlatformRetries) != 0 {
				t.Fatal("successful cleanup did not reset retry")
			}
			if done, err := p.Cleanup(ctx, rc, domain.DeleteOptions{}); !done || err != nil {
				t.Fatal("cleanup did not progress", done, err)
			}
		})
	}
}

func TestPlatformRetryPreservesAppliedTargets(t *testing.T) {
	for _, action := range []string{"upgrade", "resources", "disk", "containers", "delete-runtime", "delete-volume"} {
		t.Run(action, func(t *testing.T) {
			p, rc, f, clock := retrySetup(t)
			ctx := context.Background()
			s := retryState(t, rc)
			s.UID = "target-retry"
			plan(rc.Cluster, s)
			if err := p.ensureSecrets(ctx, rc, s); err != nil {
				t.Fatal(err)
			}
			m := s.Members[0]
			// Router actions avoid a real MongoDB dependency in disruption checks.
			m.Role, m.Stage, m.Version = agentapi.RoleRouter, "active", "7.0.43-23"
			m.ServiceID, m.VolumeID = m.Name, m.Volume
			f.services[m.Name] = &infra.Service{ID: m.Name, Hostname: m.Name}
			f.services[m.Volume] = &infra.Service{ID: m.Volume, Hostname: m.Volume}
			target := &pending{Op: infra.Op{ID: "failed"}, Action: action, Member: m.Name, Version: "8.0.32-14", Resources: domain.Resources{MinCPU: 2, MaxCPU: 4, MinRAM: 2, MaxRAM: 4, MinDisk: 10, MaxDisk: 20}, Containers: 3}
			s.Pending = target
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			f.failed = true
			if _, err := p.Sync(ctx, rc); err == nil {
				t.Fatal("failure not surfaced")
			}
			s = retryState(t, rc)
			if s.Members[0].Version != "7.0.43-23" || s.Members[0].Stage != "active" {
				t.Fatal("failed process advanced applied state")
			}
			clock.at = s.PlatformRetries[platformRetryKey(s.Retry)].RetryAfter
			f.failed = false
			if _, err := p.Sync(ctx, rc); err != nil {
				t.Fatal(err)
			}
			s = retryState(t, rc)
			if s.Pending == nil || s.Pending.Action != action || s.Pending.Member != m.Name || s.Pending.Version != target.Version || s.Pending.Resources != target.Resources || s.Pending.Containers != target.Containers {
				// Deploy persists only its version target, not irrelevant fields.
				if action != "upgrade" || s.Pending == nil || s.Pending.Member != m.Name || s.Pending.Version != target.Version {
					t.Fatalf("retry changed proposed target: %+v", s.Pending)
				}
			}
			if strings.HasPrefix(action, "delete-") {
				name := m.Name
				if action == "delete-volume" {
					name = m.Volume
				}
				delete(f.services, name)
			}
			if _, err := p.Sync(ctx, rc); err != nil {
				t.Fatal(err)
			}
			s = retryState(t, rc)
			m = s.find(m.Name)
			switch action {
			case "upgrade":
				if m.Stage != "upgrade-wait" || m.Version != target.Version {
					t.Fatal("upgrade retry did not wait for observed binary")
				}
			case "resources":
				if m.Resources != target.Resources {
					t.Fatal("runtime resource target not applied")
				}
			case "disk":
				if m.Disk != target.Resources {
					t.Fatal("disk target not applied")
				}
			case "containers":
				if m.Containers != target.Containers {
					t.Fatal("container count not applied")
				}
			case "delete-runtime":
				if m.Stage != "delete-volume" {
					t.Fatal("runtime deletion did not advance")
				}
			case "delete-volume":
				if m != nil {
					t.Fatal("volume deletion did not remove member")
				}
			}
			if len(s.PlatformRetries) != 0 || s.Retry != nil || s.Pending != nil {
				t.Fatal("success did not clear retry state")
			}
		})
	}
}

func TestPlatformRetryPreservesRollingCheckpoint(t *testing.T) {
	p, rc, f, clock := retrySetup(t)
	ctx := context.Background()
	s := syncToPending(t, p, rc, "deploy")
	m := s.Members[0]
	m.Role = agentapi.RoleRouter
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	cp := operationState{Waiting: m.Name, Done: []string{"already-completed"}}
	checkpoint, err := json.Marshal(cp)
	if err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Kind: domain.OperationRestart, Checkpoint: checkpoint}
	f.failed = true
	result, err := p.RunOperation(ctx, rc, op)
	if err == nil || result.Done || result.Step != "waiting-platform" {
		t.Fatal("rolling operation ignored failed deploy", result, err)
	}
	var observed operationState
	if err := json.Unmarshal(result.Checkpoint, &observed); err != nil {
		t.Fatal(err)
	}
	if observed.Waiting != cp.Waiting || len(observed.Done) != 1 {
		t.Fatal("failure advanced operation checkpoint")
	}
	before := f.mutations
	if _, err := p.RunOperation(ctx, rc, op); err == nil || f.mutations != before {
		t.Fatal("rolling operation bypassed backoff")
	}
	s = retryState(t, rc)
	clock.at = s.PlatformRetries[platformRetryKey(s.Retry)].RetryAfter
	f.failed = false
	for range 2 {
		result, err = p.RunOperation(ctx, rc, op)
		if err != nil || result.Done || result.Step != "waiting-platform" {
			t.Fatal("rolling operation advanced before platform completion", result, err)
		}
		if err := json.Unmarshal(result.Checkpoint, &observed); err != nil {
			t.Fatal(err)
		}
		if observed.Waiting != cp.Waiting || len(observed.Done) != 1 {
			t.Fatal("retry lost rolling checkpoint")
		}
	}
	result, err = p.RunOperation(ctx, rc, op)
	if err != nil || result.Step != "member-healthy" {
		t.Fatal("rolling operation did not resume after platform success", result, err)
	}
}

type cooldownInfra struct {
	*fakeInfra
	err error
}

func (f *cooldownInfra) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	if f.err != nil {
		f.mutations++
		return infra.Op{}, f.err
	}
	return f.fakeInfra.SetResources(ctx, name, resources)
}

func TestCPUModeCooldownPersistsAndConverges(t *testing.T) {
	for _, knownChange := range []bool{true, false} {
		t.Run(fmt.Sprint(knownChange), func(t *testing.T) {
			p, rc, f, clock := retrySetup(t)
			ctx := context.Background()
			s := syncToPending(t, p, rc, "deploy")
			m := s.Members[0]
			m.Role = agentapi.RoleRouter
			m.Resources = runtimeResources(domain.Resources{CPUMode: "DEDICATED", MinCPU: 1, MaxCPU: 1, MinRAM: 1, MaxRAM: 1})
			if knownChange {
				m.CPUModeChangedAt = clock.at.Add(-10 * time.Minute)
			}
			wantAt := clock.at.Add(time.Hour)
			if knownChange {
				wantAt = m.CPUModeChangedAt.Add(time.Hour)
			}
			target := &pending{Action: "resources", Member: m.Name, Resources: m.Resources}
			target.Resources.CPUMode = "SHARED"
			s.Pending = nil
			cause := fmt.Errorf("resource update rejected: %w", infra.ErrResourceChangeCooldown)
			var wait *engine.PlatformRetryWait
			if err := s.scheduleCPUModeCooldown(rc, target, cause); !errors.As(err, &wait) || !wait.RetryAt.Equal(wantAt) {
				t.Fatalf("cooldown deadline: %v", err)
			}
			if err := save(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			// Reloading proves the deadline/target survives control-plane restart.
			s = retryState(t, rc)
			r := s.PlatformRetries[platformRetryKey(s.Retry)]
			if !r.CPUModeCooldown || r.Failures != 0 || !r.RetryAfter.Equal(wantAt) || s.Retry.Resources.CPUMode != "SHARED" {
				t.Fatalf("persisted cooldown: %+v %+v", r, s.Retry)
			}
			status, err := p.Status(ctx, rc)
			if err != nil || !strings.Contains(status.Message, "CPU-mode changes may be rate-limited") {
				t.Fatalf("cooldown status: %+v %v", status, err)
			}
			found := false
			for _, condition := range status.Conditions {
				found = found || condition.Type == "PlatformCooldown" && condition.Reason == "CPUModeChangeTooRecent"
			}
			if !found {
				t.Fatal("cooldown condition missing")
			}
			before := f.mutations
			clock.at = wantAt.Add(-time.Nanosecond)
			for range 3 {
				if _, err := p.Sync(ctx, rc); !errors.As(err, &wait) || f.mutations != before {
					t.Fatalf("cooldown reissued request: %v", err)
				}
			}
			clock.at = wantAt
			if _, err := p.Sync(ctx, rc); err != nil {
				t.Fatal(err)
			}
			if f.mutations != before+1 {
				t.Fatal("cooldown expiration did not issue exactly one request")
			}
			if _, err := p.Sync(ctx, rc); err != nil {
				t.Fatal(err)
			}
			s = retryState(t, rc)
			if s.find(m.Name).Resources.CPUMode != "SHARED" || !s.find(m.Name).CPUModeChangedAt.Equal(clock.at) || s.Retry != nil || s.Pending != nil || len(s.PlatformRetries) != 0 {
				t.Fatalf("cooldown did not converge/reset: %+v", s)
			}
		})
	}
}

func TestCPUModeCooldownOnReissuedResourceRequest(t *testing.T) {
	p, rc, f, clock := retrySetup(t)
	s := syncToPending(t, p, rc, "deploy")
	m := s.Members[0]
	m.Role = agentapi.RoleRouter
	v := &pending{Action: "resources", Member: m.Name, Resources: domain.Resources{CPUMode: "SHARED"}}
	s.Retry, s.Pending = v, nil
	s.PlatformRetries = map[string]*platformRetry{platformRetryKey(v): {RetryAfter: clock.at}}
	rc.Backend = &cooldownInfra{fakeInfra: f.fakeInfra, err: fmt.Errorf("resource update rejected: %w", infra.ErrResourceChangeCooldown)}
	var wait *engine.PlatformRetryWait
	if _, err := p.retryPending(context.Background(), rc, s); !errors.As(err, &wait) || !wait.RetryAt.Equal(clock.at.Add(time.Hour)) {
		t.Fatalf("reissued request did not schedule cooldown: %v", err)
	}
	if r := s.PlatformRetries[platformRetryKey(v)]; !r.CPUModeCooldown || r.Failures != 0 {
		t.Fatalf("cooldown incorrectly counted as failure: %+v", r)
	}
}

func TestCPUModeInitialResourceRejectionSchedulesCooldown(t *testing.T) {
	p, rc, f, clock := retrySetup(t)
	s := syncToPending(t, p, rc, "deploy")
	m := s.Members[0]
	m.Stage, m.Version = "active", rc.Cluster.Spec.Version
	m.Resources = runtimeResources(component(rc.Cluster.Spec, m).Resources)
	m.Resources.CPUMode = "DEDICATED"
	m.CPUModeChangedAt = clock.at.Add(-5 * time.Minute)
	m.Disk = component(rc.Cluster.Spec, m).Resources
	for _, set := range s.Sets {
		set.Stage, set.Added = "ready", true
	}
	s.Pending = nil
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Backend = &cooldownInfra{fakeInfra: f.fakeInfra, err: fmt.Errorf("resource update rejected: %w", infra.ErrResourceChangeCooldown)}
	var wait *engine.PlatformRetryWait
	for range 5 {
		_, err := p.sync(context.Background(), rc, func(context.Context, *engine.Context, *internalState, *member) (bool, error) {
			return false, nil
		}, p.replication)
		if errors.As(err, &wait) {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
	}
	if wait == nil || !wait.RetryAt.Equal(m.CPUModeChangedAt.Add(time.Hour)) {
		t.Fatalf("initial rejection was not scheduled: %+v", wait)
	}
	s = retryState(t, rc)
	if s.Retry == nil || s.Retry.Resources.CPUMode != "SHARED" || s.find(m.Name).Resources.CPUMode != "DEDICATED" {
		t.Fatalf("initial rejection changed applied state or lost target: %+v", s)
	}
}

func TestCPUModeCooldownDoesNotHideOtherPlatformErrors(t *testing.T) {
	_, rc, _, _ := retrySetup(t)
	s := retryState(t, rc)
	for _, cause := range []error{
		errors.New("resource update rejected"),
		&infra.RejectedError{Code: "invalidResources"},
	} {
		if err := s.scheduleCPUModeCooldown(rc, &pending{Action: "resources"}, cause); err != nil || s.Retry != nil {
			t.Fatal("ordinary error was treated as a CPU-mode cooldown", err)
		}
	}
}
