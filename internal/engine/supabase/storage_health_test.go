// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestStorageDependencyFailureDegradesAndRecovers(t *testing.T) {
	p, rc, backend := setupProvider(t)
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), &domain.BackupStorage{
		Name: "objects", Namespace: "default", Type: "s3", EndpointURL: "https://objects.example.test", Bucket: "objects", Region: "test", VerifyTLS: true,
		Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "test-value"},
	}); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"backendStorage":"objects"}}`)
	desired := rc.Cluster.Spec.Components.Extra["storage"]
	desired.Replicas = 1
	rc.Cluster.Spec.Components.Extra["storage"] = desired
	convergeProvider(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := state.Members["storage"]
	instance := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, nil)
	rc.Backend = &componentInventoryBackend{testBackend: backend, member: m.Name, instances: []infra.Container{{ID: "storage-instance", Hostname: "storage-instance", Status: infra.ContainerRunning}}}
	rc.Agents = gatewayInstanceAgents{AgentDialer: rc.Agents, member: m.Name, instances: map[string]*gatewayInstanceFixture{"gateway-a": instance, "storage-instance": instance}}
	for _, failure := range []string{"credentials", "stale", "future", "zero"} {
		t.Run(failure, func(t *testing.T) {
			at := clockNow(rc)
			instance.mu.Lock()
			instance.ready, instance.message, instance.reason = true, "", ""
			switch failure {
			case "credentials":
				instance.ready = false
				instance.message = "Storage S3 dependency: authentication or bucket access denied"
				instance.reason = "StorageS3Unavailable"
			case "stale":
				at = at.Add(-engine.ReadinessMaxAge - time.Second)
			case "future":
				at = at.Add(time.Minute)
			case "zero":
				at = time.Time{}
			}
			instance.s3CheckedAt = &at
			instance.mu.Unlock()
			status, err := p.Status(t.Context(), rc)
			if err != nil {
				t.Fatal(err)
			}
			if status.Phase != domain.PhaseDegraded {
				t.Fatalf("dependency failure phase=%s, want Degraded", status.Phase)
			}
			for _, typ := range []string{"Ready", "ComponentsReady"} {
				found := false
				for _, c := range status.Conditions {
					if c.Type == typ {
						found = true
						if c.Status != "False" || c.Reason != "StorageS3Unavailable" || !strings.Contains(strings.ToLower(c.Message), "storage s3") {
							t.Fatalf("lost dependency condition: %+v", c)
						}
					}
				}
				if !found {
					t.Fatal("missing", typ)
				}
			}
			for _, member := range status.Members {
				if member.Name != m.Name && !member.Health {
					t.Fatalf("unrelated member became unhealthy: %s", member.Name)
				}
			}
			instance.mu.Lock()
			at = clockNow(rc)
			instance.s3CheckedAt = &at
			instance.ready, instance.reason, instance.message = true, "", ""
			instance.mu.Unlock()
			status, err = p.Status(t.Context(), rc)
			if err != nil || status.Phase != domain.PhaseReady {
				t.Fatalf("dependency recovery phase=%s err=%v", status.Phase, err)
			}
		})
	}
	// Older agents omit the observation; rolling members is recommended, not a
	// permanent readiness prerequisite for a mixed-version deployment.
	instance.mu.Lock()
	instance.s3CheckedAt = nil
	instance.mu.Unlock()
	status, err := p.Status(t.Context(), rc)
	if err != nil || status.Phase != domain.PhaseReady {
		t.Fatalf("legacy observation rejected: %s %v", status.Phase, err)
	}
}

func TestStorageSlowInstancesFitObservationSweep(t *testing.T) {
	for _, tc := range []struct {
		name     string
		replicas int
		delay    time.Duration
		failed   bool
	}{
		{"five-healthy", 5, 1100 * time.Millisecond, false},
		{"three-dependency-timeouts", 3, 1900 * time.Millisecond, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, rc, backend := setupProvider(t)
			if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), &domain.BackupStorage{
				Name: "objects", Namespace: "default", Type: "s3", EndpointURL: "https://objects.example.test",
				Bucket: "objects", Region: "test", VerifyTLS: true,
				Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "test-value"},
			}); err != nil {
				t.Fatal(err)
			}
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"backendStorage":"objects"}}`)
			desired := rc.Cluster.Spec.Components.Extra["storage"]
			desired.Replicas = tc.replicas
			rc.Cluster.Spec.Components.Extra["storage"] = desired
			convergeProvider(t, p, rc)
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := state.Members["storage"]
			var containers []infra.Container
			instances := map[string]*gatewayInstanceFixture{}
			first := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, nil)
			instances["gateway-a"] = first
			for i := range tc.replicas {
				instance := first
				if i != 0 {
					instance = newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, first.server.TLS.Clone())
				}
				at := clockNow(rc)
				instance.delay = tc.delay
				instance.s3CheckedAt = &at
				if tc.failed {
					instance.ready = false
					instance.reason = "StorageS3Unavailable"
					instance.message = "Storage S3 dependency: observation timed out"
				}
				host := fmt.Sprintf("storage-%d", i)
				instances[host] = instance
				containers = append(containers, infra.Container{ID: host, Hostname: host, Status: infra.ContainerRunning})
			}
			rc.Backend = &componentInventoryBackend{testBackend: backend, member: m.Name, instances: containers}
			rc.Agents = gatewayInstanceAgents{AgentDialer: rc.Agents, member: m.Name, instances: instances}
			status, err := engine.ObserveStatus(t.Context(), p, rc)
			if err != nil || status.ObservedAt.IsZero() {
				t.Fatalf("bounded instances exhausted the sweep: phase=%s observed=%v err=%v", status.Phase, status.ObservedAt, err)
			}
			want := domain.PhaseReady
			if tc.failed {
				want = domain.PhaseDegraded
			}
			if status.Phase != want {
				t.Fatalf("phase=%s want=%s", status.Phase, want)
			}
			for _, member := range status.Members {
				if member.Name != m.Name && (!member.Health || !member.Agent.Healthy) {
					t.Errorf("slow dependency withdrew healthy sibling %s", member.Name)
				}
			}
			if tc.failed {
				for _, typ := range []string{"Ready", "ComponentsReady"} {
					found := false
					for _, condition := range status.Conditions {
						if condition.Type == typ {
							found = condition.Status == "False" && condition.Reason == "StorageS3Unavailable"
						}
					}
					if !found {
						t.Error("lost precise dependency condition", typ)
					}
				}
			}
		})
	}
}

func TestStorageDependencyClockSkewAcrossRefreshes(t *testing.T) {
	p, rc, deployed := storageDeployFailureFixture(t, infra.OpState{Done: true})
	convergeProvider(t, p, rc)
	state, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := state.Members["storage"]
	instance := newGatewayInstanceFixture(t, m.Name, m.ConfigVersion, nil)
	rc.Backend = &componentInventoryBackend{testBackend: deployed.testBackend, member: m.Name, instances: []infra.Container{{ID: "storage-instance", Hostname: "storage-instance", Status: infra.ContainerRunning}}}
	rc.Agents = gatewayInstanceAgents{AgentDialer: rc.Agents, member: m.Name, instances: map[string]*gatewayInstanceFixture{"gateway-a": instance, "storage-instance": instance}}
	base := clockNow(rc)
	observe := func(t *testing.T, now, checkedAt time.Time, nativeReady, wantReady bool) {
		t.Helper()
		rc.Clock = importReplayClock{at: now}
		instance.mu.Lock()
		instance.s3CheckedAt = &checkedAt
		instance.ready, instance.reason, instance.message = nativeReady, "", ""
		if !nativeReady {
			instance.reason = "StorageS3Unavailable"
			instance.message = "Storage S3 dependency: authenticated bucket HEAD unavailable"
		}
		instance.mu.Unlock()
		status, err := p.Status(t.Context(), rc)
		if err != nil {
			t.Fatal(err)
		}
		wantPhase := domain.PhaseReady
		if !wantReady {
			wantPhase = domain.PhaseDegraded
		}
		if status.Phase != wantPhase {
			t.Errorf("checkedAt delta=%s nativeReady=%v: phase=%s want=%s", checkedAt.Sub(now), nativeReady, status.Phase, wantPhase)
		}
		for _, typ := range []string{"Ready", "ComponentsReady"} {
			found := false
			for _, condition := range status.Conditions {
				if condition.Type == typ {
					found = true
					if (condition.Status == "True") != wantReady || !wantReady && condition.Reason != "StorageS3Unavailable" {
						t.Errorf("checkedAt delta=%s nativeReady=%v: unexpected %s %+v", checkedAt.Sub(now), nativeReady, typ, condition)
					}
				}
			}
			if !found {
				t.Errorf("missing %s", typ)
			}
		}
	}
	for cycle := range 3 {
		now := base.Add(time.Duration(cycle) * engine.ReadinessInterval)
		checkedAt := now.Add(12 * time.Millisecond)
		t.Run(fmt.Sprintf("refresh-%d", cycle), func(t *testing.T) {
			observe(t, now, checkedAt, true, true)
		})
		t.Run(fmt.Sprintf("cached-%d", cycle), func(t *testing.T) {
			observe(t, now.Add(engine.ReadinessInterval-time.Second), checkedAt, true, true)
		})
	}
	for _, tc := range []struct {
		name        string
		checkedAt   time.Time
		nativeReady bool
		wantReady   bool
	}{
		{"forward-boundary", base.Add(time.Second), true, true},
		{"past-boundary", base.Add(-engine.ReadinessMaxAge), true, true},
		{"excess-forward-skew", base.Add(time.Second + time.Nanosecond), true, false},
		{"large-future", base.Add(time.Minute), true, false},
		{"expired", base.Add(-engine.ReadinessMaxAge - time.Nanosecond), true, false},
		{"zero", time.Time{}, true, false},
		{"head-failure-with-small-skew", base.Add(12 * time.Millisecond), false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observe(t, base, tc.checkedAt, tc.nativeReady, tc.wantReady)
		})
	}
}
