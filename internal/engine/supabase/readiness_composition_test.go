// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package supabase

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/reconciler"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type readinessPublicationProvider struct {
	*Provider
	synced chan time.Time
}

func (p readinessPublicationProvider) Sync(ctx context.Context, rc *engine.Context) (engine.Result, error) {
	result, err := p.Provider.Sync(ctx, rc)
	select {
	case p.synced <- time.Now():
	default:
	}
	return result, err
}

func exhaustedStorageReadinessFixture(t *testing.T) (*Provider, *engine.Context, string) {
	t.Helper()
	p, rc, original := storageDeployFailureFixture(t, infra.OpState{Status: "PREPARING_RUNTIME"})
	rc.Backend = &deploymentGuardBackend{
		Backend: rc.Backend, target: original.member, pendingID: "version:storage-version",
		siblings: map[string]bool{}, observed: map[string]bool{},
	}
	if _, err := p.Sync(t.Context(), rc); !errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
		t.Fatalf("expected exhausted Storage deployment: %v", err)
	}
	return p, rc, original.member
}

func assertExhaustedStorageReadiness(t *testing.T, status domain.ClusterStatus, member string) {
	t.Helper()
	if status.Phase != domain.PhaseDegraded {
		t.Errorf("exhausted required component phase=%s, want Degraded", status.Phase)
	}
	for _, typ := range []string{"Ready", "ComponentsReady", "Deployment/" + member} {
		reason := "ComponentDeploymentFailed"
		if typ == "Deployment/"+member {
			reason = "DeploymentRetriesExhausted"
		}
		found := false
		for _, condition := range status.Conditions {
			if condition.Type == typ {
				found = true
				if condition.Status != "False" || condition.Reason != reason || condition.Message == "" {
					t.Errorf("contradictory %s: %+v", typ, condition)
				}
			}
		}
		if !found {
			t.Errorf("missing %s: %+v", typ, status.Conditions)
		}
	}
}

func TestStorageExhaustionComposesReadyThroughWorkerPublication(t *testing.T) {
	p, rc, member := exhaustedStorageReadinessFixture(t)
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	// This is the raw provider snapshot, including the core's guard overlay.
	assertExhaustedStorageReadiness(t, status, member)
	applied := rc.Cluster.Status.ObservedGeneration
	st := rc.Store.(*store.Store)
	if err := st.CreateCluster(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	publishing := readinessPublicationProvider{Provider: p, synced: make(chan time.Time, 1)}
	registry, err := engine.NewRegistry(publishing)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := reconciler.New(reconciler.Options{
		Store: st, Registry: registry, Context: *rc, ResyncInterval: time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	done := make(chan error, 1)
	go func() { done <- manager.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("manager shutdown: %v", err)
		}
	})
	// Poll only authoritative publication, not Sync-mutated fixture state.
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	var synchronizedAt time.Time
	select {
	case synchronizedAt = <-publishing.synced:
	case <-deadline.C:
		t.Fatal("worker did not execute Sync")
	}
	for {
		published, err := st.GetCluster(t.Context(), rc.Cluster.Ref())
		if err != nil {
			t.Fatal(err)
		}
		observed := false
		for _, condition := range published.Status.Conditions {
			observed = observed || condition.Type == "ComponentsReady" && condition.Status == "False"
		}
		if observed && !published.Status.ObservedAt.Before(synchronizedAt) {
			assertExhaustedStorageReadiness(t, published.Status, member)
			if published.Status.ObservedGeneration != applied || published.Status.ObservedGeneration >= published.Generation {
				t.Errorf("worker acknowledged unresolved generation: desired=%d observed=%d prior=%d", published.Generation, published.Status.ObservedGeneration, applied)
			}
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("worker did not publish fresh component failure: %+v", published.Status)
		case <-ticker.C:
		}
	}
}

func TestStorageExhaustionPreservesCoreReadinessFailure(t *testing.T) {
	p, rc, _ := exhaustedStorageReadinessFixture(t)
	backend := rc.Backend.(*deploymentGuardBackend).Backend.(*failedStorageDeployBackend).testBackend
	backend.stopped = map[string]bool{rc.Cluster.Name + "p0": true}
	core, err := p.Core.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	var coreReady domain.Condition
	for _, condition := range core.Conditions {
		if condition.Type == "Ready" {
			coreReady = condition
		}
	}
	if coreReady.Status != "False" {
		t.Fatalf("fixture did not produce genuine core failure: %+v", core.Conditions)
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, condition := range status.Conditions {
		if condition.Type == "Ready" {
			if condition.Status != coreReady.Status || condition.Reason != coreReady.Reason || condition.Message != coreReady.Message {
				t.Errorf("component failure replaced core diagnosis: got=%+v core=%+v", condition, coreReady)
			}
			return
		}
	}
	t.Fatal("combined status lost core Ready condition")
}

func TestReadyCompositionIgnoresDisabledOptionalComponents(t *testing.T) {
	p, rc, backend := setupProvider(t)
	backend.gatewayHosts["storage"] = 0
	convergeProvider(t, p, rc)
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	if status.Phase != domain.PhaseReady {
		t.Fatalf("healthy minimal stack phase=%s", status.Phase)
	}
	for _, typ := range []string{"Ready", "ComponentsReady"} {
		found := false
		for _, condition := range status.Conditions {
			if condition.Type == typ {
				found = condition.Status == "True"
			}
		}
		if !found {
			t.Errorf("disabled optional Storage withdrew %s: %+v", typ, status.Conditions)
		}
	}
}

// Resume an already formed HA core using the same durable joined-member
// fixture as replacement tests, retaining the converged Supabase identities.
func disabledAnalyticsReadinessFixture(t *testing.T) (*Provider, *engine.Context, *testBackend) {
	t.Helper()
	p, rc, backend := setupProvider(t)
	for _, name := range []string{"analytics", "storage"} {
		desired := rc.Cluster.Spec.Components.Extra[name]
		desired.Replicas = 1
		rc.Cluster.Spec.Components.Extra[name] = desired
	}
	convergeProvider(t, p, rc)
	var root map[string]json.RawMessage
	if err := json.Unmarshal(rc.Cluster.Internal, &root); err != nil {
		t.Fatal(err)
	}
	var core map[string]json.RawMessage
	if err := json.Unmarshal(root["postgres"], &core); err != nil {
		t.Fatal(err)
	}
	var members []map[string]json.RawMessage
	if err := json.Unmarshal(core["members"], &members); err != nil {
		t.Fatal(err)
	}
	var primary map[string]json.RawMessage
	for _, member := range members {
		if string(member["name"]) == `"demop0"` {
			primary = member
		}
	}
	if primary == nil {
		t.Fatal("missing joined primary fixture")
	}
	backend.nativeStatus = map[string]pgspec.StatusResponse{}
	for i := range 3 {
		name := fmt.Sprintf("demop%d", i)
		native := pgspec.StatusResponse{
			Running: true, Role: "replica", Leader: "demop0", Streaming: true,
			EtcdHealthy: true, EtcdMemberID: name + "-etcd",
			Version: rc.Cluster.Spec.Version, SystemID: "644", Timeline: 1,
			WALPosition: "0/1000000", SlotStatusKnown: true, WALStatusKnown: true,
			ArchiveStatusKnown: true,
		}
		if i == 0 {
			native.Role = "primary"
		}
		backend.nativeStatus[name] = native
		if i == 0 {
			continue
		}
		value := "joined-fixture-" + name
		if err := rc.Secrets.SetAgentToken(t.Context(), &domain.AgentToken{
			Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name,
			Member: name, Token: value, Hash: secrets.Hash(value),
		}); err != nil {
			t.Fatal(err)
		}
		if _, err := backend.ImportServices(t.Context(), []infra.ServiceSpec{
			engine.VolumeServiceSpec(rc.Cluster.Name, name, name+"v", value, rc.Cluster.Spec.Components.Engine.Resources),
			engine.RuntimeServiceSpec(backend.Layout(), engine.MemberService{
				Cluster: rc.Cluster.Name, Member: name, Token: value, Volume: name + "v",
				Resources: rc.Cluster.Spec.Components.Engine.Resources, Containers: 1,
			}),
		}); err != nil {
			t.Fatal(err)
		}
		backend.deployed[name] = true
		member := map[string]json.RawMessage{}
		for key, value := range primary {
			member[key] = value
		}
		member["name"], _ = json.Marshal(name)
		member["index"], _ = json.Marshal(i)
		member["serviceId"], _ = json.Marshal(name + "-id")
		member["volume"], _ = json.Marshal(name + "v")
		member["volumeId"], _ = json.Marshal(name + "v-id")
		member["etcdId"], _ = json.Marshal(name + "-etcd")
		member["etcdState"] = json.RawMessage(`"existing"`)
		members = append(members, member)
	}
	core["members"], _ = json.Marshal(members)
	core["topology"] = json.RawMessage(`"ha"`)
	core["formed"] = json.RawMessage(`true`)
	core["nextIndex"] = json.RawMessage(`3`)
	root["postgres"], _ = json.Marshal(core)
	rc.Cluster.Internal, _ = json.Marshal(root)
	rc.Cluster.Spec.Topology.Type = "ha"
	rc.Cluster.Spec.Components.Engine.Replicas = 3
	convergeProvider(t, p, rc)
	return p, rc, backend
}

type readinessCleanupBackend struct {
	*testBackend
	deletes []string
	polls   int
	release bool
}

func (b *readinessCleanupBackend) DeleteServiceByID(ctx context.Context, name, id string) (infra.Op, error) {
	if name != "demoan" {
		return b.testBackend.DeleteServiceByID(ctx, name, id)
	}
	if service := b.services[name]; service == nil || service.ID != id {
		return infra.Op{}, infra.ErrConflict
	}
	b.deletes = append(b.deletes, name)
	return infra.Op{ID: "analytics-cleanup", ServiceID: id}, nil
}

func (b *readinessCleanupBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID != "analytics-cleanup" {
		return b.testBackend.OpStatus(ctx, op)
	}
	b.polls++
	if !b.release {
		return infra.OpState{Status: "pending"}, nil
	}
	delete(b.services, "demoan")
	return infra.OpState{Done: true}, nil
}

func assertSlotReadinessBlocked(t *testing.T, p *Provider, rc *engine.Context, observed int64) {
	t.Helper()
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"Ready", "ReplicationSlots"} {
		found := false
		for _, condition := range status.Conditions {
			if condition.Type == typ {
				found = condition.Status == "False" && condition.Reason == "SlotWALRetention"
			}
		}
		if !found {
			t.Fatalf("cleanup hid %s slot failure: %+v", typ, status.Conditions)
		}
	}
	if rc.Cluster.Status.ObservedGeneration != observed {
		t.Fatalf("premature acknowledgement: got %d want %d", rc.Cluster.Status.ObservedGeneration, observed)
	}
}

func TestDisabledAnalyticsCleanupWhileHACoreRetainsWAL(t *testing.T) {
	for _, observed := range []int64{2, 4, 0} {
		t.Run(fmt.Sprint(observed), func(t *testing.T) {
			p, rc, backend := disabledAnalyticsReadinessFixture(t)
			// Reproduce the prior Storage resize/revert without touching the
			// database path, then accept the Analytics disable as generation 5.
			storage := rc.Cluster.Spec.Components.Extra["storage"]
			original := storage.Resources
			storage.Resources.MaxCPU++
			rc.Cluster.Spec.Components.Extra["storage"] = storage
			rc.Cluster.Generation = 3
			convergeProvider(t, p, rc)
			storage.Resources = original
			rc.Cluster.Spec.Components.Extra["storage"] = storage
			rc.Cluster.Generation = 4
			convergeProvider(t, p, rc)
			native := backend.nativeStatus["demop0"]
			native.ReplicationSlots = []pgspec.ReplicationSlot{{
				Name: "cainophile_dw1nslcr", Type: "logical", Active: true,
				RetainedBytes: 6 << 30, WALStatus: "extended",
			}}
			backend.nativeStatus["demop0"] = native
			analytics := rc.Cluster.Spec.Components.Extra["analytics"]
			analytics.Replicas = 0
			rc.Cluster.Spec.Components.Extra["analytics"] = analytics
			rc.Cluster.Generation = 5
			rc.Cluster.Status.ObservedGeneration = observed
			b := &readinessCleanupBackend{testBackend: backend}
			rc.Backend = b
			assertSlotReadinessBlocked(t, p, rc, observed)
			if _, err := p.Sync(t.Context(), rc); err != nil {
				t.Fatal(err)
			}
			if len(b.deletes) != 1 {
				t.Fatalf("disabled Analytics cleanup not submitted: %v", b.deletes)
			}
			for range 2 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				assertSlotReadinessBlocked(t, p, rc, observed)
			}
			if b.polls != 2 || len(b.deletes) != 1 {
				t.Fatalf("delete receipt lost serialization: polls=%d deletes=%v", b.polls, b.deletes)
			}
			b.release = true
			for range 3 {
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
			}
			state, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			if state.Members["analytics"] != nil || backend.services["demoan"] != nil {
				t.Fatal("delete receipt did not remove Analytics")
			}
			assertSlotReadinessBlocked(t, p, rc, observed)
			native.ReplicationSlots = nil
			backend.nativeStatus["demop0"] = native
			convergeProvider(t, p, rc)
			if rc.Cluster.Status.ObservedGeneration != 5 || len(b.deletes) != 1 {
				t.Fatal("fresh core readiness did not acknowledge generation without re-import")
			}
		})
	}
}
