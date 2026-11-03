// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"net/http"
	"reflect"
	"slices"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

type upgradeBackend struct {
	*fake.Backend
	names    []string
	onDeploy func()
	specs    []infra.RuntimeSpec
}

func (b *upgradeBackend) Deploy(ctx context.Context, name string, d infra.Deployment) (infra.Op, error) {
	b.names = append(b.names, name)
	b.specs = append(b.specs, d.Runtime)
	op, e := b.Backend.Deploy(ctx, name, d)
	if e == nil && b.onDeploy != nil {
		b.onDeploy()
	}
	return op, e
}
func upgradeOperation(include bool) *domain.Operation {
	raw, _ := json.Marshal(engine.AgentUpgradePayload{TargetBuild: engine.CurrentAgentBuild().Build, IncludeCurrent: include})
	return &domain.Operation{ID: "agents", Kind: domain.OperationUpgradeAgents, Payload: raw}
}
func upgradeTick(t *testing.T, f *coreFixture, op *domain.Operation) engine.OpResult {
	t.Helper()
	f.p = New(f.p.options)
	r, e := f.p.RunOperation(t.Context(), f.rc, op)
	if e != nil {
		t.Fatal(e)
	}
	op.Checkpoint = r.Checkpoint
	return r
}
func TestAgentUpgradeCatalogOrderReplayAndPreservation(t *testing.T) {
	f := coreSetup(t, 4)
	s := coreConverge(t, f)
	if !slices.Contains(f.p.Describe().Operations, domain.OperationUpgradeAgents) {
		t.Fatal("missing operation")
	}
	before, _ := json.Marshal(s.Members)
	configVersion := s.ConfigVersion
	wrapped := &upgradeBackend{Backend: f.backend, onDeploy: func() { f.agentBuild = agentapi.AgentBuild{Build: engine.CurrentAgentBuild().Build} }}
	f.rc.Backend = wrapped
	op := upgradeOperation(true)
	done := false
	for range 80 {
		if upgradeTick(t, f, op).Done {
			done = true
			break
		}
	}
	want := []string{s.Members[3].Name, s.Members[1].Name, s.Members[2].Name, s.Members[0].Name}
	if !done || !reflect.DeepEqual(wrapped.names, want) {
		t.Fatalf("done=%v order=%v want=%v", done, wrapped.names, want)
	}
	for i, name := range wrapped.names {
		expected, err := f.p.runtimeSpec(f.rc, s.find(name), s.find(name).Version)
		if err != nil || !reflect.DeepEqual(wrapped.specs[i], expected) {
			t.Fatal("changed deployed runtime", name, err)
		}
	}
	after, _ := load(f.rc.Cluster)
	raw, _ := json.Marshal(after.Members)
	if string(before) != string(raw) || after.ConfigVersion != configVersion {
		t.Fatal("agent rollout changed config/version/resources/volume")
	}
}
func TestAgentUpgradeSkipAndIncludeCurrent(t *testing.T) {
	for _, include := range []bool{false, true} {
		t.Run(map[bool]string{false: "skip", true: "include"}[include], func(t *testing.T) {
			f := coreSetup(t, 1)
			coreConverge(t, f)
			f.agentBuild = agentapi.AgentBuild{Build: engine.CurrentAgentBuild().Build}
			wrapped := &upgradeBackend{Backend: f.backend}
			f.rc.Backend = wrapped
			op := upgradeOperation(include)
			done := false
			for range 20 {
				if upgradeTick(t, f, op).Done {
					done = true
					break
				}
			}
			if !done || len(wrapped.names) != map[bool]int{false: 0, true: 1}[include] {
				t.Fatal(done, wrapped.names)
			}
		})
	}
}
func TestAgentUpgradeDisruptionGates(t *testing.T) {
	for _, gate := range []string{"unready", "quorum", "replication", "collection", "pending", "build"} {
		t.Run(gate, func(t *testing.T) {
			rf := 3
			f := coreSetup(t, 3, func(w http.ResponseWriter, r *http.Request) bool {
				if r.URL.Path != "/v1/schema" {
					return false
				}
				json.NewEncoder(w).Encode(map[string]any{"classes": []any{map[string]any{"class": "Books", "replicationConfig": map[string]int{"factor": rf}}}})
				return true
			})
			s := coreConverge(t, f)
			wrapped := &upgradeBackend{Backend: f.backend}
			f.rc.Backend = wrapped
			op := upgradeOperation(true)
			switch gate {
			case "unready":
				f.readinessStale = true
			case "quorum":
				s.Voters = s.Voters[:2]
				s.save(f.rc.Cluster)
				if e := f.p.agentUpgradeSafe(t.Context(), f.rc, s, s.Members[1]); e == nil {
					t.Fatal("voting majority loss accepted")
				}
				if len(wrapped.names) != 0 {
					t.Fatal("quorum check deployed")
				}
				return
			case "replication":
				f.rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"replicationMinimumFactor":1}`)
				coreConverge(t, f)
				wrapped.names = nil
			case "collection":
				rf = 1
			case "pending":
				s.Pending = &pending{Action: "stop", Member: s.Members[0].Name}
				s.save(f.rc.Cluster)
			case "build":
				op.Payload = json.RawMessage(`{"targetBuild":"different"}`)
			}
			refused := false
			for range 5 {
				r, e := f.p.RunOperation(t.Context(), f.rc, op)
				if e != nil {
					refused = true
					break
				}
				op.Checkpoint = r.Checkpoint
			}
			if !refused || len(wrapped.names) != 0 {
				t.Fatal("unsafe redeploy", refused, wrapped.names)
			}
		})
	}
}
func TestAgentUpgradeWindowBuildAckAndCancellation(t *testing.T) {
	f := coreSetup(t, 3)
	coreConverge(t, f)
	wrapped := &upgradeBackend{Backend: f.backend}
	f.rc.Backend = wrapped
	op := upgradeOperation(true)
	f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + time.Now().UTC().Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
	for range 4 {
		upgradeTick(t, f, op)
	}
	if len(wrapped.names) != 0 {
		t.Fatal("closed window deployed")
	}
	f.rc.Cluster.Spec.Maintenance = nil
	for range 10 {
		upgradeTick(t, f, op)
		if len(wrapped.names) > 0 {
			break
		}
	}
	if len(wrapped.names) != 1 {
		t.Fatal(wrapped.names)
	}
	f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + time.Now().UTC().Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
	for range 3 {
		r, e := f.p.CancelOperation(t.Context(), f.rc, op)
		if e != nil || r.Done {
			t.Fatal("abandoned in-flight old build", r, e)
		}
		op.Checkpoint = r.Checkpoint
	}
	f.agentBuild = agentapi.AgentBuild{Build: engine.CurrentAgentBuild().Build}
	f.quorum = false
	r, e := f.p.CancelOperation(t.Context(), f.rc, op)
	if e != nil || r.Done {
		t.Fatal("unsynchronized cluster acknowledged", r, e)
	}
	op.Checkpoint = r.Checkpoint
	f.quorum = true
	done := false
	for range 6 {
		r, e = f.p.CancelOperation(t.Context(), f.rc, op)
		if e != nil {
			t.Fatal(e)
		}
		op.Checkpoint = r.Checkpoint
		if r.Done {
			done = true
			break
		}
	}
	if !done || len(wrapped.names) != 1 {
		t.Fatal("did not stop between members", done, wrapped.names)
	}
}

func TestAgentUpgradeRejectsSoleShardReplica(t *testing.T) {
	var f *coreFixture
	f = coreSetup(t, 3, func(w http.ResponseWriter, r *http.Request) bool {
		switch r.URL.Path {
		case "/v1/schema":
			json.NewEncoder(w).Encode(map[string]any{"classes": []any{map[string]any{"class": "Books", "replicationConfig": map[string]int{"factor": 3}}}})
		case "/v1/replication/sharding-state":
			s, _ := load(f.rc.Cluster)
			json.NewEncoder(w).Encode(map[string]any{"shardingState": map[string]any{"collection": "Books", "shards": []any{map[string]any{"shard": "hot", "replicas": []string{s.Members[1].Name}}}}})
		default:
			return false
		}
		return true
	})
	s := coreConverge(t, f)
	if err := f.p.agentUpgradeSafe(t.Context(), f.rc, s, s.Members[1]); err == nil {
		t.Fatal("configured replication factor hid a sole physical replica")
	}
}

func TestAgentUpgradeQueuedWindowAndFrozenTarget(t *testing.T) {
	f := coreSetup(t, 3)
	coreConverge(t, f)
	wrapped := &upgradeBackend{Backend: f.backend}
	f.rc.Backend = wrapped
	op := upgradeOperation(true)
	upgradeTick(t, f, op)
	// Payload changes after the first durable journal must not retarget the roll.
	op.Payload = json.RawMessage(`{"targetBuild":"different"}`)
	upgradeTick(t, f, op)
	f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + time.Now().UTC().Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
	upgradeTick(t, f, op)
	if len(wrapped.names) != 0 {
		t.Fatal("queued deployment bypassed a newly closed window")
	}
	f.rc.Cluster.Spec.Maintenance = nil
	upgradeTick(t, f, op)
	if len(wrapped.names) != 1 {
		t.Fatal("queued deployment did not resume", wrapped.names)
	}
	s, _ := load(f.rc.Cluster)
	// Simulate a control-plane build change while a platform receipt is in flight.
	s.Rolling.AgentTarget.TargetBuild = "frozen-old-control-plane"
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	f.agentBuild = agentapi.AgentBuild{Build: "frozen-old-control-plane"}
	upgradeTick(t, f, op)
	upgradeTick(t, f, op)
	if _, err := f.p.RunOperation(t.Context(), f.rc, op); err == nil {
		t.Fatal("new member bypassed the frozen build fence")
	}
	if len(wrapped.names) != 1 {
		t.Fatal("build mismatch deployed another member")
	}
}

func TestAgentUpgradeRejectsRecoveryAndReplacementJournals(t *testing.T) {
	for _, journal := range []string{"receipt", "import", "lost-volume", "replacement", "rolling"} {
		t.Run(journal, func(t *testing.T) {
			f := coreSetup(t, 3)
			s := coreConverge(t, f)
			switch journal {
			case "receipt":
				s.Members[0].RuntimeRecovery.Op = &infra.Op{ID: "recovery"}
			case "import":
				s.Members[0].RuntimeRecovery.ImportPlan = &engine.ImportPlan{}
			case "lost-volume":
				s.Members[0].RuntimeRecovery.VolumeLost = true
			case "replacement":
				s.Pending = &pending{Action: "delete-runtime", Member: s.Members[0].Name}
			case "rolling":
				s.Rolling = &rolling{Reason: "restart", OperationID: "restart"}
			}
			if err := s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			wrapped := &upgradeBackend{Backend: f.backend}
			f.rc.Backend = wrapped
			if _, err := f.p.RunOperation(t.Context(), f.rc, upgradeOperation(true)); err == nil || len(wrapped.names) != 0 {
				t.Fatal("existing structural work did not block rollout", err, wrapped.names)
			}
		})
	}
}

func TestAgentUpgradeCancelsWhileWaitingForWindow(t *testing.T) {
	f := coreSetup(t, 3)
	coreConverge(t, f)
	f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + time.Now().UTC().Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
	wrapped := &upgradeBackend{Backend: f.backend}
	f.rc.Backend = wrapped
	op := upgradeOperation(true)
	upgradeTick(t, f, op)
	upgradeTick(t, f, op)
	var canceller engine.OperationCanceller = f.p
	if err := canceller.CanCancelOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	result, err := canceller.CancelOperation(t.Context(), f.rc, op)
	if err != nil || !result.Done || len(wrapped.names) != 0 {
		t.Fatal("window wait cancellation did not stop safely", result, err)
	}
	s, _ := load(f.rc.Cluster)
	if s.Rolling != nil || s.Pending != nil {
		t.Fatal("cancel left an active disruption journal")
	}
}

func TestAgentUpgradeDeferredSubmissionRechecksWindowAndCancellation(t *testing.T) {
	for _, action := range []string{"window", "cancel"} {
		t.Run(action, func(t *testing.T) {
			f := coreSetup(t, 3)
			coreConverge(t, f)
			wrapped := &upgradeBackend{Backend: f.backend}
			f.rc.Backend = wrapped
			op := upgradeOperation(true)
			upgradeTick(t, f, op)
			upgradeTick(t, f, op)
			s, _ := load(f.rc.Cluster)
			s.Pending.RetryAt = time.Now().Add(time.Minute)
			if err := s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			upgradeTick(t, f, op) // Pending owner defers without a receipt.
			s, _ = load(f.rc.Cluster)
			if s.Rolling.Step != "deploy" || s.Pending.Op != nil {
				t.Fatal("deferred submission was recorded as accepted")
			}
			s.Pending.RetryAt = time.Time{}
			if err := s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if action == "window" {
				f.rc.Cluster.Spec.Maintenance = &domain.MaintenancePolicy{Windows: []domain.MaintenanceWindow{{Schedule: "0 " + time.Now().UTC().Add(2*time.Hour).Format("15") + " * * *", Duration: "1h"}}}
				upgradeTick(t, f, op)
				if len(wrapped.names) != 0 {
					t.Fatal("deferred submission bypassed the closed window")
				}
				f.rc.Cluster.Spec.Maintenance = nil
				f.readinessStale = true
				if _, err := f.p.RunOperation(t.Context(), f.rc, op); err == nil {
					t.Fatal("deferred submission bypassed native readiness")
				}
			} else {
				f.readinessStale = true
				result, err := f.p.CancelOperation(t.Context(), f.rc, op)
				if err != nil || result.Done {
					t.Fatal("ambiguous intent abandoned an unready member", result, err)
				}
				op.Checkpoint = result.Checkpoint
				f.readinessStale = false
				result, err = f.p.CancelOperation(t.Context(), f.rc, op)
				if err != nil || !result.Done {
					t.Fatal("settled deferred intent could not cancel", result, err)
				}
			}
			if len(wrapped.names) != 0 {
				t.Fatal("deferred cancellation/readiness gate deployed")
			}
		})
	}
}

func TestAgentUpgradeFailedBuildFenceCannotResumeThroughSync(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(map[bool]string{false: "gate", true: "intent"}[queued], func(t *testing.T) {
			f := coreSetup(t, 3)
			coreConverge(t, f)
			wrapped := &upgradeBackend{Backend: f.backend}
			f.rc.Backend = wrapped
			op := upgradeOperation(true)
			upgradeTick(t, f, op)
			if queued {
				upgradeTick(t, f, op)
			}
			s, _ := load(f.rc.Cluster)
			s.Rolling.AgentTarget.TargetBuild = "previous-control-plane"
			if err := s.save(f.rc.Cluster); err != nil {
				t.Fatal(err)
			}
			if _, err := f.p.RunOperation(t.Context(), f.rc, op); err == nil {
				t.Fatal("missing frozen build fence")
			}
			op.Phase = "Failed"
			for range 5 {
				if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
					t.Fatal(err)
				}
			}
			s, _ = load(f.rc.Cluster)
			if len(wrapped.names) != 0 || s.Rolling != nil || s.Pending != nil {
				t.Fatal("ordinary Sync resumed a failed agent rollout", wrapped.names, s.Rolling, s.Pending)
			}
		})
	}
}

func TestAgentUpgradeSyncRecoversAcceptedMemberOnly(t *testing.T) {
	f := coreSetup(t, 3)
	coreConverge(t, f)
	wrapped := &upgradeBackend{Backend: f.backend}
	f.rc.Backend = wrapped
	op := upgradeOperation(true)
	for range 3 {
		upgradeTick(t, f, op)
	}
	s, _ := load(f.rc.Cluster)
	s.Rolling.AgentTarget.TargetBuild = "previous-control-plane"
	if err := s.save(f.rc.Cluster); err != nil {
		t.Fatal(err)
	}
	if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
		t.Fatal(err)
	}
	s, _ = load(f.rc.Cluster)
	if s.Rolling == nil {
		t.Fatal("Sync abandoned an accepted member before build acknowledgement")
	}
	f.agentBuild = agentapi.AgentBuild{Build: "previous-control-plane"}
	for range 4 {
		if _, err := f.p.Sync(t.Context(), f.rc); err != nil {
			t.Fatal(err)
		}
	}
	s, _ = load(f.rc.Cluster)
	if len(wrapped.names) != 1 || s.Rolling != nil || s.Pending != nil {
		t.Fatal("Sync did not retire safely between members", wrapped.names)
	}
}

func TestAgentUpgradeMissingReceiptCancellationSettlesTargetBuild(t *testing.T) {
	f := coreSetup(t, 3)
	coreConverge(t, f)
	wrapped := &upgradeBackend{Backend: f.backend}
	f.rc.Backend = wrapped
	op := upgradeOperation(true)
	upgradeTick(t, f, op)
	upgradeTick(t, f, op)
	// Simulate a platform acceptance followed by a crash before its receipt
	// reached the journal: the durable state still contains only intent.
	f.agentBuild = agentapi.AgentBuild{Build: engine.CurrentAgentBuild().Build}
	f.readinessStale = true
	result, err := f.p.CancelOperation(t.Context(), f.rc, op)
	if err != nil || result.Done {
		t.Fatal("cancel abandoned accepted-but-unrecorded deployment", result, err)
	}
	op.Checkpoint = result.Checkpoint
	f.readinessStale = false
	result, err = f.p.CancelOperation(t.Context(), f.rc, op)
	if err != nil || !result.Done || len(wrapped.names) != 0 {
		t.Fatal("cancel failed to settle without resubmission", result, err, wrapped.names)
	}
}
