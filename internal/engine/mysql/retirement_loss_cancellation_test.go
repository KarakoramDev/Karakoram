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
	"sync/atomic"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestGRConfirmedLossCancellation(t *testing.T) {
	for _, legacy := range []bool{false, true} {
		policy := "strict"
		if legacy {
			policy = "legacy"
		}
		for _, stage := range []string{"planned", "joined", "persisted-rollback", "persisted-handoff", "persisted-drain", "persisted-runtime-stop"} {
			t.Run(policy+"/"+stage, func(t *testing.T) {
				p, rc, b, s, lost := recoveryFixture(t)
				lost.ConfirmedVolumeLost, lost.RuntimeRecovery.VolumeLost = true, true
				delete(b.deployed, lost.Name)
				rc.Backend = physicallyLostBackend{fakeBackend: b, volume: lost.Volume, volumeID: lost.VolumeID}
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				if legacy {
					b.action = legacyGRStatus(b, false)
				}
				var nativeStops atomic.Int32
				var unavailableJoiner atomic.Bool
				action := b.action
				b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
					if name == "demom3" {
						if r.URL.Path == "/v1/mysql/"+myspec.ActionStopGroup {
							nativeStops.Add(1)
						}
						if unavailableJoiner.Load() && r.URL.Path == "/v1/mysql/status" {
							http.Error(w, "joiner unavailable", http.StatusServiceUnavailable)
							return true
						}
					}
					return action != nil && action(w, r, name)
				}
				op := &domain.Operation{ID: "cancel-lost", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost.Name + `"}`)}
				for tick := range 240 {
					result, err := p.RunOperation(t.Context(), rc, op)
					if err != nil {
						t.Fatal(err)
					}
					op.Checkpoint = result.Checkpoint
					current, _ := load(rc.Cluster)
					if stage == "planned" || current.find(current.Replacement.New).Stage == "joined" {
						break
					}
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					b.apply(t, p, rc)
					if tick == 239 {
						t.Fatal("joiner never joined")
					}
				}
				if _, err := p.CancelOperation(t.Context(), rc, op); err != nil {
					t.Fatal(err)
				}
				current, _ := load(rc.Cluster)
				if strings.HasPrefix(stage, "persisted-") {
					// An older CP may already have accepted rollback before this fix.
					current.Replacement.Recovery = "rollback"
					current.Replacement.Stage = "retire"
					next := current.find(current.Replacement.New)
					switch stage {
					case "persisted-handoff":
						next.Retirement = "handoff"
					case "persisted-drain":
						next.Removing, next.Retirement = true, "drain"
					case "persisted-runtime-stop":
						next.Removing, next.Offline, next.Retirement = true, true, "runtime-stop"
					}
					if err := save(rc.Cluster, current); err != nil {
						t.Fatal(err)
					}
					unavailableJoiner.Store(true)
					if _, err := New(Options{}).Sync(t.Context(), rc); err == nil {
						t.Fatal("unknown joiner observation authorized migration")
					}
					if nativeStops.Load() != 0 || len(b.stopped) != 0 {
						t.Fatal("unknown joiner observation authorized shutdown")
					}
					unavailableJoiner.Store(false)
				} else if stage == "joined" && current.Replacement.Recovery != "complete" {
					t.Fatal("cancellation would dismantle recovered quorum")
				}
				if !legacy && stage == "planned" {
					// Excluding the lost predecessor from required observers
					// must not hide its UUID if native views still contain it.
					b.membership = map[string]*myspec.GroupView{}
					for name, native := range b.native {
						if native.MemberState != "ONLINE" {
							continue
						}
						view := &myspec.GroupView{}
						for peer, st := range b.native {
							if st.MemberState == "ONLINE" {
								view.Members = append(view.Members, myspec.GroupViewMember{ID: st.ServerUUID, Host: peer, Port: 3306, State: st.MemberState, Role: st.MemberRole})
							}
						}
						view.Members = append(view.Members, myspec.GroupViewMember{ID: lost.ServerUUID, Host: lost.Name, Port: 3306, State: "UNREACHABLE"})
						b.membership[name] = view
					}
					if _, err := p.Sync(t.Context(), rc); err == nil || !strings.Contains(err.Error(), "identity convergence") {
						t.Fatalf("lost UUID row was ignored: %v", err)
					}
					if len(b.stopped) != 0 || len(b.deleted) != 0 {
						t.Fatal("divergent UUID view released cleanup")
					}
					clear(b.membership)
				}
				// Every tick reconstructs Core and reloads persisted state.
				day2Ticks(t, rc, b, func(s *state) bool { return s.Replacement == nil })
				current, _ = load(rc.Cluster)
				if stage != "planned" {
					if current.find(lost.Name) != nil || current.find("demom3") == nil {
						t.Fatal("joined cancellation did not complete forward")
					}
					if nativeStops.Load() != 0 {
						t.Fatal("joined replacement left its group during cancellation recovery")
					}
					for _, name := range b.stopped {
						if name == "demom3" {
							t.Fatal("joined replacement was stopped during cancellation recovery")
						}
					}
				} else {
					retained := current.find(lost.Name)
					if retained == nil || !retained.ConfirmedVolumeLost || current.find("demom3") != nil {
						t.Fatal("rollback lost the predecessor loss fence")
					}
					// Cleanup released replacement ownership; a fresh operation can recover
					// the still-lost predecessor with the unchanged admission safety gates.
					retry := &domain.Operation{ID: "retry-lost", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost.Name + `"}`)}
					runDay2Operation(t, rc, b, retry)
				}
				current, _ = load(rc.Cluster)
				if current.Replacement != nil || current.find(lost.Name) != nil || len(current.data()) != 3 {
					t.Fatal("loss recovery owner remained stuck")
				}
				// Executable maintenance is admitted only after recovery has restored all
				// configured members. Merely clearing the loss fence is never the remedy.
				if _, err := New(Options{}).RunOperation(t.Context(), rc, mysqlAgentRolloutOperation(true)); err != nil {
					t.Fatalf("upgrade admission after recovery: %v", err)
				}
			})
		}
	}
}

type stoppedRollbackAgents struct {
	engine.AgentDialer
	stopped string
}

func (a stoppedRollbackAgents) Dial(ctx context.Context, ref domain.ClusterRef, name string) (*agentapi.Client, error) {
	if name == a.stopped {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	return a.AgentDialer.Dial(ctx, ref, name)
}

type rollbackContextBackend struct{ infra.Backend }

func (b rollbackContextBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return b.Backend.GetService(ctx, name)
}

func TestGRLegacyStoppedRollbackUnavailablePredecessor(t *testing.T) {
	p, rc, b, s, lost := recoveryFixture(t)
	p.options.AutoReplaceLostVolumes = false
	lost.ConfirmedVolumeLost, lost.RuntimeRecovery.VolumeLost = true, true
	delete(b.deployed, lost.Name)
	rc.Backend = physicallyLostBackend{fakeBackend: b, volume: lost.Volume, volumeID: lost.VolumeID}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	b.action = legacyGRStatus(b, false)
	op := &domain.Operation{ID: "legacy-cancel", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost.Name + `"}`)}
	for tick := range 240 {
		result, err := p.RunOperation(t.Context(), rc, op)
		if err != nil {
			t.Fatal(err)
		}
		op.Checkpoint = result.Checkpoint
		s, _ = load(rc.Cluster)
		if s.find(s.Replacement.New).Stage == "joined" {
			break
		}
		if _, err := p.Sync(t.Context(), rc); err != nil {
			t.Fatal(err)
		}
		b.apply(t, p, rc)
		if tick == 239 {
			t.Fatal("joiner never joined")
		}
	}
	next := s.find(s.Replacement.New)
	next.Removing, next.Offline, next.Retirement = true, true, "stop"
	stop, err := b.Stop(t.Context(), next.Name)
	if err != nil {
		t.Fatal(err)
	}
	next.RetirementOp = &stop
	delete(b.deployed, next.Name)
	delete(b.native, next.Name)
	s.Replacement.Stage, s.Replacement.Recovery, s.Replacement.IdentityBarrier = "retire", "rollback", false
	next.IdentityBarrier = false
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if claimable, err := p.CanClaimOperation(rc.Cluster); err != nil || claimable {
		t.Fatalf("queued work bypassed persisted replacement: claimable=%v err=%v", claimable, err)
	}
	legacy := legacyGRStatus(b, false)
	unavailableSurvivor := ""
	b.action = func(w http.ResponseWriter, r *http.Request, name string) bool {
		if (name == lost.Name || name == unavailableSurvivor) && r.URL.Path == "/v1/mysql/status" {
			w.WriteHeader(http.StatusServiceUnavailable)
			_, _ = w.Write([]byte(`{"error":"MySQL native startup/status unavailable: MySQL server stopped"}`))
			return true
		}
		if name == next.Name {
			http.Error(w, "stopped member has no allocation", http.StatusServiceUnavailable)
			return true
		}
		return legacy(w, r, name)
	}
	if b.services[next.Name].Status != infra.StatusStopped {
		t.Fatal("retiree runtime is not stopped")
	}
	events := &placementSafetyEvents{}
	rc.Events = events
	unavailableSurvivor = s.data()[0].Name
	if _, err := p.Sync(t.Context(), rc); err == nil {
		t.Fatal("unavailable real survivor released retirement")
	}
	current, _ := load(rc.Cluster)
	if current.find(next.Name).Retirement != "stop" || len(b.deleted) != 0 {
		t.Fatal("unknown survivor observation became deletion proof")
	}
	unavailableSurvivor = ""
	rc.Backend = rollbackContextBackend{Backend: rc.Backend}
	rc.Agents = stoppedRollbackAgents{AgentDialer: rc.Agents, stopped: next.Name}
	// Reconciler pre-observation and Sync share one absolute read budget.
	// A disappeared retiree exhausts that budget without making its survivors
	// unavailable. Cleanup must still reach fresh retirement proofs.
	pass := engine.WithObservationPass(t.Context())
	_, _ = p.Status(pass, rc)
	if _, err := p.Sync(pass, rc); err != nil {
		t.Fatalf("pre-observation starved rollback cleanup: %v", err)
	}
	current, _ = load(rc.Cluster)
	if current.find(next.Name).Retirement != "delete" {
		t.Fatal("completed stop did not advance to delete")
	}
	rc.Agents = rc.Agents.(stoppedRollbackAgents).AgentDialer
	day2Ticks(t, rc, b, func(current *state) bool { return current.Replacement == nil })
	s, _ = load(rc.Cluster)
	if retained := s.find(lost.Name); retained == nil || !retained.ConfirmedVolumeLost || s.find(next.Name) != nil || b.services[next.Name] != nil || b.services[next.Volume] != nil {
		t.Fatal("stopped rollback did not preserve loss fence and remove only joiner")
	}
	for _, name := range b.deleted {
		if name != next.Name && name != next.Volume {
			t.Fatalf("rollback deleted another member's service: %s", name)
		}
	}
	if claimable, err := p.CanClaimOperation(rc.Cluster); err != nil || !claimable {
		t.Fatalf("queued work remained blocked after cleanup: claimable=%v err=%v", claimable, err)
	}
	found := false
	for _, event := range events.events {
		found = found || event.Reason == "LegacyGRRetirement"
	}
	if !found {
		t.Fatal("legacy retirement warning missing")
	}
	retry := &domain.Operation{ID: "queued-retry", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost.Name + `"}`)}
	runDay2Operation(t, rc, b, retry)
}

type refusedReplacementBackend struct {
	physicallyLostBackend
	provider string
	refuse   bool
	attempts int
	late     *lateReplacementImport
}

// This is server-side pending work, deliberately shared by reconstructed clients.
type lateReplacementImport struct {
	specs  []infra.ServiceSpec
	calls  int
	accept bool
}

func (b *refusedReplacementBackend) Name() string { return b.provider }

func (b *refusedReplacementBackend) ImportServices(ctx context.Context, specs []infra.ServiceSpec) (infra.Op, error) {
	b.attempts++
	if b.late != nil {
		b.late.calls++
		b.late.specs = append([]infra.ServiceSpec(nil), specs...)
		if b.late.accept {
			return infra.Op{ID: "accepted-import"}, nil
		}
		return infra.Op{}, context.DeadlineExceeded
	}
	if b.refuse {
		// Both adapters reject unsatisfiable placement before reserving services.
		return infra.Op{}, fmt.Errorf("%s placement: %w", b.provider, infra.ErrPlacementUnsatisfiable)
	}
	return b.fakeBackend.ImportServices(ctx, specs)
}

func (b *refusedReplacementBackend) Stop(ctx context.Context, name string) (infra.Op, error) {
	service := b.services[name]
	// The real adapter now supports a stopped-intent receipt for an imported
	// runtime, but refuses an unexpected native service in undeployed metadata.
	if b.provider == "dokploy" && !b.deployed[name] && service.Status != infra.StatusRunning && b.native[name].ServerUUID != "" {
		return infra.Op{}, fmt.Errorf("%w: undeployed application has an unexpected native service", infra.ErrConflict)
	}
	if _, err := b.fakeBackend.Stop(ctx, name); err != nil {
		return infra.Op{}, err
	}
	return infra.Op{ID: "stop", Services: map[string]string{name: service.ID}}, nil
}

func (b *refusedReplacementBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "stop" {
		for name, id := range op.Services {
			if service := b.services[name]; service == nil || service.ID != id {
				return infra.OpState{Done: b.provider == "nomad", Failed: true, Expired: true}, nil
			}
		}
	}
	return b.fakeBackend.OpStatus(ctx, op)
}

func restartReplacementFixture(t *testing.T, rc *engine.Context, b *refusedReplacementBackend) *refusedReplacementBackend {
	t.Helper()
	writer := rc.Store.(interface {
		UpdateClusterState(context.Context, *domain.Cluster) error
	})
	if err := writer.UpdateClusterState(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	cluster, err := rc.Store.GetCluster(t.Context(), rc.Cluster.Ref())
	if err != nil {
		t.Fatal(err)
	}
	rc.Cluster = cluster
	next := &refusedReplacementBackend{physicallyLostBackend: b.physicallyLostBackend, provider: b.provider, refuse: b.refuse, attempts: b.attempts, late: b.late}
	rc.Backend = next
	return next
}

func TestGRConfirmedLossCancellationProvisioningMatrix(t *testing.T) {
	for _, backend := range []string{"nomad", "dokploy"} {
		for _, legacy := range []bool{false, true} {
			policy := "strict"
			if legacy {
				policy = "legacy"
			}
			for _, point := range []string{"before-planning", "planned-before-provisioning", "provisioning-refused", "provisioning-in-flight", "provisioned-not-joined", "deployed-not-joined", "joined", "retiring-predecessor", "predecessor-retired"} {
				t.Run(backend+"/"+policy+"/"+point, func(t *testing.T) {
					p, rc, base, s, lost := recoveryFixture(t)
					lost.ConfirmedVolumeLost, lost.RuntimeRecovery.VolumeLost = true, true
					delete(base.deployed, lost.Name)
					b := &refusedReplacementBackend{physicallyLostBackend: physicallyLostBackend{fakeBackend: base, volume: lost.Volume, volumeID: lost.VolumeID}, provider: backend, refuse: point == "provisioning-refused"}
					rc.Backend = b
					if legacy {
						base.action = legacyGRStatus(base, false)
					}
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					op := &domain.Operation{ID: "cancel-provisioning", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost.Name + `"}`)}
					if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
						t.Fatal(err)
					}
					s, _ = load(rc.Cluster)
					nextName := s.Replacement.New
					reached := point == "before-planning"
					for tick := 0; !reached && tick < 240; tick++ {
						b = restartReplacementFixture(t, rc, b)
						_, syncErr := New(Options{}).Sync(t.Context(), rc)
						if syncErr != nil && !(point == "provisioning-refused" && errors.Is(syncErr, infra.ErrPlacementUnsatisfiable)) {
							t.Fatal(syncErr)
						}
						s, _ = load(rc.Cluster)
						next, old := s.find(nextName), s.find(lost.Name)
						switch point {
						case "planned-before-provisioning":
							reached = next.ImportBatch != nil && b.attempts == 0
						case "provisioning-refused":
							reached = errors.Is(syncErr, infra.ErrPlacementUnsatisfiable)
						case "provisioning-in-flight":
							reached = s.Pending != nil && s.Pending.Member == nextName && s.Pending.Action == "import"
						case "provisioned-not-joined":
							reached = next.Stage == "imported" && s.Pending == nil
						case "deployed-not-joined":
							reached = next.Stage == "deployed"
						case "joined":
							reached = next.Stage == "joined"
						case "retiring-predecessor":
							reached = old != nil && old.Retirement != ""
						case "predecessor-retired":
							reached = old == nil && s.Replacement != nil
						}
						if !reached {
							base.apply(t, New(Options{}), rc)
						}
					}
					if !reached {
						t.Fatal("actual lifecycle never reached cancellation boundary")
					}
					unprovisioned := point == "before-planning" || point == "planned-before-provisioning" || point == "provisioning-refused"
					if point == "planned-before-provisioning" || point == "provisioning-refused" {
						next := s.find(nextName)
						if next.Stage != "planned" || next.Resources == (domain.Resources{}) || next.Replicas == 0 || next.Version == "" || next.ImportBatch == nil {
							t.Fatal("test did not produce the real frozen planning shape")
						}
						if next.ServiceID != "" || next.VolumeID != "" || next.DeployOp != nil || next.ServerUUID != "" || base.services[next.Name] != nil || base.services[next.Volume] != nil {
							t.Fatal("refused/unsubmitted planning created provisioning evidence")
						}
					}
					if point == "provisioning-refused" {
						_, err := New(Options{}).Sync(t.Context(), rc)
						var wait *engine.PlatformRetryWait
						if !errors.As(err, &wait) {
							t.Fatalf("definitively refused import did not preserve its retry wait: %v", err)
						}
						outcome, err := replacementImportOutcome(t.Context(), rc, s.find(nextName))
						if err != nil || outcome != "refused" {
							t.Fatalf("non-submitting retry wait became ambiguous: outcome=%q err=%v", outcome, err)
						}
					}
					if _, err := New(Options{}).CancelOperation(t.Context(), rc, op); err != nil {
						t.Fatal(err)
					}
					b = restartReplacementFixture(t, rc, b)
					day2Ticks(t, rc, base, func(s *state) bool {
						b = restartReplacementFixture(t, rc, b)
						return s.Replacement == nil
					})
					s, _ = load(rc.Cluster)
					forward := point == "joined" || point == "retiring-predecessor" || point == "predecessor-retired"
					if forward {
						if s.find(lost.Name) != nil || s.find(nextName) == nil {
							t.Fatal("admitted replacement did not complete forward")
						}
					} else if retained := s.find(lost.Name); retained == nil || !retained.ConfirmedVolumeLost || s.find(nextName) != nil {
						t.Fatal("rollback did not preserve the loss fence")
					}
					if unprovisioned && (len(base.deleted) != 0 || len(base.stopped) != 0 || base.services[nextName] != nil || base.services[nextName+"v"] != nil) {
						t.Fatal("unprovisioned cancellation stopped, created or deleted infrastructure")
					}
					if claimable, err := p.CanClaimOperation(rc.Cluster); err != nil || !claimable {
						t.Fatalf("fresh replacement remains blocked: claimable=%v err=%v", claimable, err)
					}
				})
			}
		}
	}
}

func TestGRUnadmittedRollbackWaitsForCompleteIdentityView(t *testing.T) {
	p, rc, b, s, lost := recoveryFixture(t)
	lost.ConfirmedVolumeLost, lost.RuntimeRecovery.VolumeLost = true, true
	delete(b.deployed, lost.Name)
	adapter := &refusedReplacementBackend{physicallyLostBackend: physicallyLostBackend{fakeBackend: b, volume: lost.Volume, volumeID: lost.VolumeID}, provider: "nomad"}
	rc.Backend = adapter
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "cancel-unadmitted", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost.Name + `"}`)}
	if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	s, _ = load(rc.Cluster)
	nextName := s.Replacement.New
	day2Ticks(t, rc, b, func(s *state) bool { return s.find(nextName).Stage == "imported" && s.Pending == nil })
	if _, err := New(Options{}).CancelOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	day2Ticks(t, rc, b, func(s *state) bool { return s.find(nextName).Retirement == "stop" })
	s, _ = load(rc.Cluster)
	next := s.find(nextName)
	if next.ServerUUID != "" || next.Initialized || next.RetirementOp == nil {
		t.Fatal("test did not preserve unadmitted identity and durable Stop receipt")
	}
	backend := rc.Backend
	for _, receipt := range []infra.OpState{{}, {Done: true, Failed: true}} {
		rc.Backend = stopReceiptStateBackend{Backend: backend, status: receipt}
		if err := membershipRemoved(freshMySQLReads(t.Context()), rc, s, nextName); !errors.Is(err, engine.ErrUnavailable) {
			t.Fatalf("incomplete/failed Stop receipt released identity proof: %v", err)
		}
	}
	rc.Backend = backend
	b.services[nextName].Status = infra.StatusRunning
	if err := membershipRemoved(freshMySQLReads(t.Context()), rc, s, nextName); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatalf("running runtime released identity proof: %v", err)
	}
	b.services[nextName].Status = infra.StatusStopped
	b.services[nextName].ID = "another-runtime"
	if err := membershipRemoved(freshMySQLReads(t.Context()), rc, s, nextName); !errors.Is(err, engine.ErrUnavailable) {
		t.Fatalf("different runtime identity released native proof: %v", err)
	}
	b.services[nextName].ID = next.ServiceID
	unknownUUID := "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
	for _, state := range []string{"RECOVERING", "UNREACHABLE", "ONLINE"} {
		b.membership = make(map[string]*myspec.GroupView)
		for observer, native := range b.native {
			if native.MemberState != "ONLINE" {
				continue
			}
			view := &myspec.GroupView{}
			for name, peer := range b.native {
				if peer.MemberState == "ONLINE" {
					view.Members = append(view.Members, myspec.GroupViewMember{ID: peer.ServerUUID, Host: name, Port: 3306, State: "ONLINE", Role: peer.MemberRole})
				}
			}
			view.Members = append(view.Members, myspec.GroupViewMember{ID: unknownUUID, Host: nextName, Port: 3306, State: state, Role: "SECONDARY"})
			b.membership[observer] = view
		}
		// New Core reloads the persisted owner/receipt after Stop submission.
		if _, err := New(Options{}).Sync(t.Context(), rc); err == nil || !strings.Contains(err.Error(), "identity convergence") {
			t.Fatalf("extra %s row released deletion: %v", state, err)
		}
		current, _ := load(rc.Cluster)
		if current.find(nextName).Retirement != "stop" || len(b.deleted) != 0 {
			t.Fatal("incomplete native identity proof advanced cleanup")
		}
	}
	clear(b.membership)
	// Replay an older owner's completed exact runtime deletion before the new
	// durable fence fields existed. Its original Stop receipt has now expired.
	s, _ = load(rc.Cluster)
	next = s.find(nextName)
	if _, err := b.DeleteServiceByID(t.Context(), nextName, next.ServiceID); err != nil {
		t.Fatal(err)
	}
	next.ServiceID, next.Stage, next.Retirement = "", "planned", "delete"
	next.RetirementFenceID, next.RetirementFenceComplete = "", false
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	adapter = restartReplacementFixture(t, rc, adapter)
	if receipt, err := adapter.OpStatus(t.Context(), *next.RetirementOp); err != nil || !receipt.Expired || !receipt.Failed {
		t.Fatalf("deleted-runtime Stop receipt did not expire: %+v %v", receipt, err)
	}
	day2Ticks(t, rc, b, func(s *state) bool { return s.Replacement == nil })
	s, _ = load(rc.Cluster)
	if s.find(nextName) != nil || s.find(lost.Name) == nil || !s.find(lost.Name).ConfirmedVolumeLost {
		t.Fatal("exact unadmitted cleanup lost its predecessor fence")
	}
}

type stopReceiptStateBackend struct {
	infra.Backend
	status infra.OpState
}

func (b stopReceiptStateBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	return b.status, nil
}

func TestGRCancelAmbiguousImportRetainsLateReservations(t *testing.T) {
	for _, provider := range []string{"nomad", "dokploy"} {
		for _, arrival := range []string{"before-cancel", "after-negative-observations", "partial-then-complete", "legacy-no-journal", "accepted-no-receipt"} {
			t.Run(provider+"/"+arrival, func(t *testing.T) {
				p, rc, base, s, lost := recoveryFixture(t)
				lost.ConfirmedVolumeLost, lost.RuntimeRecovery.VolumeLost = true, true
				delete(base.deployed, lost.Name)
				pendingServer := &lateReplacementImport{}
				b := &refusedReplacementBackend{physicallyLostBackend: physicallyLostBackend{fakeBackend: base, volume: lost.Volume, volumeID: lost.VolumeID}, provider: provider, late: pendingServer}
				rc.Backend = b
				if err := save(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				op := &domain.Operation{ID: "cancel-ambiguous-import", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost.Name + `"}`)}
				if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
					t.Fatal(err)
				}
				s, _ = load(rc.Cluster)
				name := s.Replacement.New
				day2Ticks(t, rc, base, func(s *state) bool { return s.find(name).ImportBatch != nil })
				b = restartReplacementFixture(t, rc, b)
				if arrival == "legacy-no-journal" {
					current, _ := load(rc.Cluster)
					token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), name)
					if err != nil || token == nil {
						t.Fatal("missing planned import ownership token")
					}
					pendingServer.specs = current.find(name).ImportBatch.Services(rc.Backend.Layout(), rc.Cluster.Name, name, token.Token)
					if err := rc.Secrets.DeleteClusterSecret(t.Context(), rc.Cluster.Ref(), "mysql/imports/"+name); err != nil {
						t.Fatal(err)
					}
					// No old process-local record can establish whether this
					// historical frozen plan was submitted before the upgrade.
				} else if arrival == "accepted-no-receipt" {
					pendingServer.accept = true
					if _, err := New(Options{}).Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					// The accepted request survives at the server and in the
					// sealed journal, but the CP loses its in-memory Pending op.
					checkpoint, err := rc.Store.GetCluster(t.Context(), rc.Cluster.Ref())
					if err != nil {
						t.Fatal(err)
					}
					rc.Cluster = checkpoint
				} else {
					if _, err := New(Options{}).Sync(t.Context(), rc); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("lost submit reply was not exercised: %v", err)
					}
				}
				b = restartReplacementFixture(t, rc, b)
				publish := func(specs []infra.ServiceSpec) {
					if _, err := base.ImportServices(t.Context(), specs); err != nil {
						t.Fatal(err)
					}
				}
				if arrival == "before-cancel" {
					publish(pendingServer.specs)
				}
				if _, err := New(Options{}).CancelOperation(t.Context(), rc, op); err != nil {
					t.Fatal(err)
				}
				if arrival != "before-cancel" {
					for range 2 {
						b = restartReplacementFixture(t, rc, b)
						if _, err := New(Options{}).Sync(t.Context(), rc); err == nil || !strings.Contains(err.Error(), "ambiguous replacement import outcome") {
							t.Fatalf("negative inventory settled unknown submission: %v", err)
						}
						current, _ := load(rc.Cluster)
						if current.Replacement == nil || current.find(name).ImportBatch == nil || len(base.deleted) != 0 || len(base.stopped) != 0 {
							t.Fatal("negative inventory forgot import ownership")
						}
						token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), name)
						if err != nil || token == nil {
							t.Fatal("ambiguous reservation lost its owner token")
						}
						status, _ := New(Options{}).Status(t.Context(), rc)
						if !strings.Contains(status.Message, name) || !strings.Contains(status.Message, name+"v") || !strings.Contains(status.Message, "ambiguous") {
							t.Fatalf("missing actionable import uncertainty: %s", status.Message)
						}
						if claimable, err := New(Options{}).CanClaimOperation(rc.Cluster); err != nil || claimable {
							t.Fatalf("unknown import released queued work: claimable=%v err=%v", claimable, err)
						}
					}
					if arrival == "partial-then-complete" {
						for _, spec := range pendingServer.specs {
							if spec.Kind == infra.KindVolume {
								publish([]infra.ServiceSpec{spec})
							}
						}
						b = restartReplacementFixture(t, rc, b)
						if _, err := New(Options{}).Sync(t.Context(), rc); err == nil || !strings.Contains(err.Error(), "ambiguous") {
							t.Fatalf("partial batch proved terminal outcome: %v", err)
						}
						if len(base.deleted) != 0 || len(base.stopped) != 0 {
							t.Fatal("partial publication released unresolved batch")
						}
					}
					// Publish the original server-owned reservation, not another
					// import attempt by the reconstructed control plane.
					for _, spec := range pendingServer.specs {
						if base.services[spec.Hostname] == nil {
							publish([]infra.ServiceSpec{spec})
						}
					}
				}
				b = restartReplacementFixture(t, rc, b)
				day2Ticks(t, rc, base, func(s *state) bool {
					b = restartReplacementFixture(t, rc, b)
					return s.Replacement == nil
				})
				s, _ = load(rc.Cluster)
				if s.find(name) != nil || base.services[name] != nil || base.services[name+"v"] != nil || !s.find(lost.Name).ConfirmedVolumeLost {
					t.Fatal("late reservation did not settle through exact rollback")
				}
				if claimable, err := New(Options{}).CanClaimOperation(rc.Cluster); err != nil || !claimable {
					t.Fatalf("settled late reservation blocked new work: claimable=%v err=%v", claimable, err)
				}
				if outcome, err := replacementImportOutcome(t.Context(), rc, &member{Name: name}); err != nil || outcome != "" {
					t.Fatalf("retired import left a journal: outcome=%q err=%v", outcome, err)
				}
				wantCalls := 1
				if arrival == "legacy-no-journal" {
					wantCalls = 0
				}
				if pendingServer.calls != wantCalls {
					t.Fatalf("unknown import was submitted again: %d calls", pendingServer.calls)
				}
			})
		}
	}
}

type legacyReservationInventoryFailure struct {
	infra.Backend
	member, volume string
}

func (b legacyReservationInventoryFailure) GetService(ctx context.Context, name string) (*infra.Service, error) {
	if name == b.member || name == b.volume {
		return nil, infra.ErrUnavailable
	}
	return b.Backend.GetService(ctx, name)
}

func TestGRReleasedPlanlessRollbackInventoriesReservations(t *testing.T) {
	for _, provider := range []string{"nomad", "dokploy"} {
		for _, retirement := range []string{"credentials", "delete"} {
			for _, publication := range []string{"absent", "visible", "volume-only", "importing-then-removed", "importing-then-completed", "inventory-error"} {
				t.Run(provider+"/"+retirement+"/"+publication, func(t *testing.T) {
					p, rc, base, s, lost := recoveryFixture(t)
					lost.ConfirmedVolumeLost, lost.RuntimeRecovery.VolumeLost = true, true
					delete(base.deployed, lost.Name)
					base.action = legacyGRStatus(base, false)
					server := &lateReplacementImport{}
					b := &refusedReplacementBackend{physicallyLostBackend: physicallyLostBackend{fakeBackend: base, volume: lost.Volume, volumeID: lost.VolumeID}, provider: provider, late: server}
					rc.Backend = b
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					op := &domain.Operation{ID: "released-planless-cancel", Kind: domain.OperationReplaceMember, Payload: json.RawMessage(`{"member":"` + lost.Name + `"}`)}
					if _, err := p.RunOperation(t.Context(), rc, op); err != nil {
						t.Fatal(err)
					}
					s, _ = load(rc.Cluster)
					name := s.Replacement.New
					day2Ticks(t, rc, base, func(s *state) bool { return s.find(name).ImportBatch != nil })
					if _, err := p.Sync(t.Context(), rc); !errors.Is(err, context.DeadlineExceeded) {
						t.Fatalf("historical lost-create reply not exercised: %v", err)
					}
					s, _ = load(rc.Cluster)
					next := s.find(name)
					if resumed, err := resumePlannedImport(t.Context(), rc, next); err != nil || resumed != nil {
						t.Fatalf("released negative reservation observation: %v %v", resumed, err)
					}
					// Replay the released rollback transition: discard the frozen
					// plan and select credentials for planned, empty-ID members.
					next.ImportBatch = nil
					next.Removing, next.Offline, next.Retirement = true, true, retirement
					next.IdentityBarrier = false
					s.Replacement.Recovery, s.Replacement.Stage, s.Replacement.IdentityBarrier = "rollback", "retire", false
					if err := rc.Secrets.DeleteClusterSecret(t.Context(), rc.Cluster.Ref(), "mysql/imports/"+name); err != nil {
						t.Fatal(err)
					}
					if err := save(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					b = restartReplacementFixture(t, rc, b)
					if publication == "visible" || publication == "volume-only" || strings.HasPrefix(publication, "importing-then-") {
						for _, spec := range server.specs {
							if publication == "volume-only" && spec.Kind != infra.KindVolume {
								continue
							}
							if _, err := base.ImportServices(t.Context(), []infra.ServiceSpec{spec}); err != nil {
								t.Fatal(err)
							}
							if strings.HasPrefix(publication, "importing-then-") {
								base.services[spec.Hostname].Importing = true
							}
						}
					}
					if publication == "inventory-error" {
						rc.Backend = legacyReservationInventoryFailure{Backend: b, member: name, volume: name + "v"}
					}
					if publication == "inventory-error" || strings.HasPrefix(publication, "importing-then-") {
						recovery := New(Options{})
						if _, err := recovery.Sync(t.Context(), rc); !errors.Is(err, infra.ErrUnavailable) && !errors.Is(err, engine.ErrUnavailable) {
							t.Fatalf("unknown/incomplete legacy reservation advanced: %v", err)
						}
						current, _ := load(rc.Cluster)
						token, err := rc.Secrets.GetAgentToken(t.Context(), rc.Cluster.Ref(), name)
						if err != nil || token == nil || current.Replacement == nil || len(base.deleted) != 0 || len(base.stopped) != 0 {
							t.Fatal("legacy reservation wait released credentials or ownership")
						}
						if strings.HasPrefix(publication, "importing-then-") {
							status, _ := New(Options{}).Status(t.Context(), rc)
							if !strings.Contains(status.Message, name) || !strings.Contains(status.Message, "incomplete") {
								t.Fatalf("incomplete reservation lacks operator guidance: %s", status.Message)
							}
							// The operator removes or completes the exact platform
							// reservations; the same Core must retry without restart.
							for _, spec := range server.specs {
								if publication == "importing-then-removed" {
									delete(base.services, spec.Hostname)
								} else {
									base.services[spec.Hostname].Importing = false
								}
							}
							if _, err := recovery.Sync(t.Context(), rc); err != nil {
								t.Fatalf("operator resolution required a restart: %v", err)
							}
						}
						rc.Backend = b
					}
					day2Ticks(t, rc, base, func(s *state) bool {
						b = restartReplacementFixture(t, rc, b)
						return s.Replacement == nil
					})
					s, _ = load(rc.Cluster)
					if s.find(name) != nil || base.services[name] != nil || base.services[name+"v"] != nil || !s.find(lost.Name).ConfirmedVolumeLost {
						t.Fatal("released rollback left visible reservations or lost its loss fence")
					}
					if publication == "visible" && (len(base.stopped) == 0 || len(base.deleted) != 2) {
						t.Fatal("visible legacy reservations bypassed exact fence/delete")
					}
					if publication == "volume-only" && len(base.deleted) != 1 {
						t.Fatal("visible legacy volume bypassed exact deletion")
					}
					if server.calls != 1 {
						t.Fatal("released plan-less recovery reconstructed or submitted an import")
					}
					if claimable, err := p.CanClaimOperation(rc.Cluster); err != nil || !claimable {
						t.Fatalf("released owner remains stuck: claimable=%v err=%v", claimable, err)
					}
				})
			}
		}
	}
}
