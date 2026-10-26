// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package supabase

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/reconciler"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type terminalResizeBackend struct {
	*testBackend
	target  string
	resizes int
}

func (b *terminalResizeBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if op.ID == "terminal-resize" {
		return infra.OpState{Failed: true, Status: "failed", Message: "stateless job was superseded or automatically reverted"}, nil
	}
	return b.testBackend.OpStatus(ctx, op)
}

func (b *terminalResizeBackend) SetResources(ctx context.Context, name string, desired domain.Resources) (infra.Op, error) {
	if name == b.target {
		b.resizes++
	}
	return b.testBackend.SetResources(ctx, name, desired)
}

func TestStorageTerminalResizeHandledWhileCoreNotReady(t *testing.T) {
	for _, corrected := range []bool{false, true} {
		t.Run(map[bool]string{false: "unchanged_failure", true: "corrected_target"}[corrected], func(t *testing.T) {
			p, rc, deployed := storageDeployFailureFixture(t, infra.OpState{Done: true})
			convergeProvider(t, p, rc)
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members["storage"]
			failed := domain.ComponentSpec{Replicas: m.Replicas, Resources: m.Resources}
			failed.Resources.MinRAM, failed.Resources.MaxRAM = 2, 2
			s.Pending = &componentPending{Op: infra.Op{ID: "terminal-resize"}, Component: "storage", Action: "resize", Resources: failed.Resources, Target: failed}
			if err := saveState(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			if !corrected {
				rc.Cluster.Spec.Components.Extra["storage"] = failed
			}
			rc.Cluster.Generation++
			deployed.services[m.Name].Resources = failed.Resources
			deployed.services[m.Name].Status = infra.StatusFailed
			deployed.stopped = map[string]bool{"demop0": true}
			backend := &terminalResizeBackend{testBackend: deployed.testBackend, target: m.Name}
			rc.Backend = backend
			if _, err := p.Sync(t.Context(), rc); err == nil {
				t.Fatal("terminal operation was not observed")
			}
			p = New(p.options)
			for range 3 {
				_, err := p.Sync(t.Context(), rc)
				if !corrected && err == nil {
					t.Fatal("core readiness hid the terminal error from the tracking failure budget")
				}
				if corrected && err != nil {
					t.Fatal(err)
				}
			}
			if !corrected {
				if backend.resizes != 0 {
					t.Fatal("unchanged failed target was automatically resubmitted")
				}
				return
			}
			s, err = loadState(rc.Cluster)
			if err != nil || backend.resizes != 1 || s.Pending != nil || s.Members["storage"].Resources.MaxRAM != 1 || s.Members["storage"].RuntimeFailure != nil {
				t.Fatalf("correction trapped behind core readiness: resizes=%d state=%+v err=%v", backend.resizes, s, err)
			}
			deployed.stopped["demop0"] = false
			convergeProvider(t, p, rc)
			if backend.resizes != 1 {
				t.Fatal("completed correction was submitted twice")
			}
		})
	}
}

func TestTerminalComponentFailureExhaustsTrackingBudgetWhileCoreNotReady(t *testing.T) {
	p, rc, deployed := storageDeployFailureFixture(t, infra.OpState{Done: true})
	convergeProvider(t, p, rc)
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	m := s.Members["storage"]
	target := domain.ComponentSpec{Replicas: m.Replicas, Resources: m.Resources}
	target.Resources.MinRAM, target.Resources.MaxRAM = 2, 2
	m.RuntimeFailure = &componentRuntimeFailure{Op: infra.Op{ID: "terminal-resize"}, Action: "resize", Target: target, Status: "failed", Detail: "stateless job was superseded or automatically reverted"}
	if err := saveState(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	deployed.services[m.Name].Resources = target.Resources
	deployed.services[m.Name].Status = infra.StatusFailed
	deployed.stopped = map[string]bool{"demop0": true}
	backend := &terminalResizeBackend{testBackend: deployed.testBackend, target: m.Name}
	rc.Backend = backend
	st := rc.Store.(*store.Store)
	if err := st.CreateCluster(t.Context(), rc.Cluster); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Components.Extra["storage"] = target
	op := &domain.Operation{Kind: domain.OperationUpdate}
	if err := st.UpdateClusterSpec(store.WithOperation(t.Context(), op), rc.Cluster.Ref(), rc.Cluster.Spec); err != nil {
		t.Fatal(err)
	}
	registry, err := engine.NewRegistry(p)
	if err != nil {
		t.Fatal(err)
	}
	manager, err := reconciler.New(reconciler.Options{
		Store: st, Registry: registry, Context: *rc, ResyncInterval: time.Hour,
		OperationRetryLimit: 2, BackoffMin: time.Millisecond, BackoffMax: time.Millisecond,
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
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		published, err := st.GetOperation(t.Context(), op.ID)
		if err != nil {
			t.Fatal(err)
		}
		if published.Phase == domain.PhaseFailed {
			if published.Failures != 2 || published.CompletedAt == nil || backend.resizes != 0 {
				t.Fatalf("terminal failure was not bounded without replay: op=%+v resizes=%d", published, backend.resizes)
			}
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("terminal component failure left tracking operation running: %+v", published)
		case <-ticker.C:
		}
	}
}

func TestGatewayRevocationPrecedesTerminalComponentFailure(t *testing.T) {
	for _, disable := range []string{"public-https", "component"} {
		t.Run(disable, func(t *testing.T) {
			p, rc, deployed := storageDeployFailureFixture(t, infra.OpState{Done: true})
			convergeProvider(t, p, rc)
			public := &publicBackend{testBackend: deployed.testBackend}
			rc.Backend = public
			rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"backendStorage":"objects"},"gateway":{"publicHttps":true}}`)
			convergeProvider(t, p, rc)
			if !public.enabled {
				t.Fatal("fixture requires a published gateway")
			}
			s, err := loadState(rc.Cluster)
			if err != nil {
				t.Fatal(err)
			}
			m := s.Members["storage"]
			m.RuntimeFailure = &componentRuntimeFailure{
				Op: infra.Op{ID: "terminal-resize"}, Action: "resize",
				Target: rc.Cluster.Spec.Components.Extra["storage"], Status: "failed", Detail: "native rollout failed",
			}
			if err := saveState(rc.Cluster, s); err != nil {
				t.Fatal(err)
			}
			deployed.stopped = map[string]bool{"demop0": true}
			if disable == "public-https" {
				rc.Cluster.Spec.EngineOptions = json.RawMessage(`{"storage":{"backendStorage":"objects"}}`)
			} else {
				gateway := rc.Cluster.Spec.Components.Extra["gateway"]
				gateway.Replicas = 0
				rc.Cluster.Spec.Components.Extra["gateway"] = gateway
			}
			rc.Cluster.Generation++
			_, err = p.Sync(t.Context(), rc)
			if public.enabled {
				t.Error("terminal component failure prevented gateway revocation")
			}
			if err == nil || !strings.Contains(err.Error(), "terminal-resize") {
				t.Fatalf("revocation hid the terminal component error: %v", err)
			}
			// Accepted revocation must still be observed and committed while
			// the unrelated terminal error continues charging its budget.
			_, err = p.Sync(t.Context(), rc)
			if err == nil || !strings.Contains(err.Error(), "terminal-resize") {
				t.Fatalf("terminal failure disappeared during revocation polling: %v", err)
			}
			s, err = loadState(rc.Cluster)
			if err != nil || s.Pending != nil || s.Members["gateway"].Public || s.Members["storage"].RuntimeFailure == nil {
				t.Fatalf("revocation receipt or terminal evidence lost: %+v %v", s, err)
			}
		})
	}
}

type correctiveComponentBackend struct {
	*testBackend
	target        string
	allowRetained bool
	fail          bool
	submissions   map[string]int
	polls         map[string]int
}

func (b *correctiveComponentBackend) receipt(action string) infra.Op {
	b.submissions[action]++
	b.services[b.target].Status = infra.StatusCreating
	return infra.Op{ID: action + "-correction", Kind: action}
}

func (b *correctiveComponentBackend) SetResources(ctx context.Context, name string, desired domain.Resources) (infra.Op, error) {
	op, err := b.testBackend.SetResources(ctx, name, desired)
	if err != nil || name != b.target {
		return op, err
	}
	return b.receipt("resources"), nil
}

func (b *correctiveComponentBackend) SetContainerCount(ctx context.Context, name string, min, max int) (infra.Op, error) {
	op, err := b.testBackend.SetContainerCount(ctx, name, min, max)
	if err != nil || name != b.target {
		return op, err
	}
	return b.receipt("scale"), nil
}

func (b *correctiveComponentBackend) RecoverFailedScale(_ context.Context, name string, min, max int) (infra.Op, bool, error) {
	if name != b.target || !b.allowRetained || b.submissions["retained"] != 0 {
		return infra.Op{}, false, nil
	}
	b.services[name].MinContainers, b.services[name].MaxContainers = min, max
	return b.receipt("retained"), true, nil
}

func (*correctiveComponentBackend) ResourceUpdatesDeployRuntime() bool { return true }

func (b *correctiveComponentBackend) ResizeUnplacedRuntime(ctx context.Context, name string, expected infra.Op, desired infra.Resources) (infra.Op, bool, error) {
	if name != b.target || expected.ID != "resources-correction" || b.submissions["resources-replacement"] != 0 {
		return infra.Op{}, false, nil
	}
	if _, err := b.testBackend.SetResources(ctx, name, desired); err != nil {
		return infra.Op{}, false, err
	}
	return b.receipt("resources-replacement"), true, nil
}

func (b *correctiveComponentBackend) OpStatus(ctx context.Context, op infra.Op) (infra.OpState, error) {
	if b.submissions[op.Kind] == 0 || op.ID != op.Kind+"-correction" {
		return b.testBackend.OpStatus(ctx, op)
	}
	b.polls[op.ID]++
	if b.polls[op.ID] <= 3 {
		return infra.OpState{Status: "starting"}, nil
	}
	if b.fail {
		return infra.OpState{Failed: true, Status: "failed", Message: "corrective rollout failed"}, nil
	}
	b.services[b.target].Status = infra.StatusRunning
	return infra.OpState{Done: true, Status: "done"}, nil
}

func TestAcceptedComponentCorrectionsOwnTheirFailureUntilTerminal(t *testing.T) {
	for _, scenario := range []struct {
		branch string
		legacy bool
	}{
		{branch: "resources-after-scale"},
		{branch: "resources-after-resize"},
		{branch: "retained-after-scale"},
		{branch: "retained-after-resize"},
		{branch: "scale-reduction"},
		{branch: "resize-supersession"},
		{branch: "resources-after-scale", legacy: true},
		{branch: "retained-after-scale", legacy: true},
		{branch: "retained-after-resize", legacy: true},
	} {
		branch := scenario.branch
		label := branch
		if scenario.legacy {
			label = "legacy/" + branch
		}
		for _, failed := range []bool{false, true} {
			outcome := map[bool]string{false: "success", true: "failure"}[failed]
			t.Run(label+"/"+outcome, func(t *testing.T) {
				p, rc, base := setupProvider(t)
				initial := rc.Cluster.Spec.Components.Extra["rest"]
				initial.Replicas = 2
				rc.Cluster.Spec.Components.Extra["rest"] = initial
				convergeProvider(t, p, rc)
				s, err := loadState(rc.Cluster)
				if err != nil {
					t.Fatal(err)
				}
				m := s.Members["rest"]
				target := initial
				target.Resources.MinRAM, target.Resources.MaxRAM = 2, 2
				action := "resize"
				if strings.HasSuffix(branch, "-scale") || branch == "scale-reduction" || branch == "resize-supersession" {
					target.Replicas = 3
					action = "scale"
				}
				if branch == "scale-reduction" {
					target.Resources = initial.Resources
				}
				failureTarget := target
				actualResources := target.Resources
				wantResources, wantRetained, wantScales := 0, 1, 0
				wantReplacements := 0
				activeAction := "retained"
				if branch == "resources-after-scale" || branch == "resize-supersession" {
					actualResources = initial.Resources
					wantResources, wantRetained = 1, 0
					activeAction = "resources"
				} else if branch == "resources-after-resize" {
					failureTarget.Resources.MinRAM, failureTarget.Resources.MaxRAM = 4, 4
					actualResources = failureTarget.Resources
					wantResources, wantRetained = 1, 0
					activeAction = "resources"
				}
				rc.Cluster.Spec.Components.Extra["rest"] = target
				rc.Cluster.Generation++
				m.RuntimeFailure = &componentRuntimeFailure{Op: infra.Op{ID: "failed-runtime"}, Action: action, Target: failureTarget, Status: "failed"}
				if err := saveState(rc.Cluster, s); err != nil {
					t.Fatal(err)
				}
				base.services[m.Name].Resources = actualResources
				base.services[m.Name].MinContainers, base.services[m.Name].MaxContainers = target.Replicas, target.Replicas
				base.services[m.Name].Status = infra.StatusFailed
				base.stopped = map[string]bool{"demop0": true}
				backend := &correctiveComponentBackend{
					testBackend: base, target: m.Name, allowRetained: wantRetained != 0, fail: failed,
					submissions: map[string]int{}, polls: map[string]int{},
				}
				rc.Backend = backend
				if _, err := p.Sync(t.Context(), rc); err != nil {
					t.Fatal(err)
				}
				if branch == "scale-reduction" {
					// A smaller count replaces an accepted corrective receipt;
					// it must inherit that receipt's failure ownership.
					target.Replicas = 2
					rc.Cluster.Spec.Components.Extra["rest"] = target
					rc.Cluster.Generation++
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					wantScales, activeAction = 1, "scale"
				}
				if branch == "resize-supersession" {
					// Supersession changes the accepted resize receipt in place.
					target.Resources.MinRAM, target.Resources.MaxRAM = 1.5, 1.5
					rc.Cluster.Spec.Components.Extra["rest"] = target
					rc.Cluster.Generation++
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatal(err)
					}
					wantReplacements, activeAction = 1, "resources-replacement"
				}
				receiptID := activeAction + "-correction"
				if scenario.legacy {
					// Persist the pre-upgrade JSON shape, not a linked receipt
					// merely carried by an in-memory older provider.
					s, err = loadState(rc.Cluster)
					if err != nil || s.Pending == nil {
						t.Fatalf("missing legacy correction checkpoint: %+v %v", s, err)
					}
					s.Pending.CorrectsFailure = nil
					if err := saveState(rc.Cluster, s); err != nil {
						t.Fatal(err)
					}
					if strings.Contains(string(rc.Cluster.Internal), `"correctsFailure"`) {
						t.Fatal("legacy JSON still contains the new linkage field")
					}
					p = New(p.options)
				}
				for i := range 3 {
					if _, err := p.Sync(t.Context(), rc); err != nil {
						t.Fatalf("accepted correction poll %d charged an obsolete failure: %v", i+1, err)
					}
					s, err = loadState(rc.Cluster)
					if err != nil || s.Pending == nil || s.Pending.Op.ID != receiptID || s.Members["rest"].RuntimeFailure == nil {
						t.Fatalf("nonterminal correction lost its receipt/evidence: %+v %v", s, err)
					}
					if scenario.legacy {
						if s.Pending.CorrectsFailure != nil {
							t.Fatal("legacy observation fabricated explicit provenance")
						}
					} else if s.Pending.CorrectsFailure == nil || s.Pending.CorrectsFailure.ID != "failed-runtime" {
						t.Fatal("accepted correction lost explicit failure linkage")
					}
					p = New(p.options)
				}
				_, err = p.Sync(t.Context(), rc)
				if failed {
					if err == nil || !strings.Contains(err.Error(), receiptID) {
						t.Fatalf("new terminal correction failure was hidden: %v", err)
					}
					for range 3 {
						if _, err := p.Sync(t.Context(), rc); err == nil || !strings.Contains(err.Error(), receiptID) {
							t.Fatalf("new failure lost its bounded error: %v", err)
						}
					}
				} else {
					if err != nil {
						t.Fatal(err)
					}
					base.stopped["demop0"] = false
					convergeProvider(t, p, rc)
					if branch == "resources-after-scale" || branch == "resize-supersession" {
						// Resource-first recovery is followed by the existing
						// ordinary count reconciliation, not a second resize.
						wantScales = 1
					}
				}
				s, err = loadState(rc.Cluster)
				if err != nil || s.Pending != nil || (s.Members["rest"].RuntimeFailure != nil) != failed {
					t.Fatalf("terminal correction evidence is wrong: %+v %v", s, err)
				}
				if backend.submissions["resources"] != wantResources || backend.submissions["retained"] != wantRetained || backend.submissions["scale"] != wantScales || backend.submissions["resources-replacement"] != wantReplacements || backend.polls[receiptID] != 4 {
					t.Fatalf("correction replayed: submissions=%v polls=%v", backend.submissions, backend.polls)
				}
			})
		}
	}
}

func TestCorrectiveReceiptSuppressesOnlyItsExactFailureTarget(t *testing.T) {
	for _, mismatch := range []string{"none", "component", "target", "failure", "same-id-different-receipt", "unlinked-public", "unlinked-ordinary-resize", "unlinked-ordinary-scale", "legacy-old-receipt", "legacy-wrong-resources", "legacy-wrong-count"} {
		t.Run(mismatch, func(t *testing.T) {
			p, rc, _ := setupProvider(t)
			target := domain.ComponentSpec{Replicas: 1, Resources: domain.Resources{MinRAM: 1, MaxRAM: 1}}
			rc.Cluster.Spec.Components.Extra["storage"] = target
			s := &componentState{
				Members: map[string]*componentMember{"storage": {
					Name: "storage", Component: "storage",
					RuntimeFailure: &componentRuntimeFailure{Op: infra.Op{ID: "failed-runtime"}, Action: "resize", Target: target, Status: "failed"},
				}},
				Pending: &componentPending{Op: infra.Op{ID: "pending-operation"}, Component: "storage", Action: "resize", Replicas: target.Replicas, Resources: target.Resources, Target: target, CorrectsFailure: &infra.Op{ID: "failed-runtime"}},
			}
			switch mismatch {
			case "component":
				s.Pending.Component = "gateway"
			case "target":
				s.Pending.Target.Resources.MaxRAM = 2
			case "failure":
				s.Pending.CorrectsFailure.ID = "unrelated-failure"
				s.Pending.Action, s.Pending.RetainedRecovery = "scale", true
			case "same-id-different-receipt":
				s.Pending.CorrectsFailure.ServiceID = "unrelated-service"
				s.Pending.Action, s.Pending.RetainedRecovery = "scale", true
			case "unlinked-public":
				s.Pending.CorrectsFailure, s.Pending.Action = nil, "public"
			case "unlinked-ordinary-resize":
				s.Pending.CorrectsFailure = nil
			case "unlinked-ordinary-scale":
				s.Pending.CorrectsFailure, s.Pending.Action = nil, "scale"
			case "legacy-old-receipt":
				s.Pending.CorrectsFailure, s.Pending.Action, s.Pending.RetainedRecovery = nil, "scale", true
				s.Pending.Op = s.Members["storage"].RuntimeFailure.Op
			case "legacy-wrong-resources":
				s.Pending.CorrectsFailure = nil
				s.Members["storage"].RuntimeFailure.Action = "scale"
				s.Pending.Resources.MaxRAM = 2
			case "legacy-wrong-count":
				s.Pending.CorrectsFailure, s.Pending.Action, s.Pending.RetainedRecovery = nil, "scale", true
				s.Pending.Replicas = 2
			}
			busy, err := p.reconcileComponentRuntimeFailures(t.Context(), rc, s, false)
			if mismatch == "none" {
				if busy || err != nil {
					t.Fatalf("matching accepted recovery charged its old failure: busy=%v err=%v", busy, err)
				}
			} else if !busy || err == nil || !strings.Contains(err.Error(), "failed-runtime") {
				t.Fatalf("unrelated receipt hid a terminal failure: busy=%v err=%v", busy, err)
			}
		})
	}
}
