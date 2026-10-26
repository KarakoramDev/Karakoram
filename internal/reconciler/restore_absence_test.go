// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/KarakoramDev/Karakoram/internal/infra/fake"
)

type absenceWindowProvider struct{ *operationProvider }

func (*absenceWindowProvider) InvalidateOperationObservation(op *domain.Operation) error {
	raw, err := pbm.ResetRestoreAbsence(op.Checkpoint)
	if err == nil {
		op.Checkpoint = raw
	}
	return err
}

func TestRestoreAbsenceWindowResetsBeforeProviderEntry(t *testing.T) {
	for _, failure := range []string{"namespace-resolution", "restore-validation"} {
		t.Run(failure, func(t *testing.T) {
			base := &operationProvider{fakeProvider: lifecycleProvider().fakeProvider}
			provider := &absenceWindowProvider{operationProvider: base}
			m, st := newTestManager(t, provider)
			backend := fake.New(fake.Options{})
			good := recoveredBackoffNamespace{engine.StaticNamespaces{DefaultBackend: backend}}
			m.opts.Namespaces = good
			now := time.Date(2026, 10, 21, 8, 0, 0, 0, time.UTC)
			m.opts.Context.Clock = fixedClock{now: now}
			if err := st.CreateNamespace(t.Context(), &domain.Namespace{Name: "broken", Phase: "Ready"}); err != nil {
				t.Fatal(err)
			}
			c := &domain.Cluster{Namespace: "broken", Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			if err := st.CreateRestore(t.Context(), &domain.Restore{Namespace: c.Namespace, Cluster: c.Name, Name: "recover"}); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Namespace: c.Namespace, Cluster: c.Name, Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"recover"}`), Checkpoint: json.RawMessage(`{"step":"poll"}`)}
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			calls := 0
			base.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
				return domain.ClusterStatus{Phase: domain.PhaseReady}, nil
			}
			base.runFn = func(rc *engine.Context, work *domain.Operation) (engine.OpResult, error) {
				calls++
				stored, err := st.GetOperation(t.Context(), work.ID)
				if err != nil {
					t.Fatal(err)
				}
				var durable struct {
					Since *time.Time `json:"restoreAbsentSince"`
				}
				_ = json.Unmarshal(stored.Checkpoint, &durable)
				if durable.Since != nil {
					t.Error("old proof was not durably invalidated before provider call")
				}
				var cp struct {
					Step  string     `json:"step"`
					Since *time.Time `json:"restoreAbsentSince,omitempty"`
				}
				_ = json.Unmarshal(work.Checkpoint, &cp)
				if cp.Since == nil {
					stamp := rc.Clock.Now()
					cp.Since = &stamp
					cp.Step = "poll"
				} else if rc.Clock.Now().Sub(*cp.Since) >= time.Minute {
					cp.Step = "fence-pbm"
				}
				raw, _ := json.Marshal(cp)
				return engine.OpResult{Step: cp.Step, Checkpoint: raw, RequeueAfter: time.Second}, nil
			}
			if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			if calls != 1 {
				t.Fatal("first complete proof did not execute")
			}
			now = now.Add(2 * time.Minute)
			m.opts.Context.Clock = fixedClock{now: now}
			if failure == "namespace-resolution" {
				m.opts.Namespaces = &testNamespaces{calls: map[string]int{}}
			} else {
				current, _ := st.GetOperation(t.Context(), op.ID)
				current.Payload = json.RawMessage(`{"restore":"missing"}`)
				if err := st.UpdateOperation(t.Context(), current); err != nil {
					t.Fatal(err)
				}
			}
			if _, err := m.reconcile(t.Context(), c.Ref()); err == nil {
				t.Fatal("fixture failed to interrupt before provider entry")
			}
			if calls != 1 {
				t.Fatal("failed dependency/validation entered provider")
			}
			held, err := st.GetOperation(t.Context(), op.ID)
			if err != nil {
				t.Fatal(err)
			}
			var cp struct {
				Since *time.Time `json:"restoreAbsentSince"`
			}
			_ = json.Unmarshal(held.Checkpoint, &cp)
			if cp.Since != nil {
				t.Fatal("pre-provider failure retained stale proof", cp.Since)
			}
			m.opts.Namespaces = good
			held.Payload = json.RawMessage(`{"restore":"recover"}`)
			if err := st.UpdateOperation(t.Context(), held); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			fresh, _ := st.GetOperation(t.Context(), op.ID)
			_ = json.Unmarshal(fresh.Checkpoint, &cp)
			if fresh.Step != "poll" || cp.Since == nil || !cp.Since.Equal(now) {
				t.Fatal("recovered attempt reused stale window", fresh.Step, cp.Since)
			}
		})
	}
}
