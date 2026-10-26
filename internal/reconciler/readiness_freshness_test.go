// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestReadinessWithdrawalPersistsBeforeRecoverySync(t *testing.T) {
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	m, st := newTestManager(t, p)
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 1, ObservedAt: time.Now(), Conditions: []domain.Condition{{Type: "Replication/rs0", Status: "True", Reason: "WritablePrimary"}}}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseDegraded, Conditions: []domain.Condition{{Type: "Replication/rs0", Status: "False", Reason: "NoWritablePrimary"}}}, nil
	}
	syncs := 0
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		syncs++
		stored, err := st.GetCluster(t.Context(), c.Ref())
		if err != nil || stored.Status.Phase == domain.PhaseReady {
			t.Fatal("recovery began before withdrawing persisted Ready", stored, err)
		}
		for _, condition := range stored.Status.Conditions {
			if condition.Reason == "WritablePrimary" && condition.Status == "True" {
				t.Fatal("recovery began before withdrawing WritablePrimary")
			}
		}
		return engine.Result{RequeueAfter: time.Minute}, nil
	}
	rc := m.opts.Context
	rc.Cluster = c
	result, err := m.synchronize(t.Context(), p, &rc, nil)
	if err != nil || syncs != 1 || result.RequeueAfter != engine.ReadinessInterval || c.Status.ObservedGeneration != 1 {
		t.Fatal("freshness changed generation or convergence ownership", result, c.Status, err, syncs)
	}
}

func TestReadinessPrepublicationKeepsImperativeCheckpointAtomic(t *testing.T) {
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	m, st := newTestManager(t, p)
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{ID: "replace", Cluster: c.Name, Kind: domain.OperationReplaceMember, Phase: domain.PhaseRunning}
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = json.RawMessage(`{"stage":"join"}`)
	c.Internal = json.RawMessage(`{"newMember":"demom3"}`)
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{Phase: domain.PhaseDegraded}, nil
	}
	p.syncFn = func(*engine.Context) (engine.Result, error) {
		storedOp, err := st.GetOperation(t.Context(), op.ID)
		if err != nil || string(storedOp.Checkpoint) != string(op.Checkpoint) {
			t.Fatal("health publication lost the native operation receipt", storedOp, err)
		}
		stored, err := st.GetCluster(t.Context(), c.Ref())
		if err != nil || string(stored.Internal) != string(c.Internal) || stored.Status.Phase == domain.PhaseReady {
			t.Fatal("operation receipt and withdrawn health were not published together", stored, err)
		}
		return engine.Result{RequeueAfter: time.Second}, nil
	}
	rc := m.opts.Context
	rc.Cluster = c
	if _, err := m.synchronize(t.Context(), p, &rc, op); err != nil {
		t.Fatal(err)
	}
}

func TestCreatingWithoutAgentsContinuesSyncThroughDeployment(t *testing.T) {
	p := &operationProvider{fakeProvider: &fakeProvider{}}
	m, st := newTestManager(t, p)
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}, Status: domain.ClusterStatus{Phase: domain.PhaseCreating}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	syncs := 0
	p.syncFn = func(rc *engine.Context) (engine.Result, error) {
		syncs++
		// Model separate durable import and deployment passes. Neither can
		// require a successful agent observation before a runtime exists.
		rc.Cluster.Internal, _ = json.Marshal(struct {
			Imported bool `json:"imported"`
			Deployed bool `json:"deployed"`
		}{Imported: true, Deployed: syncs >= 2})
		return engine.Result{RequeueAfter: time.Second}, nil
	}
	p.statusFn = func(*engine.Context) (domain.ClusterStatus, error) {
		return domain.ClusterStatus{
			Phase: domain.PhaseCreating, Message: "Deploying member runtimes",
			Conditions: []domain.Condition{{Type: "Ready", Status: "False", Reason: "MembersStarting"}},
			Members: []domain.MemberStatus{
				{Name: "m0", State: "starting"},
				{Name: "m1", State: "starting"},
				{Name: "m2", State: "starting"},
			},
		}, nil
	}
	var firstStatus domain.ClusterStatus
	for range 2 {
		if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
			t.Fatal("unavailable bootstrap agents blocked provisioning", err)
		}
		stored, err := st.GetCluster(t.Context(), c.Ref())
		if err != nil {
			t.Fatal(err)
		}
		if syncs == 1 {
			firstStatus = stored.Status
		}
		for _, member := range stored.Status.Members {
			if member.Health || member.Agent.Healthy {
				t.Fatal("creation asserted member availability before agent deployment", member)
			}
		}
		c = stored
	}
	var provisioned struct {
		Imported bool `json:"imported"`
		Deployed bool `json:"deployed"`
	}
	if err := json.Unmarshal(c.Internal, &provisioned); err != nil || !provisioned.Imported || !provisioned.Deployed || syncs != 2 {
		t.Fatal("creation did not progress through import and deployment", provisioned, syncs, err)
	}
	for _, status := range []domain.ClusterStatus{firstStatus, c.Status} {
		if status.Phase != domain.PhaseCreating || status.Message != "Deploying member runtimes" || status.ObservedAt.IsZero() {
			t.Fatal("negative bootstrap observation replaced provisioning progress", status)
		}
		for _, condition := range status.Conditions {
			if condition.Type == "Ready" && condition.Status != "False" {
				t.Fatal("creation retained positive readiness", condition)
			}
		}
	}
}
