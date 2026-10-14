// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestStatusObservedVersionAndLeavingMembers(t *testing.T) {
	p, rc, _, _ := setup(t)
	ctx := context.Background()
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.UID = "observed-runtime"
	plan(rc.Cluster, s)
	s.Phase = "upgrade"
	s.Members[0].Removing = true
	if err = p.ensureSecrets(ctx, rc, s); err != nil {
		t.Fatal(err)
	}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Version = "9.0.1"
	got, err := p.Status(ctx, rc)
	if err != nil || got.Version != "8.0.32-14" || len(got.Members) != 1 || got.Members[0].State != "LEAVING" || got.Members[0].Health {
		t.Fatal(got, err)
	}
}

func TestStatusSuspendedAndInitialCreationPhase(t *testing.T) {
	p, rc, _, _ := setup(t)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.UID = "suspended-runtime"
	plan(rc.Cluster, s)
	s.Phase = "join"
	if err = p.ensureSecrets(context.Background(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	got, err := p.Status(context.Background(), rc)
	if err != nil || got.Phase != domain.PhaseCreating || got.Message == "" {
		t.Fatal(got, err)
	}
	rc.Cluster.Status = got
	s.Suspended = true
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	got, err = p.Status(context.Background(), rc)
	if err != nil || got.Phase != domain.PhaseSuspended || got.ObservedGeneration != rc.Cluster.Generation || len(got.Members) != 1 {
		t.Fatal(got, err)
	}
	m := got.Members[0]
	if m.State != "SUSPENDED" || m.Health || m.Agent.Healthy {
		t.Fatal(m)
	}
}

func TestProviderConditionTransitionsRemainStable(t *testing.T) {
	p, rc, _, _ := setup(t)
	s, _ := load(rc.Cluster)
	s.UID = "conditions"
	plan(rc.Cluster, s)
	if err := p.ensureSecrets(context.Background(), rc, s); err != nil {
		t.Fatal(err)
	}
	if err := save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	first, err := p.Status(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	old := time.Now().Add(-time.Hour)
	for i := range first.Conditions {
		first.Conditions[i].LastTransitionTime = old
	}
	rc.Cluster.Status = first
	second, err := p.Status(context.Background(), rc)
	if err != nil {
		t.Fatal(err)
	}
	for _, condition := range second.Conditions {
		if !condition.LastTransitionTime.Equal(old) {
			t.Fatal(condition)
		}
	}
}

func TestNeverStartedBackupsDoNotBlockUnformedClusterCleanup(t *testing.T) {
	p, rc, _, _ := setup(t)
	b := &domain.Backup{Cluster: rc.Cluster.Name, Name: "never", Storage: "archive", Type: "logical", Phase: domain.PhaseFailed, Error: "Cluster deletion requested"}
	if err := rc.Store.CreateBackup(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	done := false
	for range 4 {
		var err error
		done, err = p.Cleanup(context.Background(), rc, domain.DeleteOptions{})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("never-ready cluster cleanup did not finish")
	}
	items, err := rc.Store.ListBackups(context.Background(), rc.Cluster.Ref())
	if err != nil || len(items) != 0 {
		t.Fatal(items, err)
	}
}

func TestCancellationRejectsDataMutationAndSupportsWaitingCheckpoints(t *testing.T) {
	p, _, _, _ := setup(t)
	for _, tc := range []struct {
		kind, checkpoint string
		allowed          bool
	}{
		{domain.OperationRestore, `{"step":"source-inventory"}`, true},
		{domain.OperationRestore, `{"step":"poll"}`, false},
		{domain.OperationRestore, `{"step":"start"}`, false},
		{domain.OperationBackup, `{"step":"poll"}`, true},
		{domain.OperationRaiseFCV, `{}`, true},
		{domain.OperationRaiseFCV, `{"stage":"apply-fcv"}`, false},
		{domain.OperationDelete, `{}`, false},
	} {
		err := p.CanCancelOperation(&domain.Operation{ID: "task", Kind: tc.kind, Checkpoint: json.RawMessage(tc.checkpoint)})
		if (err == nil) != tc.allowed {
			t.Fatalf("%s %s: %v", tc.kind, tc.checkpoint, err)
		}
	}
}
