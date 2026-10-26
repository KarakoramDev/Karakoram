// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package reconciler

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
)

type recoveredCleanupProvider struct {
	*clockCleanupProvider
	recovered bool
}

func (p *recoveredCleanupProvider) CancelOperation(context.Context, *engine.Context, *domain.Operation) (engine.OpResult, error) {
	return engine.OpResult{Done: true, Recovered: p.recovered, Message: "native outcome unconfirmed; candidate unmanaged artifacts [native-1]; Operator action required"}, nil
}
func TestFailureCleanupRecoveredOutcome(t *testing.T) {
	for _, kind := range []string{domain.OperationBackup, domain.OperationRestore} {
		for _, recovered := range []bool{true, false} {
			p := &recoveredCleanupProvider{clockCleanupProvider: &clockCleanupProvider{fakeProvider: lifecycleProvider().fakeProvider}, recovered: recovered}
			m, st := newTestManager(t, p)
			c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "fake"}}
			if err := st.CreateCluster(t.Context(), c); err != nil {
				t.Fatal(err)
			}
			op := &domain.Operation{Cluster: c.Name, Kind: kind, Phase: domain.PhaseRunning, Step: failureCleanupStep, LastError: "exhausted retry budget"}
			if err := st.EnqueueOperation(t.Context(), op); err != nil {
				t.Fatal(err)
			}
			if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
				t.Fatal(err)
			}
			got, err := st.GetOperation(t.Context(), op.ID)
			if err != nil {
				t.Fatal(err)
			}
			expected := domain.PhaseFailed
			if recovered && kind == domain.OperationBackup {
				expected = domain.PhaseSucceeded
			}
			if got.Phase != expected || got.CompletedAt == nil {
				t.Fatalf("kind=%s recovered=%v: %+v", kind, recovered, got)
			}
			if !recovered && kind == domain.OperationBackup && got.Message != "native outcome unconfirmed; candidate unmanaged artifacts [native-1]; Operator action required" {
				t.Fatal("operator diagnostic lost", got)
			}
		}
	}
}

func TestBackupFailureCleanupExpiresBeforeNamespaceResolution(t *testing.T) {
	m, st := newTestManager(t, &psmdb.Provider{})
	m.opts.Namespaces = unavailableCleanupNamespaces{}
	c := &domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}}
	if err := st.CreateCluster(t.Context(), c); err != nil {
		t.Fatal(err)
	}
	b := &domain.Backup{Name: "manual", Cluster: c.Name, Phase: domain.PhasePending, Type: "logical"}
	if err := st.CreateBackup(t.Context(), b); err != nil {
		t.Fatal(err)
	}
	op := &domain.Operation{Cluster: c.Name, Kind: domain.OperationBackup, Phase: domain.PhaseRunning, Step: failureCleanupStep, LastError: "original initiation failure", FailureCleanupElapsed: 2 * time.Minute, Payload: json.RawMessage(`{"backup":"manual"}`), Checkpoint: json.RawMessage(`{"step":"start","backupCleanupStartedElapsed":0,"backupCleanupCandidates":["native-1"]}`)}
	if err := st.EnqueueOperation(t.Context(), op); err != nil {
		t.Fatal(err)
	}
	if _, err := m.reconcile(t.Context(), c.Ref()); err != nil {
		t.Fatal(err)
	}
	got, err := st.GetOperation(t.Context(), op.ID)
	if err != nil || got.Phase != domain.PhaseFailed || got.CompletedAt == nil || !strings.Contains(got.Message, "native-1") || !strings.Contains(got.Message, "Operator action required") {
		t.Fatal(got, err)
	}
	stored, err := st.GetBackup(t.Context(), c.Ref(), b.Name)
	if err != nil || stored.Phase != domain.PhaseFailed {
		t.Fatal(stored, err)
	}
}
