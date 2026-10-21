// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"encoding/json"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestSyncRecoversWhenReplayedRestoreIsRefusedAsShared(t *testing.T) {
	p, rc, binder, op := restoreFixture(t)
	// Preflight sees an exclusive source. The durable request intent survives
	// interruption before the request's response or source operation ID exists.
	binder.conn.Shared = false
	op.Payload = json.RawMessage(`{"restore":"restore"}`)
	requested := false
	for range 40 {
		stepOperation(t, p, rc, op)
		s, err := loadState(rc.Cluster)
		if err != nil {
			t.Fatal(err)
		}
		if s.RestoreRecovery != nil && s.RestoreRecovery.SourceRequested {
			requested = true
			if s.RestoreRecovery.SourceID != "" || s.RestoreRecovery.AcknowledgeShared {
				t.Fatal("unexpected source receipt or shared-source acknowledgement")
			}
			for _, member := range s.Members {
				service, err := rc.Backend.GetService(t.Context(), member.Name)
				if err != nil || service.Status != infra.StatusStopped {
					t.Fatal("runtime was not stopped before restore intent", err)
				}
			}
			break
		}
	}
	if !requested || binder.restores != 0 {
		t.Fatal("did not interrupt before the source request")
	}

	binder.shared = true
	binder.conn.Shared = true
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	const failure = "PostgreSQL source restore refused: shared source"
	if s.RestoreRecovery == nil || !s.RestoreRecovery.SourceFinished || s.RestoreRecovery.Failure != failure {
		t.Fatal("refusal did not durably transition to runtime recovery")
	}
	status, err := p.Status(t.Context(), rc)
	if err != nil {
		t.Fatal(err)
	}
	observedFailure := false
	for _, condition := range status.Conditions {
		if condition.Type == "TemporalRestoreRecovery" && strings.Contains(condition.Message, failure) {
			observedFailure = true
		}
	}
	if !observedFailure || status.Phase == domain.PhaseReady {
		t.Fatal("source refusal was not surfaced while recovery owned stopped runtimes")
	}

	converge(t, p, rc)
	if binder.restores != 1 {
		t.Fatal("refused source request was replayed again", binder.restores)
	}
	s, err = loadState(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	if s.RestoreRecovery != nil {
		t.Fatal("recovery fence was not released after readiness")
	}
	for _, member := range s.Members {
		service, err := rc.Backend.GetService(t.Context(), member.Name)
		if err != nil || service.Status != infra.StatusActive {
			t.Fatal("component did not restart after refused restore", err)
		}
		observed, err := componentStatus(t.Context(), rc, member)
		if err != nil || !observed.Ready {
			t.Fatal("component did not become Ready after refused restore", err)
		}
	}
}
