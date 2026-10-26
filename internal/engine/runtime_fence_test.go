// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"testing"
	"time"
)

type recoveryFenceBackend struct {
	infra.Backend
	state            infra.OpState
	polls            int
	hidden           bool
	observationError error
}

func (b *recoveryFenceBackend) OpStatus(context.Context, infra.Op) (infra.OpState, error) {
	b.polls++
	return b.state, nil
}
func (b *recoveryFenceBackend) GetService(ctx context.Context, name string) (*infra.Service, error) {
	if b.observationError != nil {
		return nil, b.observationError
	}
	if b.hidden && name == "member" {
		return nil, nil
	}
	return b.Backend.GetService(ctx, name)
}
func TestAcceptedRuntimeRecoveryDrainsBeforeDeletionAndInventory(t *testing.T) {
	rc, b, m := recoveryFixture(t, true)
	b.DeleteService(t.Context(), m.Member)
	if _, err := ReconcileRuntime(t.Context(), rc, m); err != nil {
		t.Fatal(err)
	}
	out, err := ReconcileRuntime(t.Context(), rc, m)
	if err != nil || !out.Busy || m.Recovery.Op == nil {
		t.Fatal(out, err)
	}
	raw, _ := json.Marshal(m.Recovery)
	m.Recovery = &RuntimeRecovery{}
	if err := json.Unmarshal(raw, m.Recovery); err != nil {
		t.Fatal(err)
	}
	delayed := &recoveryFenceBackend{Backend: b, hidden: true, observationError: infra.ErrUnavailable}
	rc.Backend = delayed
	rc.Cluster.Deleting = true
	out, err = ReconcileRuntime(t.Context(), rc, m)
	if !errors.Is(err, infra.ErrUnavailable) || !out.Busy || m.Recovery.Op == nil {
		t.Fatal("accepted work bypassed by deletion/stale inventory", out, err)
	}
	delayed.state = infra.OpState{Done: true}
	delayed.observationError = nil
	out, err = ObserveRuntimeRecovery(t.Context(), rc, m)
	if err != nil || !out.Busy || m.Recovery.Op == nil {
		t.Fatal("terminal import cleared before visibility", out, err)
	}
	delayed.hidden = false
	out, err = ObserveRuntimeRecovery(t.Context(), rc, m)
	if err != nil || !out.Imported || m.Recovery.Op != nil {
		t.Fatal("late import not adopted", out, err)
	}
	if *m.ServiceID == "" {
		t.Fatal("import attribution lost")
	}
}

func TestRuntimeRecoveryPersistsBackendRetryAfter(t *testing.T) {
	for _, action := range []string{"Start", "ImportServices"} {
		t.Run(action, func(t *testing.T) {
			rc, b, m := recoveryFixture(t, true)
			if action == "Start" {
				b.Stop(t.Context(), m.Member)
			} else {
				b.DeleteService(t.Context(), m.Member)
			}
			if action == "ImportServices" {
				if _, err := ReconcileRuntime(t.Context(), rc, m); err != nil {
					t.Fatal(err)
				}
			}
			reset := rc.Clock.Now().Add(time.Hour)
			b.FailNext(action, errors.Join(errors.New("submission unavailable"), fmt.Errorf("wrapped: %w", &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: reset})))
			_, err := ReconcileRuntime(t.Context(), rc, m)
			if err == nil || !infra.RetryAt(err).Equal(reset) || !m.Recovery.RetryAt.Equal(reset) {
				t.Fatal("retry constraint lost", m.Recovery, err)
			}
			raw, _ := json.Marshal(m.Recovery)
			m.Recovery = &RuntimeRecovery{}
			if err := json.Unmarshal(raw, m.Recovery); err != nil {
				t.Fatal(err)
			}
			before := countRecoveryCalls(b.Calls(), action)
			rc.Clock.(*recoveryClock).now = reset.Add(-time.Minute)
			if _, err := ReconcileRuntime(t.Context(), rc, m); err != nil {
				t.Fatal(err)
			}
			if countRecoveryCalls(b.Calls(), action) != before {
				t.Fatal("retried before persisted reset")
			}
			rc.Clock.(*recoveryClock).now = reset
			if _, err := ReconcileRuntime(t.Context(), rc, m); err != nil {
				t.Fatal(err)
			}
			if countRecoveryCalls(b.Calls(), action) != before+1 {
				t.Fatal("did not retry at reset")
			}
		})
	}
}
