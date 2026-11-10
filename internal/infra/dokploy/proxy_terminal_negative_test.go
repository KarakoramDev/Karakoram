// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestOrdinaryProxyNonterminalDoesNotGainAuthority(t *testing.T) {
	for _, outcome := range []string{"updating", "unknown", "timeout", "transport", "legacy-failed", "old-cp-failed", "superseded", "predecessor-paused", "predecessor-rollback", "reload-not-applied"} {
		t.Run(outcome, func(t *testing.T) {
			f, b, a := markedProxyFake(t, 2)
			op, err := b.SetContainerCount(t.Context(), "proxy", 3, 3)
			if err != nil {
				t.Fatal(err)
			}
			f.mu.Lock()
			a.holdTasks = true
			service := f.services[fakeString(a.row, "appName")]
			state := "updating"
			if outcome == "unknown" {
				state = ""
			}
			if outcome == "legacy-failed" || outcome == "old-cp-failed" || outcome == "superseded" {
				state = "paused"
			}
			if outcome == "predecessor-paused" {
				state = "paused"
			}
			if outcome == "predecessor-rollback" {
				state = "rollback_completed"
			}
			service["UpdateStatus"] = map[string]any{"State": state}
			m, _ := parseMetadata(fakeString(a.row, "description"))
			switch outcome {
			case "transport":
				f.failure["docker.getConfig"] = fakeResponse{500, "INTERNAL_SERVER_ERROR", "unavailable"}
			case "legacy-failed":
				m.Pending.HealthGated = false
				m.ProxyServingProbe = false
			case "old-cp-failed":
				m.Pending.Failed = true
			case "superseded":
				m.Pending.Issued = m.Pending.Issued.Add(time.Second)
			case "predecessor-paused", "predecessor-rollback", "reload-not-applied":
				service["Spec"].(map[string]any)["TaskTemplate"].(map[string]any)["ForceUpdate"] = m.Pending.ForceUpdate - 1
			}
			a.row["description"] = m.description()
			f.mu.Unlock()
			if outcome == "timeout" {
				o, _ := decodeOp(op)
				b.now = func() time.Time { return o.Issued.Add(taskStartupWindow + time.Second) }
			}
			st, err := b.OpStatus(t.Context(), op)
			if outcome == "transport" && err == nil {
				t.Fatal("transport failure was not exercised")
			}
			// A pre-fix failed receipt can gain authority only by freshly observing
			// the same exact marked receipt's authoritative live pause.
			if outcome == "old-cp-failed" {
				if err != nil || st.ProxyRolloutFailure == nil {
					t.Fatalf("old CP receipt not re-observed: %+v %v", st, err)
				}
				return
			}
			if st.ProxyRolloutFailure != nil {
				t.Fatalf("ambiguous/legacy outcome classified terminal: %+v %v", st, err)
			}
			if outcome == "updating" || outcome == "unknown" {
				if err != nil || st.Failed || st.Done {
					t.Fatalf("in-progress rollout became terminal: %+v %v", st, err)
				}
			}
			var terminal *infra.ProxyRolloutFailureError
			if errors.As(err, &terminal) {
				t.Fatalf("unrelated error made terminal: %v", err)
			}
		})
	}
}

type proxyObservationBackend struct {
	infra.Backend
	status infra.OpState
	err    error
}

func (b proxyObservationBackend) OpStatus(_ context.Context, _ infra.Op) (infra.OpState, error) {
	return b.status, b.err
}

func TestOrdinaryProxyAmbiguityRemainsRetryable(t *testing.T) {
	for _, outcome := range []string{"stalled", "expired", "unknown", "transport", "data-member", "non-serving"} {
		t.Run(outcome, func(t *testing.T) {
			st := infra.OpState{Failed: true, Status: "failed"}
			var want error
			switch outcome {
			case "stalled":
				st.Status = "stalled"
			case "expired":
				st.Expired = true
			case "unknown":
				st.Status = "unknown"
			case "transport":
				want = infra.ErrUnavailable
			}
			_, err := engine.ObserveProxyRollout(t.Context(), &engine.Context{Backend: proxyObservationBackend{status: st, err: want}}, infra.Op{}, "proxy", 1, outcome != "data-member")
			var terminal *infra.ProxyRolloutFailureError
			if errors.As(err, &terminal) || !errors.Is(err, want) {
				t.Fatalf("ambiguous failure changed: %v", err)
			}
		})
	}
}
