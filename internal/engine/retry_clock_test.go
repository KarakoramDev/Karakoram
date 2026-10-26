// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package engine

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestSharedRetryClockSteps(t *testing.T) {
	for _, pattern := range []string{"import-plan", "observe-import", "recover-import", "recover-start"} {
		for _, delay := range []time.Duration{time.Minute, 48 * time.Hour} {
			for _, step := range []struct {
				name  string
				shift time.Duration
				due   bool
			}{
				{"unchanged", 0, false}, {"small-backward", -2 * time.Second, false},
				{"large-backward", -time.Hour, true}, {"forward", delay + time.Second, true},
			} {
				t.Run(pattern+"/"+delay.String()+"/"+step.name, func(t *testing.T) {
					rc, b, m := recoveryFixture(t, true)
					action := "ImportServices"
					if pattern == "recover-start" {
						action = "Start"
						b.Stop(t.Context(), m.Member)
					} else {
						b.DeleteService(t.Context(), m.Member)
						if _, err := ReconcileRuntime(t.Context(), rc, m); err != nil {
							t.Fatal(err)
						}
					}
					run := func() error {
						switch pattern {
						case "import-plan":
							_, err := m.Recovery.ImportPlan.Submit(t.Context(), rc, m.Member, m.Token)
							return err
						case "observe-import":
							_, err := ObserveRuntimeRecovery(t.Context(), rc, m)
							return err
						default:
							_, err := ReconcileRuntime(t.Context(), rc, m)
							return err
						}
					}
					clock := rc.Clock.(*recoveryClock)
					recorded := clock.now
					b.FailNext(action, &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: recorded.Add(delay)})
					if err := run(); err == nil {
						t.Fatal("expected scheduling failure")
					}
					raw, err := json.Marshal(m.Recovery)
					if err != nil {
						t.Fatal(err)
					}
					m.Recovery = &RuntimeRecovery{}
					if err := json.Unmarshal(raw, m.Recovery); err != nil {
						t.Fatal(err)
					}
					before := countRecoveryCalls(b.Calls(), action)
					clock.now = recorded.Add(step.shift)
					if step.due {
						b.FailNext(action, &infra.RetryAfterError{Cause: infra.ErrUnavailable, At: clock.now.Add(48 * time.Hour)})
					}
					_ = run()
					want := before
					if step.due {
						want++
					}
					if got := countRecoveryCalls(b.Calls(), action); got != want {
						t.Fatalf("backend calls=%d want=%d", got, want)
					}
					if step.due {
						// An early retry must honor a fresh platform reset,
						// including after the new schedule is reconstructed.
						raw, err := json.Marshal(m.Recovery)
						if err != nil {
							t.Fatal(err)
						}
						m.Recovery = &RuntimeRecovery{}
						if err := json.Unmarshal(raw, m.Recovery); err != nil {
							t.Fatal(err)
						}
						for range 3 {
							_ = run()
						}
						if got := countRecoveryCalls(b.Calls(), action); got != want {
							t.Fatalf("fresh platform reset bypassed after clock step: calls=%d want=%d", got, want)
						}
					}
				})
			}
		}
	}
}
