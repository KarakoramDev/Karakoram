// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package pbm

import (
	"encoding/json"
	"testing"
	"time"
)

func TestRouterRetryClockSteps(t *testing.T) {
	for _, action := range []string{"start", "stop"} {
		for _, step := range []struct {
			name  string
			shift time.Duration
			due   bool
		}{
			{"unchanged", 0, false}, {"small-backward", -2 * time.Second, false},
			{"large-backward", -time.Hour, true}, {"forward", time.Minute, true},
		} {
			t.Run(action+"/"+step.name, func(t *testing.T) {
				env, _, _, _ := setup(t)
				env.Routers = []string{"routers"}
				backend := newRetryRouterBackend(action, "FAILED", false)
				if action == "start" {
					backend.reached("stop")
				}
				clock := &routerRetryClock{at: time.Date(2024, 1, 6, 12, 0, 0, 0, time.UTC)}
				env.RC.Backend, env.RC.Clock = backend, clock
				cp := checkpoint{}
				if _, err := routerService(t.Context(), env, &cp, action); err != nil {
					t.Fatal(err)
				}
				if _, err := routerService(t.Context(), env, &cp, action); err == nil {
					t.Fatal("expected failed process")
				}
				raw, err := json.Marshal(cp)
				if err != nil {
					t.Fatal(err)
				}
				cp = checkpoint{}
				if err := json.Unmarshal(raw, &cp); err != nil {
					t.Fatal(err)
				}
				clock.at = clock.at.Add(step.shift)
				_, _ = routerService(t.Context(), env, &cp, action)
				want := 1
				if step.due {
					want++
				}
				if backend.attempts[action] != want {
					t.Fatalf("backend attempts=%d want=%d", backend.attempts[action], want)
				}
			})
		}
	}
}
