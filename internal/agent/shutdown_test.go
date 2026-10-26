// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"errors"
	"testing"
	"time"
)

type shutdownEngine struct {
	*fakeEngine
	hook func(context.Context, *Node) error
}

func (e *shutdownEngine) Shutdown(ctx context.Context, n *Node) error { return e.hook(ctx, n) }

func TestEngineShutdownPrecedesChildSignals(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "success", true: "failed_handoff"}[fail], func(t *testing.T) {
			a, c, e := fakeNode(t, "/bin/sleep")
			a.sup.configure("worker", processSpec{binary: "/bin/sleep", args: []string{"60"}})
			if err := a.sup.action(context.Background(), "worker", "start"); err != nil {
				t.Fatal(err)
			}
			a.c = c
			called := false
			a.engine = &shutdownEngine{fakeEngine: e, hook: func(ctx context.Context, n *Node) error {
				called = true
				if ctx.Err() != nil {
					t.Fatal("handoff inherited canceled runtime context")
				}
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > 4*time.Second {
					t.Fatal("unbounded handoff")
				}
				states, _ := n.Snapshot()
				if states["worker"].PID == 0 {
					t.Fatal("child stopped before handoff")
				}
				if fail {
					return errors.New("no safe candidate")
				}
				return nil
			}}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			_ = a.shutdown(ctx)
			if !called || workerState(a).PID != 0 {
				t.Fatal("handoff failure must still stop children", called, workerState(a))
			}
		})
	}
}

type drainingShutdownEngine struct {
	*shutdownEngine
	budget time.Duration
}

func (e *drainingShutdownEngine) ShutdownBudget() time.Duration { return e.budget }

func TestEngineShutdownDrainBudgetIsBoundedBeforeChildStop(t *testing.T) {
	for _, requested := range []time.Duration{45 * time.Second, time.Hour, 0} {
		t.Run(requested.String(), func(t *testing.T) {
			a, c, e := fakeNode(t, "/bin/sleep")
			a.c = c
			a.sup.configure("worker", processSpec{binary: "/bin/sleep", args: []string{"60"}})
			if err := a.sup.action(t.Context(), "worker", "start"); err != nil {
				t.Fatal(err)
			}
			called := false
			a.engine = &drainingShutdownEngine{budget: requested, shutdownEngine: &shutdownEngine{fakeEngine: e, hook: func(ctx context.Context, n *Node) error {
				called = true
				want := 45 * time.Second
				if requested == 0 {
					want = 4 * time.Second
				}
				deadline, ok := ctx.Deadline()
				if !ok || time.Until(deadline) > want || time.Until(deadline) < want-time.Second {
					t.Fatal("wrong shutdown budget", time.Until(deadline), want)
				}
				states, _ := n.Snapshot()
				if states["worker"].PID == 0 {
					t.Fatal("child received stop before drain")
				}
				return nil
			}}}
			if err := a.shutdown(t.Context()); err != nil {
				t.Fatal(err)
			}
			if !called || workerState(a).PID != 0 {
				t.Fatal("drain did not precede child stop")
			}
		})
	}
}
