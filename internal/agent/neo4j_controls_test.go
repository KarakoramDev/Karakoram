// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package agent

import (
	"context"
	"testing"
	"time"
)

func TestEngineStartReleasesOnlyStopHold(t *testing.T) {
	for _, scenario := range []string{"running", "maintenance", "excluded"} {
		t.Run(scenario, func(t *testing.T) {
			a, c, _ := fakeNode(t, buildFake(t))
			if scenario == "maintenance" {
				c.Maintenance = true
			}
			if scenario == "excluded" {
				c.DesiredProcesses = []string{}
			}
			ctx, cancel := context.WithTimeout(t.Context(), 10*time.Second)
			defer cancel()
			if _, err := a.apply(ctx, c); err != nil {
				t.Fatal(err)
			}
			n := &Node{a: a}
			if err := n.Stop(ctx, "worker"); err != nil {
				t.Fatal(err)
			}
			if !n.Stopped("worker") {
				t.Fatal("missing durable hold")
			}
			if err := n.Start(ctx, "worker"); err != nil {
				t.Fatal(err)
			}
			controls, err := loadControls(a.o.DataDir)
			if err != nil || controls.Stopped["worker"] || n.Stopped("worker") {
				t.Fatal("start retained hold", err)
			}
			if scenario == "running" {
				eventuallyTrue(t, func() bool { return workerState(a).PID > 0 })
			} else if workerState(a).PID != 0 {
				t.Fatal("start bypassed maintenance or desired exclusion")
			}
			if err := n.Start(ctx, "unregistered"); err == nil {
				t.Fatal("accepted unknown process")
			}
		})
	}
}

func TestShutdownCheckpointBudget(t *testing.T) {
	for _, tc := range []struct{ budget, want time.Duration }{{0, 10 * time.Second}, {time.Second, 10 * time.Second}, {30 * time.Second, 30 * time.Second}, {time.Hour, 45 * time.Second}} {
		if got := shutdownChildTimeout(context.Background(), tc.budget, 1); got != tc.want {
			t.Fatalf("budget %s produced %s, want %s", tc.budget, got, tc.want)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()
	if got := shutdownChildTimeout(ctx, 45*time.Second, 2); got <= 0 || got > 500*time.Millisecond {
		t.Fatal("checkpoint budget exceeded outer deadline share", got)
	}
}
