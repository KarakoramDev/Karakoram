// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package agent

import (
	"context"
	"errors"
	"testing"
)

type preparingEngine struct {
	*fakeEngine
	prepareErr error
	prepared   []string
}

func (e *preparingEngine) BeforeStart(_ context.Context, n *Node, name string) error {
	if name != "worker" {
		return errors.New("wrong process")
	}
	e.prepared = append(e.prepared, string(n.Config().EngineSpec))
	return e.prepareErr
}

func TestEnginePreparationBeforeReleasedStartAndRestart(t *testing.T) {
	for _, action := range []string{"start", "restart"} {
		t.Run(action, func(t *testing.T) {
			a, c, base := fakeNode(t, buildFake(t))
			e := &preparingEngine{fakeEngine: base}
			a.engine = e
			ctx := t.Context()
			if _, err := a.apply(ctx, c); err != nil {
				t.Fatal(err)
			}
			if len(e.prepared) != 1 {
				t.Fatal("configuration start skipped preparation")
			}
			if err := a.processAction(ctx, "worker", "stop"); err != nil {
				t.Fatal(err)
			}
			c.ConfigVersion++
			c.EngineSpec = []byte(`{"port":5678}`)
			if _, err := a.apply(ctx, c); err != nil {
				t.Fatal(err)
			}
			if len(e.prepared) != 1 || workerState(a).PID != 0 {
				t.Fatal("held config prepared or started")
			}
			e.prepareErr = errors.New("migration failed")
			if err := a.processAction(ctx, "worker", action); !errors.Is(err, e.prepareErr) {
				t.Fatal("failed preparation did not prevent start", err)
			}
			if workerState(a).PID != 0 || !(&Node{a: a}).Stopped("worker") {
				t.Fatal("failed preparation released stop hold")
			}
			if e.prepared[len(e.prepared)-1] != string(c.EngineSpec) {
				t.Fatal("explicit start prepared an outdated payload")
			}
			e.prepareErr = nil
			if err := a.processAction(ctx, "worker", action); err != nil {
				t.Fatal(err)
			}
			if workerState(a).PID == 0 || (&Node{a: a}).Stopped("worker") {
				t.Fatal("successful preparation did not release/start process")
			}
		})
	}
}

func TestEnginePreparationFailurePreventsConfigurationStart(t *testing.T) {
	a, c, base := fakeNode(t, buildFake(t))
	c.ConfigVersion = 1
	e := &preparingEngine{fakeEngine: base, prepareErr: errors.New("migration failed")}
	a.engine = e
	if _, err := a.apply(t.Context(), c); !errors.Is(err, e.prepareErr) {
		t.Fatal("failed preparation accepted", err)
	}
	if workerState(a).PID != 0 {
		t.Fatal("failed preparation started process")
	}
	if a.config().ConfigVersion == c.ConfigVersion {
		t.Fatal("failed preparation accepted config version")
	}
}
