// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

type unplacedResourceBackend struct {
	infra.Backend
	placed         bool
	observationErr error
	resizeErr      error
	calls          int
}

func (*unplacedResourceBackend) ResourceUpdatesDeployRuntime() bool { return true }
func (b *unplacedResourceBackend) ResizeUnplacedRuntime(context.Context, string, infra.Op, infra.Resources) (infra.Op, bool, error) {
	if b.placed || b.observationErr != nil {
		return infra.Op{}, false, b.observationErr
	}
	b.calls++
	return infra.Op{ID: "replacement", Kind: "resources"}, b.resizeErr == nil, b.resizeErr
}

func TestResizeUnplacedDeployment(t *testing.T) {
	for _, mode := range []string{"resize", "same", "placed", "deleting", "observation-error", "resize-error"} {
		t.Run(mode, func(t *testing.T) {
			b := &unplacedResourceBackend{}
			rc := &Context{Cluster: &domain.Cluster{}, Backend: b}
			old := infra.Resources{MinCPU: 4, MaxCPU: 4, MinRAM: 1, MaxRAM: 1}
			desired := infra.Resources{MinCPU: 1, MaxCPU: 1, MinRAM: .25, MaxRAM: .25}
			op := infra.Op{ID: "blocked", Kind: "deploy"}
			sentinel := errors.New("unavailable")
			switch mode {
			case "same":
				desired = old
			case "placed":
				b.placed = true
			case "deleting":
				rc.Cluster.Deleting = true
			case "observation-error":
				b.observationErr = sentinel
			case "resize-error":
				b.resizeErr = sentinel
			}
			applied := old
			changed, err := ResizeUnplacedDeployment(t.Context(), rc, "proxy", desired, &applied, &op)
			if mode == "resize" {
				if !changed || err != nil || op.ID != "replacement" || applied.MaxCPU != 1 || b.calls != 1 {
					t.Fatalf("resize: changed=%v err=%v op=%+v applied=%+v calls=%d", changed, err, op, applied, b.calls)
				}
				return
			}
			if changed || op.ID != "blocked" || applied.MaxCPU != 4 {
				t.Fatalf("unsafe mutation: changed=%v op=%+v applied=%+v", changed, op, applied)
			}
			if (mode == "observation-error" || mode == "resize-error") != errors.Is(err, sentinel) {
				t.Fatalf("error = %v", err)
			}
			if mode != "resize-error" && b.calls != 0 {
				t.Fatal("unexpected resource update")
			}
		})
	}
}

func TestUnplacedResizeDeferredRecordsBoundedFailure(t *testing.T) {
	failure := &infra.DeploymentExhaustedError{Service: "member", LastError: "UnplacedResizeDeferred: allocation appeared while fencing initial bootstrap"}
	backend := &unplacedResourceBackend{resizeErr: failure}
	events := &deploymentGuardEvents{}
	rc := &Context{Cluster: &domain.Cluster{Name: "demo", Namespace: "default"}, Backend: backend, Events: events}
	applied := infra.Resources{MinRAM: 4, MaxRAM: 4}
	op := infra.Op{ID: "initial", Kind: "deploy"}
	changed, err := ResizeUnplacedDeployment(t.Context(), rc, "member", infra.Resources{MinRAM: 1, MaxRAM: 1}, &applied, &op)
	if changed || !errors.Is(err, failure) || applied.MaxRAM != 4 || op.ID != "initial" {
		t.Fatalf("failed conditional mutation changed local intent: changed=%v err=%v", changed, err)
	}
	if len(events.events) != 1 || events.events[0].Reason != "UnplacedResizeDeferred" {
		t.Fatalf("missing bounded-failure event: %+v", events.events)
	}
}
