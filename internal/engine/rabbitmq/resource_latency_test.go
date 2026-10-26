// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Model a backend whose resource receipt already publishes the stored runtime.
// A second deployment costs another stop/start window and must not be requested.
type resourceDeploymentBackend struct {
	infra.Backend
	clock *testClock
}

func (*resourceDeploymentBackend) ResourceUpdatesDeployRuntime() bool { return true }
func (b *resourceDeploymentBackend) SetResources(ctx context.Context, name string, resources infra.Resources) (infra.Op, error) {
	b.clock.at = b.clock.at.Add(20 * time.Second)
	return b.Backend.SetResources(ctx, name, resources)
}
func (b *resourceDeploymentBackend) Deploy(ctx context.Context, name string, deployment infra.Deployment) (infra.Op, error) {
	b.clock.at = b.clock.at.Add(120 * time.Second)
	return b.Backend.Deploy(ctx, name, deployment)
}

func TestResourceReplacementUsesOneGatedDeployment(t *testing.T) {
	for _, kind := range []string{"cpu", "ram"} {
		t.Run(kind, func(t *testing.T) {
			p, rc, _, agents := setup(t)
			ready(t, p, rc)
			clock := rc.Clock.(*testClock)
			start := clock.Now()
			rc.Backend = &resourceDeploymentBackend{Backend: rc.Backend, clock: clock}
			agents.log.clear()
			resources := &rc.Cluster.Spec.Components.Engine.Resources
			if kind == "cpu" {
				resources.MinCPU, resources.MaxCPU = 2, 2
			} else {
				resources.MinRAM, resources.MaxRAM = 2, 2
			}
			syncUntil(t, p, rc, func(s *internalState) bool {
				if s.Phase != "ready" || s.Pending != nil || s.Retry != nil {
					return false
				}
				for _, m := range s.Members {
					if m.ChangeStep != "" || m.Resources != runtimeResources(*resources) || m.ConfigResources != runtimeResources(*resources) {
						return false
					}
				}
				return true
			})
			if elapsed := clock.Now().Sub(start); elapsed != 60*time.Second {
				t.Fatalf("already-applied runtime was redeployed: modeled platform time=%s, want 60s", elapsed)
			}
			calls := agents.log.snapshot()
			for i, m := range ordered(state(t, rc)) {
				assertBefore(t, calls, "agent:"+m.Name+":drain", "agent:"+m.Name+":stop")
				assertBefore(t, calls, "agent:"+m.Name+":stop", "infra:"+m.Name+":resources")
				assertBefore(t, calls, "infra:"+m.Name+":resources", "agent:"+m.Name+":/v1/reload")
				assertBefore(t, calls, "agent:"+m.Name+":/v1/reload", "agent:"+m.Name+":start")
				if i > 0 {
					assertBefore(t, calls, "agent:"+ordered(state(t, rc))[i-1].Name+":revive", "infra:"+m.Name+":resources")
				}
				if countCall(calls, "infra:"+m.Name+":deploy") != 0 || countCall(calls, "infra:"+m.Name+":resources") != 1 {
					t.Fatalf("duplicate resource/runtime publication: %v", calls)
				}
			}
		})
	}
}
