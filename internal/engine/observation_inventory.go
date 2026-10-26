// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

const observationConcurrency = 8

// InstanceInventory is one member's infrastructure observation from this sweep.
// Consumers must not mutate Containers or reuse this sample in another sweep.
type InstanceInventory struct {
	Containers []infra.Container
	Err        error
}

// PlatformObservation shares project, service and instance inventory within one
// sweep. Member results preserve the caller's member order.
type PlatformObservation struct {
	Project     infra.Project
	ProjectErr  error
	Services    []infra.Service
	ServicesErr error
	Members     []InstanceInventory
}

// ObservePlatform overlaps independent read-only platform calls with at most
// eight requests in flight. Callers can overlap it with native/agent probes.
// Each result retains its own error instead of substituting cached success.
func ObservePlatform(ctx context.Context, backend infra.Backend, cluster string, members []string) PlatformObservation {
	ctx = infra.ObservationContext(ctx)
	out := PlatformObservation{Members: make([]InstanceInventory, len(members))}
	var workers sync.WaitGroup
	var next int
	var mu sync.Mutex
	for range min(observationConcurrency, len(members)+2) {
		workers.Go(func() {
			for {
				mu.Lock()
				job := next
				next++
				mu.Unlock()
				if job >= len(members)+2 {
					return
				}
				switch job {
				case 0:
					out.Project, out.ProjectErr = backend.ProjectInfo(ctx)
				case 1:
					out.Services, out.ServicesErr = backend.ListServices(ctx, cluster)
				default:
					i := job - 2
					out.Members[i].Containers, out.Members[i].Err = backend.Containers(ctx, members[i])
				}
			}
		})
	}
	workers.Wait()
	return out
}
