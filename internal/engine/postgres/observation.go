// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/redact"
)

type memberObservation struct {
	status         pgspec.StatusResponse
	err            error
	agent          agentapi.Status
	agentHealthy   bool
	proxyInstances []proxyInstanceObservation
	proxyCount     int

	// Service discovery is independent of per-instance native health.
	proxyEndpointErr error
	runtime          *infra.Service
	runtimeErr       error
	inventory        engine.InstanceInventory
}

type proxyInstanceObservation struct {
	instance     string
	agent        agentapi.Status
	agentHealthy bool
	status       pgspec.StatusResponse
	err          error
}

func observeLiveMembers(ctx context.Context, rc *engine.Context, members []*member) []memberObservation {
	out := make([]memberObservation, len(members))
	var workers sync.WaitGroup
	for i, m := range members {
		workers.Add(1)
		go func() {
			defer workers.Done()
			out[i].status, out[i].err = live(ctx, rc, m.Name)
		}()
	}
	workers.Wait()
	return out
}

// Keep transient observation failures visible without publishing credentials or
// unbounded remote diagnostics in the generation wait condition.
func nativeObservationWait(name string, err error) string {
	if err == nil {
		err = errors.New("native process is not running")
	}
	return fmt.Sprintf("waiting for member %s native running observation: %s", name, observationErrorSummary(err))
}

func observationErrorSummary(err error) string {
	detail := strings.Join(strings.Fields(redact.Diagnostic(err.Error())), " ")
	const limit = 512
	if len(detail) > limit {
		detail = detail[:limit] + "..."
	}
	return detail
}

func observeStatusMembers(ctx context.Context, rc *engine.Context, members []*member) []memberObservation {
	out := make([]memberObservation, len(members))
	// Inventory and native reads share a sweep, not a serial budget per
	// member. Keep each result indexed by member; reduction remains serial.
	var workers sync.WaitGroup
	for i, m := range members {
		if m.Stage == "planned" || m.Stage == "imported" {
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			var sample memberObservation
			var runtime *infra.Service
			var runtimeErr error
			var inventory engine.InstanceInventory
			var probes sync.WaitGroup
			probes.Go(func() {
				runtime, runtimeErr = rc.Backend.GetService(ctx, m.Name)
			})
			probes.Go(func() {
				if m.Role == "proxy" && rc.Agents != nil {
					probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
					defer cancel()
					sample = observeProxyInventoryStatus(probe, rc, m, func() engine.InstanceInventory {
						inventory.Containers, inventory.Err = rc.Backend.Containers(probe, m.Name)
						return inventory
					})
				} else {
					inventory.Containers, inventory.Err = rc.Backend.Containers(ctx, m.Name)
				}
			})
			if m.Role != "proxy" && rc.Agents != nil {
				probes.Go(func() {
					probe, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
					defer cancel()
					a, err := rc.Agents.Dial(probe, rc.Cluster.Ref(), m.Name)
					if err != nil {
						sample.err = err
						return
					}
					// Generic and engine requests use independent transports.
					var native sync.WaitGroup
					var agentErr, nativeErr error
					native.Go(func() {
						sample.agent, agentErr = a.Status(probe)
						sample.agentHealthy = agentErr == nil
					})
					native.Go(func() {
						nativeErr = a.Engine(probe, "postgres", "status", struct{}{}, &sample.status)
					})
					native.Wait()
					sample.err = errors.Join(agentErr, nativeErr)
				})
			}
			probes.Wait()
			sample.runtime, sample.runtimeErr, sample.inventory = runtime, runtimeErr, inventory
			out[i] = sample
		}()
	}
	workers.Wait()
	return out
}

// Sample proxy endpoints alongside data members. Only writer-route evaluation
// depends on the current primary, not the HTTP observations themselves.
func observeProxyStatus(ctx context.Context, rc *engine.Context, m *member) memberObservation {
	return observeProxyInventoryStatus(ctx, rc, m, func() engine.InstanceInventory {
		containers, err := rc.Backend.Containers(ctx, m.Name)
		return engine.InstanceInventory{Containers: containers, Err: err}
	})
}

func observeProxyInventoryStatus(ctx context.Context, rc *engine.Context, m *member, inventory func() engine.InstanceInventory) memberObservation {
	var out memberObservation
	// A healthy task cannot prove that clients can reach the advertised
	// logical endpoint. Probe the same namespace/DNS/VIP path used by Sync;
	// overlap it with instance visits within this sweep's observation budget.
	var endpoint sync.WaitGroup
	endpoint.Go(func() {
		status, err := live(ctx, rc, m.Name)
		if err == nil && !status.Running {
			err = errors.New("native process is not running")
		}
		out.proxyEndpointErr = err
	})
	var mu sync.Mutex
	_, out.proxyCount, out.err = engine.VisitObservedInstancesWithIdentity(ctx, rc, m.Name, inventory(), func(a *agentapi.Client, instance string) (bool, error) {
		sample := proxyInstanceObservation{instance: instance}
		var genericErr, nativeErr error
		var probes sync.WaitGroup
		probes.Go(func() {
			sample.agent, genericErr = a.Status(ctx)
			sample.agentHealthy = genericErr == nil
		})
		probes.Go(func() {
			nativeErr = a.Engine(ctx, "postgres", "status", struct{}{}, &sample.status)
		})
		probes.Wait()
		sample.err = errors.Join(genericErr, nativeErr)
		mu.Lock()
		out.proxyInstances = append(out.proxyInstances, sample)
		mu.Unlock()
		return sample.err == nil && sample.status.Running, sample.err
	})
	endpoint.Wait()
	slices.SortFunc(out.proxyInstances, func(a, b proxyInstanceObservation) int {
		return strings.Compare(a.instance, b.instance)
	})
	return out
}
