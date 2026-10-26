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
	if rc.Agents == nil {
		return out
	}
	var workers sync.WaitGroup
	for i, m := range members {
		if m.Stage == "planned" || m.Stage == "imported" {
			continue
		}
		workers.Add(1)
		go func() {
			defer workers.Done()
			ctx, cancel := context.WithTimeout(ctx, engine.ObservationTimeout)
			defer cancel()
			if m.Role == "proxy" {
				out[i] = observeProxyStatus(ctx, rc, m)
				return
			}
			a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if err != nil {
				out[i].err = err
				return
			}
			// Generic and engine requests have independent HTTP transports.
			// Overlap cold TLS setup instead of spending two serial connection
			// budgets inside one bounded member observation.
			var probes sync.WaitGroup
			var agentErr, nativeErr error
			probes.Go(func() {
				out[i].agent, agentErr = a.Status(ctx)
				out[i].agentHealthy = agentErr == nil
			})
			probes.Go(func() {
				nativeErr = a.Engine(ctx, "postgres", "status", struct{}{}, &out[i].status)
			})
			probes.Wait()
			out[i].err = errors.Join(agentErr, nativeErr)
		}()
	}
	workers.Wait()
	return out
}

// Sample proxy endpoints alongside data members. Only writer-route evaluation
// depends on the current primary, not the HTTP observations themselves.
func observeProxyStatus(ctx context.Context, rc *engine.Context, m *member) memberObservation {
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
	containers, inventoryErr := rc.Backend.Containers(ctx, m.Name)
	_, out.proxyCount, out.err = engine.VisitObservedInstancesWithIdentity(ctx, rc, m.Name, engine.InstanceInventory{Containers: containers, Err: inventoryErr}, func(a *agentapi.Client, instance string) (bool, error) {
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
