// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// VisitLiveInstances obtains fresh infrastructure inventory and visits every
// ACTIVE/RUNNING instance using a pinned client that retains the logical member's
// TLS and bearer identity. It prefers the backend's private address family, then
// an individual hostname; missing or logical-service-only addresses fail closed.
// The visitor must not retain its client: it is closed after each call.
//
// The results are the conjunction of visitor acknowledgements, the number of
// live instances, and any inventory/address/probe errors. Empty inventory (or a
// separately confirmed absent service) returns true with count zero; readiness
// callers must also require a positive count. No acknowledgements are cached, so
// stopped/disappeared instances are excluded and replacements are visited anew.
func VisitLiveInstances(ctx context.Context, rc *Context, member string, visit func(*agentapi.Client) (bool, error)) (bool, int, error) {
	inventory := ObserveInstanceInventory(ctx, rc, member)
	return visitObservedInstances(ctx, rc, member, inventory, func(client *agentapi.Client, _ string) (bool, error) { return visit(client) }, false)
}

// VisitLiveInstancesConcurrent has the live-inventory contract of
// VisitLiveInstances, but visits at most eight instances concurrently. The
// visitor must be concurrency-safe and must honor the supplied context.
func VisitLiveInstancesConcurrent(ctx context.Context, rc *Context, member string, visit func(*agentapi.Client) (bool, error)) (bool, int, error) {
	inventory := ObserveInstanceInventory(ctx, rc, member)
	return VisitObservedInstances(ctx, rc, member, inventory, visit)
}

// VisitObservedInstances visits a freshly collected inventory without reading
// the platform again. At most eight pinned instance probes run concurrently.
// The inventory belongs to the current sweep; it must never be carried forward.
func VisitObservedInstances(ctx context.Context, rc *Context, member string, inventory InstanceInventory, visit func(*agentapi.Client) (bool, error)) (bool, int, error) {
	return VisitObservedInstancesWithIdentity(ctx, rc, member, inventory, func(client *agentapi.Client, _ string) (bool, error) { return visit(client) })
}

// VisitObservedInstancesWithIdentity is VisitObservedInstances with the platform
// instance ID passed to each visitor, for instance-specific diagnostics.
func VisitObservedInstancesWithIdentity(ctx context.Context, rc *Context, member string, inventory InstanceInventory, visit func(*agentapi.Client, string) (bool, error)) (bool, int, error) {
	return visitObservedInstances(ctx, rc, member, inventory, visit, true)
}

// VisitLiveInstancesWithIdentity collects fresh inventory and visits each pinned
// instance sequentially, passing the platform instance ID to the visitor.
func VisitLiveInstancesWithIdentity(ctx context.Context, rc *Context, member string, visit func(*agentapi.Client, string) (bool, error)) (bool, int, error) {
	return visitObservedInstances(ctx, rc, member, ObserveInstanceInventory(ctx, rc, member), visit, false)
}

// ObserveInstanceInventory reads this sweep's instances, distinguishing verified
// runtime absence from a failed platform read.
func ObserveInstanceInventory(ctx context.Context, rc *Context, member string) InstanceInventory {
	containers, err := rc.Backend.Containers(ctx, member)
	if errors.Is(err, infra.ErrNotFound) {
		service, observeErr := rc.Backend.GetService(ctx, member)
		if observeErr == nil && service == nil {
			return InstanceInventory{}
		}
		err = errors.Join(err, observeErr)
	}
	return InstanceInventory{Containers: containers, Err: err}
}

func visitObservedInstances(ctx context.Context, rc *Context, member string, inventory InstanceInventory, visit func(*agentapi.Client, string) (bool, error), concurrent bool) (bool, int, error) {
	if inventory.Err != nil {
		return false, 0, inventory.Err
	}
	count := 0
	for _, container := range inventory.Containers {
		if container.Status == infra.ContainerRunning || container.Status == infra.ContainerActive {
			count++
		}
	}
	if count == 0 {
		return true, 0, nil
	}
	if rc.Agents == nil {
		return false, count, ErrUnavailable
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), member)
	if err != nil {
		return false, count, err
	}
	visitOne := func(container infra.Container) (bool, error) {
		address := container.IPv6
		if rc.Backend.Layout().PrivateAddressFamily == infra.AddressFamilyIPv4 {
			address = container.IPv4
		}
		if address == "" {
			address = container.Hostname
		}
		if address == "" || address == member {
			return false, fmt.Errorf("member %s instance %s has no individual address", member, container.ID)
		}
		instance, err := a.ForInstance(address)
		if err != nil {
			return false, fmt.Errorf("member %s instance %s: %w", member, container.ID, err)
		}
		defer instance.Close()
		ok, err := visit(instance, container.ID)
		if err != nil {
			err = fmt.Errorf("member %s instance %s: %w", member, container.ID, err)
		}
		return ok, err
	}
	all := true
	var failures error
	if !concurrent {
		for _, container := range inventory.Containers {
			if container.Status != infra.ContainerRunning && container.Status != infra.ContainerActive {
				continue
			}
			ok, err := visitOne(container)
			all = all && ok
			failures = errors.Join(failures, err)
		}
		return all, count, failures
	}
	acknowledged := make([]bool, len(inventory.Containers))
	failures = infra.ObserveParallel(ctx, len(inventory.Containers), func(i int) error {
		container := inventory.Containers[i]
		if container.Status != infra.ContainerRunning && container.Status != infra.ContainerActive {
			return nil
		}
		ok, err := visitOne(container)
		acknowledged[i] = ok
		return err
	})
	for i, container := range inventory.Containers {
		if container.Status != infra.ContainerRunning && container.Status != infra.ContainerActive {
			continue
		}
		all = all && acknowledged[i]
	}
	return all, count, failures
}
