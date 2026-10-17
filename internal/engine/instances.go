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
	containers, err := rc.Backend.Containers(ctx, member)
	if err != nil {
		if errors.Is(err, infra.ErrNotFound) {
			service, observeErr := rc.Backend.GetService(ctx, member)
			if observeErr == nil && service == nil {
				return true, 0, nil
			}
			err = errors.Join(err, observeErr)
		}
		return false, 0, err
	}
	all, count := true, 0
	var failures error
	var a *agentapi.Client
	for _, container := range containers {
		if container.Status != infra.ContainerRunning && container.Status != infra.ContainerActive {
			continue
		}
		count++
		if a == nil {
			if rc.Agents == nil {
				return false, count, ErrUnavailable
			}
			a, err = rc.Agents.Dial(ctx, rc.Cluster.Ref(), member)
			if err != nil {
				return false, count, err
			}
		}
		address := container.IPv6
		if rc.Backend.Layout().PrivateAddressFamily == infra.AddressFamilyIPv4 {
			address = container.IPv4
		}
		if address == "" {
			address = container.Hostname
		}
		if address == "" || address == member {
			all = false
			failures = errors.Join(failures, fmt.Errorf("member %s instance %s has no individual address", member, container.ID))
			continue
		}
		instance, err := a.ForInstance(address)
		if err == nil {
			var ok bool
			ok, err = visit(instance)
			instance.Close()
			all = all && ok
		}
		if err != nil {
			all = false
			failures = errors.Join(failures, fmt.Errorf("member %s instance %s: %w", member, container.ID, err))
		}
	}
	return all, count, failures
}
