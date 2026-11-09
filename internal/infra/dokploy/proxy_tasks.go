// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package dokploy

import (
	"context"
	"net/netip"

	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Inspect only the fields needed for startup admission; task specifications can
// contain credentials and must never be included in diagnostics.
type swarmTask struct {
	ID           string `json:"ID"`
	ServiceID    string `json:"ServiceID"`
	DesiredState string `json:"DesiredState"`
	Status       struct {
		State string `json:"State"`
	} `json:"Status"`
	Spec struct {
		ForceUpdate   uint64 `json:"ForceUpdate"`
		ContainerSpec struct {
			Env []string `json:"Env"`
		} `json:"ContainerSpec"`
	} `json:"Spec"`
	NetworksAttachments []struct {
		Network struct {
			ID   string `json:"ID"`
			Spec struct {
				Name string `json:"Name"`
			} `json:"Spec"`
			DriverState struct {
				Name string `json:"Name"`
			} `json:"DriverState"`
		} `json:"Network"`
		Addresses []string `json:"Addresses"`
	} `json:"NetworksAttachments"`
}

func (b *Backend) startingProxyTask(ctx context.Context, a *application, m metadata, task task, container *infra.Container) error {
	service, err := b.inspectService(ctx, a.AppName)
	if err != nil {
		return err
	}
	if service == nil || !b.ownsService(*a, m, service) {
		return restoreConflict("cannot inspect startup tasks without the owned Swarm service")
	}
	// The API invokes generic docker inspect. Full alphanumeric task IDs prevent
	// name/prefix ambiguity and keep its shell-backed argument constrained.
	if len(task.ID) != 25 {
		return restoreConflict("startup task identity is invalid")
	}
	for _, c := range task.ID {
		if c < 'a' || c > 'z' {
			if c < '0' || c > '9' {
				return restoreConflict("startup task identity is invalid")
			}
		}
	}
	var inspected *swarmTask
	if err := b.c.get(ctx, "docker.getConfig", map[string]string{"containerId": task.ID}, &inspected); err != nil {
		return err
	}
	if inspected == nil || inspected.ID != task.ID || inspected.ServiceID != service.ID {
		return restoreConflict("startup task inspection does not match the owned service")
	}
	if inspected.DesiredState != "running" || inspected.Status.State != "starting" {
		// The task transitioned after enumeration. Do not authorize its old address.
		container.Status = infra.StatusCreating
		return nil
	}
	// A rejected roll remains fenced after engine checkpoint cleanup.
	// Only an accepted, distinct serving retry can authorize new tasks.
	if proxyTaskAdmissionAllowed(m, service, task.ID, inspected.Spec.ForceUpdate) {
		for _, value := range inspected.Spec.ContainerSpec.Env {
			if value == "KARAKORAM_PROXY_SERVING_ADMISSION=1" {
				container.ProxyServingAdmission = true
				break
			}
		}
	}
	for _, attachment := range inspected.NetworksAttachments {
		network := attachment.Network
		if network.DriverState.Name != "overlay" || network.Spec.Name != b.network() && network.ID != b.network() {
			continue
		}
		for _, value := range attachment.Addresses {
			prefix, err := netip.ParsePrefix(value)
			if err != nil {
				return restoreConflict("startup task overlay address is invalid")
			}
			address := prefix.Addr().Unmap()
			if address.IsUnspecified() || address.IsMulticast() {
				return restoreConflict("startup task overlay address is not unicast")
			}
			target := &container.IPv6
			if address.Is4() {
				target = &container.IPv4
			}
			if *target != "" && *target != address.String() {
				return restoreConflict("startup task overlay address is ambiguous")
			}
			*target = address.String()
		}
	}
	if container.ProxyServingAdmission && container.IPv4 == "" && container.IPv6 == "" {
		return restoreConflict("startup task has no address on the owned overlay network")
	}
	return nil
}
