// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// VolumeReference identifies a persistent volume, never a runtime disk.
type VolumeReference struct {
	Component string
	Hostname  string
	ID        string
}

// VolumeInventory exposes identities, not provider sizing caches: legacy import
// checkpoints may contain desired bounds that were never applied to a volume.
type VolumeInventory interface {
	VolumeReferences(*domain.Cluster) ([]VolumeReference, error)
}

// ObserveVolumeSize reads the backend's volume record and fences identity reuse.
// A missing or still-importing record has no acknowledged sizing observation.
func ObserveVolumeSize(ctx context.Context, b infra.Backend, hostname, id string) (*domain.Resources, error) {
	v, err := b.GetService(ctx, hostname)
	if err != nil {
		return nil, err
	}
	if v == nil || v.Importing {
		return nil, nil
	}
	if v.Kind != infra.KindVolume || v.ID != id {
		return nil, fmt.Errorf("%w: volume %s identity changed", infra.ErrConflict, hostname)
	}
	if v.Resources.MinDisk <= 0 || v.Resources.MaxDisk <= 0 {
		return nil, nil
	}
	return &v.Resources, nil
}

// PreflightVolumeGrowth checks actual backend sizing before disruption. On an
// incapable backend, refresh stale legacy disk caches only after proving that
// the requested bounds do not require growth. Other runtime fields are retained.
func PreflightVolumeGrowth(ctx context.Context, b infra.Backend, hostname, id string, current, target domain.Resources) (domain.Resources, error) {
	if b.Capabilities().DiskGrowth {
		return current, nil
	}
	actual, err := ObserveVolumeSize(ctx, b, hostname, id)
	if err != nil {
		return current, err
	}
	if actual != nil {
		current.MinDisk, current.MaxDisk = actual.MinDisk, actual.MaxDisk
	}
	if target.MinDisk > current.MinDisk || target.MaxDisk > current.MaxDisk {
		return current, RequireDiskGrowth(b)
	}
	return current, nil
}

func volumeComponent(c *domain.Components, name string) domain.ComponentSpec {
	switch name {
	case "engine":
		return c.Engine
	case "configServer":
		return c.ConfigServer
	case "router":
		return c.Router
	default:
		return c.Extra[name]
	}
}

func setVolumeComponent(c *domain.Components, name string, value domain.ComponentSpec) {
	switch name {
	case "engine":
		c.Engine = value
	case "configServer":
		c.ConfigServer = value
	case "router":
		c.Router = value
	default:
		if c.Extra == nil {
			c.Extra = make(map[string]domain.ComponentSpec)
		}
		c.Extra[name] = value
	}
}

// VolumeAdmissionBaseline checks growth against EACH existing volume, then uses
// the largest acknowledged bounds as the shrink floor. Mixed-size volumes must
// not turn an unsupported resize of their smaller peer into an accepted no-op.
func VolumeAdmissionBaseline(ctx context.Context, p Provider, c *domain.Cluster, target domain.ClusterSpec, b infra.Backend) (*domain.Cluster, error) {
	inventory, ok := p.(VolumeInventory)
	if !ok || b == nil {
		return c, nil
	}
	refs, err := inventory.VolumeReferences(c)
	if err != nil {
		return nil, err
	}
	var sizes domain.Components
	for _, ref := range refs {
		r, err := ObserveVolumeSize(ctx, b, ref.Hostname, ref.ID)
		if err != nil {
			return nil, err
		}
		if r == nil {
			continue
		}
		desired := volumeComponent(&target.Components, ref.Component)
		if desired.Replicas > 0 && (desired.Resources.MinDisk > r.MinDisk || desired.Resources.MaxDisk > r.MaxDisk) {
			if err := RequireDiskGrowth(b); err != nil {
				return nil, err
			}
		}
		actual := volumeComponent(&sizes, ref.Component)
		actual.Replicas++
		actual.Resources.MinDisk = max(actual.Resources.MinDisk, r.MinDisk)
		actual.Resources.MaxDisk = max(actual.Resources.MaxDisk, r.MaxDisk)
		setVolumeComponent(&sizes, ref.Component, actual)
	}
	previous := *c
	if len(sizes.Extra) > 0 {
		previous.Spec.Components.Extra = make(map[string]domain.ComponentSpec, len(c.Spec.Components.Extra))
		for name, component := range c.Spec.Components.Extra {
			previous.Spec.Components.Extra[name] = component
		}
	}
	apply := func(name string) {
		actual := volumeComponent(&sizes, name)
		if actual.Replicas == 0 {
			return
		}
		component := volumeComponent(&previous.Spec.Components, name)
		component.Replicas = max(component.Replicas, 1)
		component.Resources.MinDisk, component.Resources.MaxDisk = actual.Resources.MinDisk, actual.Resources.MaxDisk
		setVolumeComponent(&previous.Spec.Components, name, component)
	}
	apply("engine")
	apply("configServer")
	apply("router")
	for name := range sizes.Extra {
		apply(name)
	}
	return &previous, nil
}
