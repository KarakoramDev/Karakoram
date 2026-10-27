// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import "github.com/KarakoramDev/Karakoram/internal/domain"

// VolumeSizer exposes acknowledged persistent-volume bounds for admission.
// Runtime disks must not be included. Zero replicas means no known volume.
// Providers aggregate the largest acknowledged bounds for each component.
type VolumeSizer interface {
	VolumeSizes(*domain.Cluster) (domain.Components, error)
}

// AddVolumeSize accumulates acknowledged bounds, including retained volumes.
func AddVolumeSize(c *domain.Components, name string, r domain.Resources) {
	var component *domain.ComponentSpec
	switch name {
	case "engine":
		component = &c.Engine
	case "configServer":
		component = &c.ConfigServer
	case "router":
		component = &c.Router
	default:
		if c.Extra == nil {
			c.Extra = make(map[string]domain.ComponentSpec)
		}
		v := c.Extra[name]
		v.Replicas++
		v.Resources.MinDisk = max(v.Resources.MinDisk, r.MinDisk)
		v.Resources.MaxDisk = max(v.Resources.MaxDisk, r.MaxDisk)
		c.Extra[name] = v
		return
	}
	component.Replicas++
	component.Resources.MinDisk = max(component.Resources.MinDisk, r.MinDisk)
	component.Resources.MaxDisk = max(component.Resources.MaxDisk, r.MaxDisk)
}

// VolumeAdmissionBaseline substitutes proven volume bounds for stale desired
// bounds on backends without growth. This permits reverting an unapplied size
// increase without authorizing a real shrink. Unknown sizes keep the old fence.
func VolumeAdmissionBaseline(p Provider, c *domain.Cluster) (*domain.Cluster, error) {
	sizer, ok := p.(VolumeSizer)
	if !ok {
		return c, nil
	}
	sizes, err := sizer.VolumeSizes(c)
	if err != nil {
		return nil, err
	}
	previous := *c
	baseline := func(component *domain.ComponentSpec, actual domain.ComponentSpec) {
		if actual.Replicas > 0 {
			component.Replicas = max(component.Replicas, 1)
			component.Resources.MinDisk = actual.Resources.MinDisk
			component.Resources.MaxDisk = actual.Resources.MaxDisk
		}
	}
	baseline(&previous.Spec.Components.Engine, sizes.Engine)
	baseline(&previous.Spec.Components.ConfigServer, sizes.ConfigServer)
	baseline(&previous.Spec.Components.Router, sizes.Router)
	if len(sizes.Extra) > 0 {
		previous.Spec.Components.Extra = make(map[string]domain.ComponentSpec, len(c.Spec.Components.Extra))
		for name, component := range c.Spec.Components.Extra {
			previous.Spec.Components.Extra[name] = component
		}
		for name, actual := range sizes.Extra {
			component := previous.Spec.Components.Extra[name]
			baseline(&component, actual)
			previous.Spec.Components.Extra[name] = component
		}
	}
	return &previous, nil
}
