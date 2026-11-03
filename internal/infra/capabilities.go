// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

import (
	"context"
	"errors"
)

// ErrPublicIPTypeUnsupported marks a definitive platform refusal of an IP family.
var ErrPublicIPTypeUnsupported = errors.New("public IP type unsupported")

// PublicIPSupport is optional: only backends with authoritative, fresh family
// information implement it. An error means unknown, never unsupported.
type PublicIPSupport interface {
	SupportsPublicIP(context.Context, string) (bool, error)
}

// RuntimeResourceDeployment is an opt-in lifecycle guarantee: SetResources on
// a running runtime reapplies its stored deployment, including stop/start when
// required. A successful receipt already covers that deployment; consumers must
// gate the resource mutation as a disruption, but need not deploy it again.
// Native configuration acknowledgement and readiness remain engine-owned.
type RuntimeResourceDeployment interface {
	ResourceUpdatesDeployRuntime() bool
}

// ServingResourceUpdate optionally narrows ResourceUpdatePreservesServing for a
// particular request. Backends with disruptive resource transitions must implement
// this check; engines must require both the capability and this request's approval.
type ServingResourceUpdate interface {
	ResourceUpdatePreservesServingFor(current, target Resources) bool
}

// Capabilities declares the optional features a backend provides, so engines
// can refuse unsupported requests with a clear error and the UI can show what
// a namespace's backend supports. Every backend provides the core contract
// (services, volumes, deploy, lifecycle, resources, operations).
type Capabilities struct {
	// PublicTCP: EnsurePublicPort exposes a runtime port on a public address.
	PublicTCP bool `json:"publicTcp"`
	// TCPAllowlist: public TCP routes enforce the CIDR allowlist.
	TCPAllowlist bool `json:"tcpAllowlist"`
	// PublicHTTPS: the backend implements SubdomainAccess for HTTP ports.
	PublicHTTPS bool `json:"publicHttps"`
	// DiskGrowth: SetResources grows a volume's disk in place.
	DiskGrowth bool `json:"diskGrowth"`
	// VolumeQuota: a volume's size is enforced by the backend.
	VolumeQuota bool `json:"volumeQuota"`
	// Placement: the backend can keep members on distinct hosts and reports
	// an authoritative Container.PlacementGroup.
	Placement bool `json:"placement"`
	// Exec: the backend can run commands inside a runtime container.
	Exec bool `json:"exec"`
	// Logs: Logs returns platform runtime logs.
	Logs bool `json:"logs"`
	// Metrics: the backend exposes platform resource metrics.
	Metrics bool `json:"metrics"`
	// ScopedTokens: credentials can be restricted to one target.
	ScopedTokens bool `json:"scopedTokens"`
	// DedicatedCPU: Resources.CPUMode=DEDICATED is honoured.
	DedicatedCPU bool `json:"dedicatedCpu"`
	// HorizontalScaling: SetContainerCount runs more than one container of a
	// stateless service.
	HorizontalScaling bool `json:"horizontalScaling"`
	// ReplicaScalePreservesServing: a count-only stateless update keeps the service
	// serving. Incumbents remain serving, or replacements start first, one at a
	// time, and pass readiness equivalent to the engine's proxy readiness before
	// receiving traffic or retiring their predecessors. Completion requires the
	// requested number of running instances. Engine readiness remains mandatory.
	// Internal execution contract, not a user-selectable feature capability.
	ReplicaScalePreservesServing bool `json:"-"`
	// ResourceUpdatePreservesServing: SetResources on a running stateless service
	// preserves serving under the same incumbent-retention or health-gated,
	// start-first rules as ReplicaScalePreservesServing. ServingResourceUpdate
	// may exclude disruptive requests. Engine readiness remains mandatory.
	ResourceUpdatePreservesServing bool `json:"-"`
	// EnvChangeRestarts: writing variables restarts or redeploys the runtime.
	EnvChangeRestarts bool `json:"envChangeRestarts"`
	// StatelessRolloutSurge bounds extra serving instances during a stateless
	// rollout above max(old replicas, desired replicas). Zero means unknown:
	// consumers must reserve both complete old and desired sets.
	StatelessRolloutSurge int `json:"statelessRolloutSurge,omitempty"`
}

// flags pairs each flag with its stable API name, in display order.
func (c Capabilities) flags() []struct {
	name string
	on   bool
} {
	return []struct {
		name string
		on   bool
	}{
		{"publicTcp", c.PublicTCP}, {"tcpAllowlist", c.TCPAllowlist}, {"publicHttps", c.PublicHTTPS},
		{"diskGrowth", c.DiskGrowth}, {"volumeQuota", c.VolumeQuota}, {"placement", c.Placement},
		{"exec", c.Exec}, {"logs", c.Logs}, {"metrics", c.Metrics}, {"scopedTokens", c.ScopedTokens},
		{"dedicatedCpu", c.DedicatedCPU}, {"horizontalScaling", c.HorizontalScaling},
		{"envChangeRestarts", c.EnvChangeRestarts},
	}
}

// List returns the names of the enabled capabilities in a stable order.
func (c Capabilities) List() []string {
	out := []string{}
	for _, f := range c.flags() {
		if f.on {
			out = append(out, f.name)
		}
	}
	return out
}

// CapabilityNames returns every known capability name in display order.
func CapabilityNames() []string {
	out := []string{}
	for _, f := range (Capabilities{}).flags() {
		out = append(out, f.name)
	}
	return out
}
