// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package infra

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
	// EnvChangeRestarts: writing variables restarts or redeploys the runtime.
	EnvChangeRestarts bool `json:"envChangeRestarts"`
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
