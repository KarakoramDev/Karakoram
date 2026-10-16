// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"fmt"
	"math"
	"reflect"
	"sort"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// ComponentDef declares an optional per-cluster service stored in Components.Extra.
// A zero replica count disables it. Stateful components participate in defaults,
// validation and planning, but their volume lifecycle remains engine-owned.
// Definitions are ordered: ComponentSet preserves declaration order in plans,
// enabled names and reconciliation, so dependencies should be declared first.
// PublicHTTPS describes eligibility, not whether public access is requested.
// ValidateResources optionally adds engine-specific constraints to the common
// finite, ordered CPU/RAM/disk bounds; it runs before the common checks.
// A future engine can declare auth, rest, realtime, storage and studio here
// without introducing new domain fields or backend-specific lifecycle code.
type ComponentDef struct {
	Name              string
	DisplayName       string
	Stateful          bool
	Defaults          domain.ComponentSpec
	MinReplicas       int      // Lower bound when enabled; zero means one.
	MaxReplicas       int      // Upper bound when enabled; must be positive.
	Requires          []string // Names in this set that must also be enabled.
	PublicHTTPS       bool
	ValidateResources func(domain.Resources) error
}

// ComponentSet is an engine's ordered catalog of optional components. Treat its
// definitions as immutable after construction. Engine/configServer/router are
// not managed here; only Extra entries are defaulted and validated.
type ComponentSet []ComponentDef

// ApplyDefaults fills missing resource fields and applies replica defaults only
// to absent entries. A present entry with zero replicas explicitly disables the
// component. Unknown entries are retained for Validate to report.
func (s ComponentSet) ApplyDefaults(c *domain.Components) {
	if c.Extra == nil {
		c.Extra = make(map[string]domain.ComponentSpec, len(s))
	}
	for _, d := range s {
		v, present := c.Extra[d.Name]
		if !present {
			v.Replicas = d.Defaults.Replicas
		}
		r, defaults := &v.Resources, d.Defaults.Resources
		if r.CPUMode == "" {
			r.CPUMode = defaults.CPUMode
		}
		if r.MinCPU == 0 {
			r.MinCPU = defaults.MinCPU
		}
		if r.MaxCPU == 0 {
			r.MaxCPU = max(r.MinCPU, defaults.MaxCPU)
		}
		if r.MinRAM == 0 {
			r.MinRAM = defaults.MinRAM
		}
		if r.MaxRAM == 0 {
			r.MaxRAM = max(r.MinRAM, defaults.MaxRAM)
		}
		if r.MinDisk == 0 {
			r.MinDisk = defaults.MinDisk
		}
		if r.MaxDisk == 0 {
			r.MaxDisk = max(r.MinDisk, defaults.MaxDisk)
		}
		if r.StartCPU == nil && defaults.StartCPU != nil {
			v := *defaults.StartCPU
			r.StartCPU = &v
		}
		c.Extra[d.Name] = v
	}
}

// Validate rejects unknown entries, invalid enabled replica counts, missing
// dependencies and invalid resources (including explicitly disabled entries).
// Call ApplyDefaults first when accepting a partial user specification.
func (s ComponentSet) Validate(c domain.Components) error {
	known := make(map[string]bool, len(s))
	for _, d := range s {
		known[d.Name] = true
	}
	names := make([]string, 0, len(c.Extra))
	for name := range c.Extra {
		names = append(names, name)
	}
	sort.Strings(names)
	for _, name := range names {
		if !known[name] {
			return Invalid("unknown component %q", name)
		}
	}
	for _, d := range s {
		v, present := c.Extra[d.Name]
		if !present {
			continue
		}
		minimum := max(1, d.MinReplicas)
		if v.Replicas < 0 || v.Replicas > 0 && (v.Replicas < minimum || v.Replicas > d.MaxReplicas) {
			if minimum == 1 {
				return Invalid("%s replicas must be 0..%d", d.Name, d.MaxReplicas)
			}
			return Invalid("%s replicas must be 0 or %d..%d", d.Name, minimum, d.MaxReplicas)
		}
		if v.Replicas > 0 {
			for _, required := range d.Requires {
				if !known[required] || c.Extra[required].Replicas <= 0 {
					return Invalid("%s requires component %q", d.Name, required)
				}
			}
		}
		if d.ValidateResources != nil {
			if err := d.ValidateResources(v.Resources); err != nil {
				return Invalid("%v", err)
			}
		}
		if err := validateComponentResources(d.Name, v.Resources); err != nil {
			return err
		}
	}
	return nil
}

func validateComponentResources(name string, r domain.Resources) error {
	for _, v := range []float64{r.MinCPU, r.MaxCPU, r.MinRAM, r.MaxRAM, r.MinDisk, r.MaxDisk} {
		if math.IsNaN(v) || math.IsInf(v, 0) {
			return Invalid("resource bounds must be finite")
		}
	}
	if r.CPUMode != "SHARED" && r.CPUMode != "DEDICATED" || r.MinCPU < 1 || r.MaxCPU < r.MinCPU || r.MinCPU != math.Trunc(r.MinCPU) || r.MaxCPU != math.Trunc(r.MaxCPU) || r.MinRAM <= 0 || r.MaxRAM < r.MinRAM || r.MinDisk < 1 || r.MaxDisk < r.MinDisk {
		return Invalid("invalid %s resource bounds", name)
	}
	if r.StartCPU != nil && (math.IsNaN(*r.StartCPU) || math.IsInf(*r.StartCPU, 0) || *r.StartCPU != math.Trunc(*r.StartCPU) || *r.StartCPU < r.MinCPU || *r.StartCPU > r.MaxCPU) {
		return Invalid("startCpu outside CPU bounds")
	}
	return nil
}

// Enabled returns enabled Extra component names in declaration order.
func (s ComponentSet) Enabled(c domain.Components) []string {
	var out []string
	for _, d := range s {
		if c.Extra[d.Name].Replicas > 0 {
			out = append(out, d.Name)
		}
	}
	return out
}

// Info returns independent entries suitable for TopologyInfo.Components. Zero
// remains the catalog minimum because optional components can be disabled.
func (s ComponentSet) Info() map[string]domain.ComponentInfo {
	out := make(map[string]domain.ComponentInfo, len(s))
	for _, d := range s {
		defaults := d.Defaults
		if defaults.Resources.StartCPU != nil {
			v := *defaults.Resources.StartCPU
			defaults.Resources.StartCPU = &v
		}
		info := domain.ComponentInfo{Defaults: defaults, MinReplicas: 0, MaxReplicas: d.MaxReplicas}
		if d.MinReplicas > 1 {
			info.AllowedReplicas = make([]int, 1, 1+d.MaxReplicas-d.MinReplicas+1)
			for replicas := d.MinReplicas; replicas <= d.MaxReplicas; replicas++ {
				info.AllowedReplicas = append(info.AllowedReplicas, replicas)
			}
		}
		out[d.Name] = info
	}
	return out
}

// ComponentChangeKind identifies an infrastructure transition. Enable and Disable
// subsume scaling/resizing; an existing enabled component may need both Scale
// and Resize, emitted in that order.
type ComponentChangeKind string

const (
	ComponentEnable  ComponentChangeKind = "enable"
	ComponentDisable ComponentChangeKind = "disable"
	ComponentScale   ComponentChangeKind = "scale"
	ComponentResize  ComponentChangeKind = "resize"
)

// ComponentChange describes desired intent, not an operation handle. Engines
// retain their own durable stages and decide when stateful changes are safe.
type ComponentChange struct {
	Name   string
	Kind   ComponentChangeKind
	Before domain.ComponentSpec
	After  domain.ComponentSpec
}

// Plan compares observed replicas AND resources with a defaulted desired spec.
// The observed map uses ComponentSpec rather than just replica counts so resize
// decisions are based on actual applied resources, not guessed from defaults.
// Missing observations mean absent; missing desired entries mean disabled.
func (s ComponentSet) Plan(observed map[string]domain.ComponentSpec, desired domain.Components) []ComponentChange {
	var out []ComponentChange
	for _, d := range s {
		before, after := observed[d.Name], desired.Extra[d.Name]
		add := func(kind ComponentChangeKind) {
			out = append(out, ComponentChange{Name: d.Name, Kind: kind, Before: before, After: after})
		}
		switch {
		case before.Replicas <= 0 && after.Replicas > 0:
			add(ComponentEnable)
		case before.Replicas > 0 && after.Replicas <= 0:
			add(ComponentDisable)
		case before.Replicas > 0 && after.Replicas > 0:
			if before.Replicas != after.Replicas {
				add(ComponentScale)
			}
			if !reflect.DeepEqual(before.Resources, after.Resources) {
				add(ComponentResize)
			}
		}
	}
	return out
}

// ComponentStage describes the engine's durable provisioning observation.
// Joined means application readiness passed, not merely that deployment ended.
type ComponentStage string

const (
	ComponentPlanned  ComponentStage = "planned"
	ComponentImported ComponentStage = "imported"
	ComponentDeployed ComponentStage = "deployed"
	ComponentJoined   ComponentStage = "joined"
)

// ComponentRuntime is a stateless service's currently observed lifecycle state.
// Replicas and Resources are the applied values, not desired values.
// HorizontalScaling reports whether the backend permits multiple containers.
type ComponentRuntime struct {
	Exists            bool
	Stage             ComponentStage
	Replicas          int
	Resources         domain.Resources
	HorizontalScaling bool
}

// ComponentCallbacks bind lifecycle actions to an engine's existing operation
// tracking. Import may allocate its service identity first. Deploy supplies the
// component-specific RuntimeSpec. AwaitReady must persist Joined once ready;
// its pass always ends, including while waiting. Resize is optional for engines
// that reconcile resources separately. Callbacks persist all resulting stages,
// applied values and operation handles themselves. Each call performs at most
// one callback, allowing asynchronous backends and restarts.
type ComponentCallbacks struct {
	Import     func(context.Context) error
	Deploy     func(context.Context) error
	AwaitReady func(context.Context) error
	Scale      func(context.Context, int) error
	Resize     func(context.Context, domain.Resources) error
	Delete     func(context.Context) error
}

// ReconcileStatelessComponent drives enable (import, deploy, readiness, scale,
// optional resize) and disable (delete). It returns handled while a transition
// or readiness wait owns this reconciliation pass. Stateful definitions require
// engine-specific lifecycle logic and are rejected rather than losing volumes.
func ReconcileStatelessComponent(ctx context.Context, def ComponentDef, current ComponentRuntime, desired domain.ComponentSpec, cb ComponentCallbacks) (bool, error) {
	if def.Stateful {
		return false, fmt.Errorf("%w: component %s requires a stateful lifecycle", ErrNotSupported, def.Name)
	}
	invoke := func(action string, fn func(context.Context) error) (bool, error) {
		if fn == nil {
			return true, fmt.Errorf("component %s requires a %s callback", def.Name, action)
		}
		return true, fn(ctx)
	}
	if desired.Replicas <= 0 {
		if current.Exists {
			return invoke("delete", cb.Delete)
		}
		return false, nil
	}
	if desired.Replicas > 1 && !current.HorizontalScaling {
		return true, fmt.Errorf("%w: component %s requires horizontal scaling", ErrNotSupported, def.Name)
	}
	if !current.Exists || current.Stage == ComponentPlanned {
		return invoke("import", cb.Import)
	}
	switch current.Stage {
	case ComponentImported:
		return invoke("deploy", cb.Deploy)
	case ComponentDeployed:
		return invoke("readiness", cb.AwaitReady)
	case ComponentJoined:
		if current.Replicas != desired.Replicas {
			if cb.Scale == nil {
				return true, fmt.Errorf("component %s requires a scale callback", def.Name)
			}
			return true, cb.Scale(ctx, desired.Replicas)
		}
		if cb.Resize != nil && !reflect.DeepEqual(current.Resources, desired.Resources) {
			return true, cb.Resize(ctx, desired.Resources)
		}
		return false, nil
	default:
		return true, fmt.Errorf("component %s has unknown lifecycle stage %q", def.Name, current.Stage)
	}
}
