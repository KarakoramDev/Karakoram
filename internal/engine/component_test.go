// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package engine

import (
	"context"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func componentCatalog() ComponentSet {
	r := domain.Resources{CPUMode: "SHARED", MinCPU: 1, MaxCPU: 2, MinRAM: 0.5, MaxRAM: 1, MinDisk: 1, MaxDisk: 5}
	return ComponentSet{
		{Name: "auth", DisplayName: "Auth", Defaults: domain.ComponentSpec{Resources: r}, MinReplicas: 1, MaxReplicas: 3},
		{Name: "rest", DisplayName: "REST", Defaults: domain.ComponentSpec{Resources: r}, MinReplicas: 2, MaxReplicas: 5, Requires: []string{"auth"}, PublicHTTPS: true},
	}
}

func TestComponentDefaultsAndInfo(t *testing.T) {
	set := componentCatalog()
	c := domain.Components{Extra: map[string]domain.ComponentSpec{"auth": {Replicas: 2, Resources: domain.Resources{MinCPU: 3, MinRAM: 2, MinDisk: 8}}}}
	set.ApplyDefaults(&c)
	r := c.Extra["auth"].Resources
	if c.Extra["auth"].Replicas != 2 || c.Extra["rest"].Replicas != 0 || r.MaxCPU != 3 || r.MaxRAM != 2 || r.MaxDisk != 8 {
		t.Fatal(c)
	}
	if !reflect.DeepEqual(set.Enabled(c), []string{"auth"}) {
		t.Fatal(set.Enabled(c))
	}
	before := c.Extra["auth"]
	set.ApplyDefaults(&c)
	if !reflect.DeepEqual(before, c.Extra["auth"]) {
		t.Fatal("defaults not idempotent")
	}
	info := set.Info()
	if info["rest"].Defaults.Replicas != 0 || info["rest"].MinReplicas != 0 || info["rest"].MaxReplicas != 5 {
		t.Fatal(info)
	}
	if !reflect.DeepEqual(info["rest"].AllowedReplicas, []int{0, 2, 3, 4, 5}) || info["auth"].AllowedReplicas != nil {
		t.Fatal("incorrect optional replica bounds", info)
	}
	info["rest"].AllowedReplicas[1] = 99
	if set.Info()["rest"].AllowedReplicas[1] != 2 {
		t.Fatal("catalog shares mutable replica bounds")
	}
	start := 1.0
	set[0].Defaults.Resources.StartCPU = &start
	defaults := domain.Components{}
	set.ApplyDefaults(&defaults)
	*defaults.Extra["auth"].Resources.StartCPU = 2
	info = set.Info()
	*info["auth"].Defaults.Resources.StartCPU = 3
	if start != 1 {
		t.Fatal("defaults share mutable resources")
	}
	set[0].Defaults.Replicas = 1
	defaults = domain.Components{}
	set.ApplyDefaults(&defaults)
	if defaults.Extra["auth"].Replicas != 1 {
		t.Fatal("nonzero default not applied")
	}
	defaults = domain.Components{Extra: map[string]domain.ComponentSpec{"auth": {Replicas: 0}}}
	set.ApplyDefaults(&defaults)
	set.ApplyDefaults(&defaults)
	if defaults.Extra["auth"].Replicas != 0 || defaults.Extra["auth"].Resources.MinCPU != 1 {
		t.Fatal("explicit disable was replaced by replica defaults", defaults)
	}
}

func TestComponentValidation(t *testing.T) {
	for _, tt := range []struct {
		name, want string
		change     func(*domain.Components)
	}{
		{"unknown", "unknown component", func(c *domain.Components) { c.Extra["studio"] = domain.ComponentSpec{} }},
		{"negative", "auth replicas must be 0..3", func(c *domain.Components) { v := c.Extra["auth"]; v.Replicas = -1; c.Extra["auth"] = v }},
		{"tooMany", "auth replicas must be 0..3", func(c *domain.Components) { v := c.Extra["auth"]; v.Replicas = 4; c.Extra["auth"] = v }},
		{"enabledMinimum", "rest replicas must be 0 or 2..5", func(c *domain.Components) { v := c.Extra["rest"]; v.Replicas = 1; c.Extra["rest"] = v }},
		{"requires", "rest requires component", func(c *domain.Components) { v := c.Extra["rest"]; v.Replicas = 2; c.Extra["rest"] = v }},
		{"nan", "resource bounds must be finite", func(c *domain.Components) { v := c.Extra["auth"]; v.Resources.MinCPU = math.NaN(); c.Extra["auth"] = v }},
		{"infinity", "resource bounds must be finite", func(c *domain.Components) {
			v := c.Extra["auth"]
			v.Resources.MaxDisk = math.Inf(1)
			c.Extra["auth"] = v
		}},
		{"fractionalCPU", "invalid auth resource bounds", func(c *domain.Components) { v := c.Extra["auth"]; v.Resources.MaxCPU = 1.5; c.Extra["auth"] = v }},
		{"cpuMode", "invalid auth resource bounds", func(c *domain.Components) { v := c.Extra["auth"]; v.Resources.CPUMode = "other"; c.Extra["auth"] = v }},
		{"ram", "invalid auth resource bounds", func(c *domain.Components) { v := c.Extra["auth"]; v.Resources.MinRAM = 0; c.Extra["auth"] = v }},
		{"disk", "invalid auth resource bounds", func(c *domain.Components) { v := c.Extra["auth"]; v.Resources.MaxDisk = 0; c.Extra["auth"] = v }},
		{"startCPU", "startCpu outside CPU bounds", func(c *domain.Components) {
			v := c.Extra["auth"]
			n := 3.0
			v.Resources.StartCPU = &n
			c.Extra["auth"] = v
		}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			set := componentCatalog()
			c := domain.Components{}
			set.ApplyDefaults(&c)
			tt.change(&c)
			err := set.Validate(c)
			if !errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("got %v, want %s", err, tt.want)
			}
		})
	}
	set := componentCatalog()
	c := domain.Components{}
	set.ApplyDefaults(&c)
	if err := set.Validate(c); err != nil {
		t.Fatal(err)
	}
	for name, v := range c.Extra {
		v.Replicas = 2
		c.Extra[name] = v
	}
	if err := set.Validate(c); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(set.Enabled(c), []string{"auth", "rest"}) {
		t.Fatal("declaration order")
	}
	set[0].ValidateResources = func(domain.Resources) error { return errors.New("engine RAM constraint") }
	if err := set.Validate(c); err == nil || !strings.Contains(err.Error(), "engine RAM constraint") {
		t.Fatal(err)
	}
	set = componentCatalog()
	set[1].Requires = []string{"unknown"}
	if err := set.Validate(c); err == nil {
		t.Fatal("unknown dependency accepted")
	}
}

func TestComponentPlan(t *testing.T) {
	set := componentCatalog()
	desired := domain.Components{}
	set.ApplyDefaults(&desired)
	enabled := desired.Extra["auth"]
	enabled.Replicas = 2
	changed := enabled
	changed.Replicas = 3
	changed.Resources.MaxRAM = 2
	for _, tt := range []struct {
		name          string
		before, after domain.ComponentSpec
		want          []ComponentChangeKind
	}{
		{"disabled", domain.ComponentSpec{}, desired.Extra["auth"], nil},
		{"enable", domain.ComponentSpec{}, enabled, []ComponentChangeKind{ComponentEnable}},
		{"disable", enabled, domain.ComponentSpec{}, []ComponentChangeKind{ComponentDisable}},
		{"unchanged", enabled, enabled, nil},
		{"scale", enabled, func() domain.ComponentSpec { v := enabled; v.Replicas = 3; return v }(), []ComponentChangeKind{ComponentScale}},
		{"resize", enabled, func() domain.ComponentSpec { v := enabled; v.Resources.MaxRAM = 2; return v }(), []ComponentChangeKind{ComponentResize}},
		{"both", enabled, changed, []ComponentChangeKind{ComponentScale, ComponentResize}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			desired.Extra["auth"] = tt.after
			plan := set.Plan(map[string]domain.ComponentSpec{"auth": tt.before}, desired)
			var kinds []ComponentChangeKind
			for _, v := range plan {
				kinds = append(kinds, v.Kind)
				if v.Name != "auth" || !reflect.DeepEqual(v.Before, tt.before) || !reflect.DeepEqual(v.After, tt.after) {
					t.Fatal(v)
				}
			}
			if !reflect.DeepEqual(kinds, tt.want) {
				t.Fatal(kinds, tt.want)
			}
		})
	}
	for name := range desired.Extra {
		desired.Extra[name] = enabled
	}
	plan := set.Plan(nil, desired)
	if len(plan) != 2 || plan[0].Name != "auth" || plan[1].Name != "rest" {
		t.Fatal(plan)
	}
}

func TestComponentLifecycleCallbacks(t *testing.T) {
	ctx := context.Background()
	def := componentCatalog()[0]
	current := ComponentRuntime{HorizontalScaling: true}
	desired := domain.ComponentSpec{Replicas: 2}
	var calls []string
	ready := false
	cb := ComponentCallbacks{
		Import: func(context.Context) error {
			calls = append(calls, "import")
			current.Exists = true
			current.Stage = ComponentImported
			return nil
		},
		Deploy: func(context.Context) error {
			calls = append(calls, "deploy")
			current.Stage = ComponentDeployed
			current.Replicas = 1
			return nil
		},
		AwaitReady: func(context.Context) error {
			calls = append(calls, "ready")
			if ready {
				current.Stage = ComponentJoined
			}
			return nil
		},
		Scale: func(_ context.Context, n int) error { calls = append(calls, "scale"); current.Replicas = n; return nil },
		Delete: func(context.Context) error {
			calls = append(calls, "delete")
			current = ComponentRuntime{HorizontalScaling: true}
			return nil
		},
	}
	step := func(want bool) {
		t.Helper()
		handled, err := ReconcileStatelessComponent(ctx, def, current, desired, cb)
		if err != nil || handled != want {
			t.Fatalf("handled %v err %v", handled, err)
		}
	}
	step(true)
	step(true)
	step(true)
	if current.Stage != ComponentDeployed {
		t.Fatal("not ready advanced stage")
	}
	ready = true
	step(true)
	step(true)
	step(false)
	desired.Replicas = 0
	step(true)
	step(false)
	if !reflect.DeepEqual(calls, []string{"import", "deploy", "ready", "ready", "scale", "delete"}) {
		t.Fatal(calls)
	}
	desired.Replicas = 1
	current = ComponentRuntime{Exists: true, Stage: ComponentPlanned}
	step(true)
}

func TestComponentLifecycleErrors(t *testing.T) {
	ctx := context.Background()
	def := componentCatalog()[0]
	failure := errors.New("action failed")
	for _, tt := range []struct {
		stage             ComponentStage
		replicas, desired int
		callbacks         ComponentCallbacks
	}{
		{ComponentPlanned, 0, 1, ComponentCallbacks{Import: func(context.Context) error { return failure }}},
		{ComponentImported, 1, 1, ComponentCallbacks{Deploy: func(context.Context) error { return failure }}},
		{ComponentDeployed, 1, 1, ComponentCallbacks{AwaitReady: func(context.Context) error { return failure }}},
		{ComponentJoined, 1, 2, ComponentCallbacks{Scale: func(context.Context, int) error { return failure }}},
		{ComponentJoined, 1, 0, ComponentCallbacks{Delete: func(context.Context) error { return failure }}},
	} {
		current := ComponentRuntime{Exists: true, Stage: tt.stage, Replicas: tt.replicas, HorizontalScaling: true}
		if handled, err := ReconcileStatelessComponent(ctx, def, current, domain.ComponentSpec{Replicas: tt.desired}, tt.callbacks); !handled || !errors.Is(err, failure) {
			t.Fatal(handled, err)
		}
		if _, err := ReconcileStatelessComponent(ctx, def, current, domain.ComponentSpec{Replicas: tt.desired}, ComponentCallbacks{}); err == nil {
			t.Fatal("missing callback accepted")
		}
	}
	def.Stateful = true
	if _, err := ReconcileStatelessComponent(ctx, def, ComponentRuntime{}, domain.ComponentSpec{}, ComponentCallbacks{}); !errors.Is(err, ErrNotSupported) {
		t.Fatal(err)
	}
	def.Stateful = false
	if _, err := ReconcileStatelessComponent(ctx, def, ComponentRuntime{Exists: true, Stage: "unknown"}, domain.ComponentSpec{Replicas: 1}, ComponentCallbacks{}); err == nil {
		t.Fatal("unknown stage accepted")
	}
}

func TestComponentLifecycleHorizontalScaling(t *testing.T) {
	for _, current := range []ComponentRuntime{
		{},
		{Exists: true, Stage: ComponentPlanned},
		{Exists: true, Stage: ComponentJoined, Replicas: 1},
	} {
		called := false
		cb := ComponentCallbacks{
			Import: func(context.Context) error { called = true; return nil },
			Scale:  func(context.Context, int) error { called = true; return nil },
		}
		handled, err := ReconcileStatelessComponent(t.Context(), componentCatalog()[0], current, domain.ComponentSpec{Replicas: 2}, cb)
		if !handled || !errors.Is(err, ErrNotSupported) || called {
			t.Fatalf("unsupported scaling reached callback: handled=%v err=%v called=%v", handled, err, called)
		}
	}
}

func TestComponentLifecycleResize(t *testing.T) {
	def := componentCatalog()[0]
	desired := def.Defaults
	desired.Replicas = 2
	current := ComponentRuntime{Exists: true, Stage: ComponentJoined, Replicas: 1, HorizontalScaling: true}
	var calls []string
	cb := ComponentCallbacks{
		Scale: func(_ context.Context, replicas int) error {
			calls = append(calls, "scale")
			current.Replicas = replicas
			return nil
		},
		Resize: func(_ context.Context, resources domain.Resources) error {
			calls = append(calls, "resize")
			if !reflect.DeepEqual(resources, desired.Resources) {
				t.Fatal(resources)
			}
			current.Resources = resources
			return nil
		},
	}
	for _, want := range []bool{true, true, false} {
		handled, err := ReconcileStatelessComponent(t.Context(), def, current, desired, cb)
		if handled != want || err != nil {
			t.Fatalf("handled=%v err=%v", handled, err)
		}
	}
	if !reflect.DeepEqual(calls, []string{"scale", "resize"}) {
		t.Fatal(calls)
	}
	failure := errors.New("resize failed")
	cb.Resize = func(context.Context, domain.Resources) error { return failure }
	current.Resources = domain.Resources{}
	if handled, err := ReconcileStatelessComponent(t.Context(), def, current, desired, cb); !handled || !errors.Is(err, failure) {
		t.Fatalf("resize failure: handled=%v err=%v", handled, err)
	}
	cb.Resize = nil
	if handled, err := ReconcileStatelessComponent(t.Context(), def, current, desired, cb); handled || err != nil {
		t.Fatalf("engine-owned resizing: handled=%v err=%v", handled, err)
	}
	current.Stage = ComponentDeployed
	cb.AwaitReady = func(context.Context) error { return nil }
	cb.Resize = func(context.Context, domain.Resources) error { t.Fatal("resize before readiness"); return nil }
	if handled, err := ReconcileStatelessComponent(t.Context(), def, current, desired, cb); !handled || err != nil {
		t.Fatalf("readiness pass: handled=%v err=%v", handled, err)
	}
}
