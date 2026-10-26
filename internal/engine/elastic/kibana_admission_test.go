// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func kibanaAdmissionCluster(p *Provider, version string, replicas int, minimum, maximum float64) *domain.Cluster {
	c := &domain.Cluster{Name: "search"}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Version = version
	k := c.Spec.Components.Extra["kibana"]
	k.Replicas, k.Resources.MinRAM, k.Resources.MaxRAM = replicas, minimum, maximum
	c.Spec.Components.Extra["kibana"] = k
	return c
}

func TestKibanaVersionAdmission(t *testing.T) {
	p := New(ProviderOptions{})
	for _, version := range catalog {
		for _, tc := range []struct {
			name             string
			replicas         int
			minimum, maximum float64
			wantError        bool
		}{
			{"one", 1, 1, 1, version.Version != "8.19.23"},
			{"floor", 1, 1.5, 1.5, false},
			{"inverted", 1, 1.5, 1, true},
			{"disabled", 0, 1, 1, false},
			{"nan", 1, math.NaN(), 2, true},
			{"infinity", 1, math.Inf(1), math.Inf(1), true},
		} {
			t.Run(version.Version+"/"+tc.name, func(t *testing.T) {
				c := kibanaAdmissionCluster(p, version.Version, tc.replicas, tc.minimum, tc.maximum)
				err := p.Validate(context.Background(), nil, c)
				if (err != nil) != tc.wantError {
					t.Fatalf("Validate = %v, want error %v", err, tc.wantError)
				}
				if tc.name == "one" && tc.wantError {
					var field *engine.ValidationError
					if !errors.As(err, &field) || field.Field != "spec.components.extra.kibana.resources.minRam" {
						t.Fatalf("missing minimum RAM field error: %v", err)
					}
					for _, text := range []string{version.Version, "1.5", "minRam", "maxRam"} {
						if !strings.Contains(err.Error(), text) {
							t.Errorf("error %q omits %q", err, text)
						}
					}
				}
			})
		}
	}
}

func TestKibanaLegacyAdmission(t *testing.T) {
	p := New(ProviderOptions{})
	for _, tc := range []struct {
		name                  string
		oldReplicas, replicas int
		minimum, maximum      float64
		wantError             bool
	}{
		{"unchanged", 2, 2, 1, 1, false},
		{"decrease", 2, 1, 1, 1, false},
		{"disable", 2, 0, 1, 1, false},
		{"increase", 1, 2, 1, 1, true},
		{"enable", 0, 1, 1, 1, true},
		{"partial correction", 1, 1, 1.25, 1.25, true},
		{"maximum only", 1, 1, 1, 2, true},
		{"floor correction", 1, 1, 1.5, 1.5, false},
		{"larger correction", 1, 1, 2, 2, false},
		{"minimum only", 1, 1, 1.5, 1, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := kibanaAdmissionCluster(p, "9.5.5", tc.oldReplicas, 1, 1)
			n := cloneCluster(t, old)
			k := n.Spec.Components.Extra["kibana"]
			k.Replicas, k.Resources.MinRAM, k.Resources.MaxRAM = tc.replicas, tc.minimum, tc.maximum
			n.Spec.Components.Extra["kibana"] = k
			n.Spec.Monitoring.Enabled = false // Unrelated updates must retain compatibility.
			p.ApplyDefaults(&n.Spec)
			err := p.Validate(context.Background(), old, n)
			if (err != nil) != tc.wantError {
				t.Fatalf("Validate = %v, want error %v", err, tc.wantError)
			}
			if tc.name == "minimum only" && (err == nil || !strings.Contains(err.Error(), "maxRam must be at least minRam")) {
				t.Fatalf("missing bounds guidance: %v", err)
			}
			if n.Spec.Components.Extra["kibana"].Resources.MinRAM != tc.minimum {
				t.Fatal("defaults changed explicit RAM")
			}
		})
	}
}

func TestKibanaUpgradeResizeFirst(t *testing.T) {
	p := New(ProviderOptions{})
	for _, from := range []string{"8.19.23", "9.4.8", "9.5.4"} {
		for _, targetRAM := range []float64{1, 1.5, 2} {
			old := kibanaAdmissionCluster(p, from, 1, 1, 1)
			n := kibanaAdmissionCluster(p, "9.5.5", 1, targetRAM, targetRAM)
			err := p.Validate(context.Background(), old, n)
			if err == nil || !strings.Contains(err.Error(), "resize Kibana first") {
				t.Errorf("%s -> 9.5.5 RAM %g: expected resize-first error, got %v", from, targetRAM, err)
			}
		}
		old := kibanaAdmissionCluster(p, from, 0, 1, 1)
		n := kibanaAdmissionCluster(p, "9.5.5", 0, 1, 1)
		if err := p.Validate(context.Background(), old, n); err != nil {
			t.Fatalf("disabled upgrade: %v", err)
		}
	}
	old := kibanaAdmissionCluster(p, "9.5.4", 1, 1, 1)
	resized := kibanaAdmissionCluster(p, "9.5.4", 1, 1.5, 1.5)
	if err := p.Validate(context.Background(), old, resized); err != nil {
		t.Fatal(err)
	}
	resized.Generation = 2
	resized.Status.ObservedGeneration = resized.Generation
	resized.Status.Phase = domain.PhaseReady
	upgraded := kibanaAdmissionCluster(p, "9.5.5", 1, 1.5, 1.5)
	if err := p.Validate(context.Background(), resized, upgraded); err != nil {
		t.Fatal(err)
	}
}

func TestKibanaUpgradeWaitsForConvergence(t *testing.T) {
	p := New(ProviderOptions{})
	for _, from := range []string{"8.19.23", "9.4.8", "9.5.4"} {
		for _, tc := range []struct {
			name      string
			observed  int64
			phase     domain.Phase
			wantError bool
		}{
			{"pending resize", 1, domain.PhaseReady, true},
			{"unobserved", 0, domain.PhaseReady, true},
			{"not ready", 2, domain.PhaseUpdating, true},
			{"degraded", 2, domain.PhaseDegraded, true},
			{"converged", 2, domain.PhaseReady, false},
		} {
			t.Run(from+"/"+tc.name, func(t *testing.T) {
				old := kibanaAdmissionCluster(p, from, 1, 1.5, 1.5)
				old.Generation = 2
				old.Status.ObservedGeneration, old.Status.Phase = tc.observed, tc.phase
				n := cloneCluster(t, old)
				n.Spec.Version = "9.5.5"
				err := p.Validate(context.Background(), old, n)
				if (err != nil) != tc.wantError {
					t.Fatalf("Validate = %v, want error %v", err, tc.wantError)
				}
				if tc.wantError {
					var field *engine.ValidationError
					if !errors.As(err, &field) || field.Field != "spec.version" || field.Reason != domain.ReasonConflict {
						t.Fatalf("missing version conflict field error: %v", err)
					}
					for _, text := range []string{"pending change", "Kibana resize", "ready"} {
						if !strings.Contains(err.Error(), text) {
							t.Errorf("error omits %q: %v", text, err)
						}
					}
				}
			})
		}
	}
	// The convergence gate does not apply to disabled Kibana, same-version
	// changes, creates, or targets retaining the 8.x floor.
	for _, tc := range []struct {
		name, version string
		replicas      int
		create        bool
	}{
		{"disabled", "9.5.5", 0, false},
		{"same version", "9.5.4", 1, false},
		{"create", "9.5.5", 1, true},
		{"eight target", "8.19.23", 1, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := kibanaAdmissionCluster(p, "9.5.4", 1, 1.5, 1.5)
			old.Generation = 2
			n := kibanaAdmissionCluster(p, tc.version, tc.replicas, 1.5, 1.5)
			if tc.create {
				old = nil
			}
			if err := validateKibanaAdmission(old, n); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestKibanaCatalogResourcesAndHeap(t *testing.T) {
	p := New(ProviderOptions{})
	for _, version := range catalog {
		for _, preset := range p.Describe().Presets {
			c := &domain.Cluster{Name: "search", Spec: domain.ClusterSpec{Version: version.Version, Topology: preset.Topology, Components: preset.Components}}
			p.ApplyDefaults(&c.Spec)
			k := c.Spec.Components.Extra["kibana"]
			if k.Resources.MinRAM < 1.5 || k.Resources.MaxRAM < k.Resources.MinRAM {
				t.Fatalf("%s/%s: %+v", version.Version, preset.Name, k)
			}
			if err := p.Validate(context.Background(), nil, c); err != nil {
				t.Fatal(err)
			}
		}
	}
	if KibanaHeapMB(1) != 614 || KibanaHeapMB(1.5) != 921 {
		t.Fatal("old-space policy changed")
	}
}

func TestKibanaActiveMinimumAndDescription(t *testing.T) {
	for _, tc := range []struct {
		version string
		want    float64
	}{
		{"8.19.23", 1}, {"9.4.8", 1.5}, {"9.5.4", 1.5}, {"9.5.5", 1.5},
		{"", 0}, {"9.invalid", 0}, {"v9.5.5", 0},
	} {
		if got := KibanaActiveMinimumRAM(tc.version); got != tc.want {
			t.Errorf("%q floor = %g, want %g", tc.version, got, tc.want)
		}
	}
	for _, topology := range New(ProviderOptions{}).Describe().Topologies {
		description := string(topology.Components["kibana"].Schema)
		for _, text := range []string{"1.5 GiB on 9.x", "1 GiB on 8.x", "unchanged same-version", "Disabled"} {
			if !strings.Contains(description, text) {
				t.Errorf("%s Kibana schema omits %q: %s", topology.Type, text, description)
			}
		}
	}
}

func TestKibanaActiveDownsizeAndEightCompatibility(t *testing.T) {
	p := New(ProviderOptions{})
	old := kibanaAdmissionCluster(p, "9.5.5", 1, 1.5, 1.5)
	n := kibanaAdmissionCluster(p, "9.5.5", 1, 1, 1)
	if err := p.Validate(context.Background(), old, n); err == nil {
		t.Fatal("active downsize below the floor was accepted")
	}
	old = kibanaAdmissionCluster(p, "8.19.23", 1, 1, 1)
	n = cloneCluster(t, old)
	n.Spec.Monitoring.Enabled = false
	if err := p.Validate(context.Background(), old, n); err != nil {
		t.Fatal(err)
	}
}
