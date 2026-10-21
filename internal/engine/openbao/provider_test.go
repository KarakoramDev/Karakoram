// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestValidate(t *testing.T) {
	p := New(ProviderOptions{})
	cases := []struct {
		name   string
		change func(*domain.Cluster)
		valid  bool
	}{
		{"defaults", func(*domain.Cluster) {}, true},
		{"single", func(c *domain.Cluster) { c.Spec.Components.Engine.Replicas = 1 }, true},
		{"five", func(c *domain.Cluster) { c.Spec.Components.Engine.Replicas = 5 }, true},
		{"name", func(c *domain.Cluster) { c.Name = "Bad" }, false},
		{"version", func(c *domain.Cluster) { c.Spec.Version = "2.6.0" }, false},
		{"topology", func(c *domain.Cluster) { c.Spec.Topology.Type = "replicaset" }, false},
		{"replicas", func(c *domain.Cluster) { c.Spec.Components.Engine.Replicas = 2 }, false},
		{"cpu", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MinCPU = 1.5 }, false},
		{"nan", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MaxRAM = math.NaN() }, false},
		{"memory", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MinRAM = 0.1 }, false},
		{"disk", func(c *domain.Cluster) { c.Spec.Components.Engine.Resources.MinDisk = 1 }, false},
		{"disabled", func(c *domain.Cluster) { c.Spec.TLS.Mode = "disabled" }, false},
		{"preferTLS", func(c *domain.Cluster) { c.Spec.TLS.Mode = "preferTLS" }, false},
		{"publicWithoutAllowlist", func(c *domain.Cluster) { c.Spec.Exposure.Public = true }, false},
		{"public", func(c *domain.Cluster) {
			c.Spec.Exposure.Public = true
			c.Spec.Exposure.Allowlist = []string{"192.0.2.0/24"}
		}, true},
		{"cidr", func(c *domain.Cluster) { c.Spec.Exposure.Allowlist = []string{"invalid"} }, false},
		{"unknownOption", func(c *domain.Cluster) { c.Spec.EngineOptions = json.RawMessage(`{"unknown":true}`) }, false},
		{"trailingJSON", func(c *domain.Cluster) { c.Spec.EngineOptions = json.RawMessage(`{} {}`) }, false},
		{"null", func(c *domain.Cluster) { c.Spec.EngineOptions = json.RawMessage(`null`) }, false},
		{"shares", func(c *domain.Cluster) { c.Spec.EngineOptions = json.RawMessage(`{"unseal":{"shares":11}}`) }, false},
		{"threshold", func(c *domain.Cluster) {
			c.Spec.EngineOptions = json.RawMessage(`{"unseal":{"shares":5,"threshold":1}}`)
		}, false},
		{"oneShare", func(c *domain.Cluster) {
			c.Spec.EngineOptions = json.RawMessage(`{"unseal":{"shares":1,"threshold":1}}`)
		}, true},
		{"transit", func(c *domain.Cluster) {
			c.Spec.EngineOptions = json.RawMessage(`{"seal":{"type":"transit","transit":{"address":"https://example.com","keyName":"key","token":"never-echo-this"}}}`)
		}, true},
		{"postgresql", func(c *domain.Cluster) {
			c.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
		}, false},
		{"pitr", func(c *domain.Cluster) { c.Spec.Backup.PITR.Enabled = true }, false},
		{"schedule", func(c *domain.Cluster) { c.Spec.Backup.Schedules = []domain.BackupSchedule{{Name: "daily"}} }, false},
		{"dataSource", func(c *domain.Cluster) { c.Spec.DataSource = &domain.DataSource{} }, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			c := &domain.Cluster{Name: "demo"}
			p.ApplyDefaults(&c.Spec)
			tc.change(c)
			err := p.Validate(context.Background(), nil, c)
			if (err == nil) != tc.valid {
				t.Fatalf("valid=%v err=%v", tc.valid, err)
			}
			if err != nil && strings.Contains(err.Error(), "never-echo-this") {
				t.Fatal("write-only option leaked in error")
			}
		})
	}
}
func TestValidateImmutableUnseal(t *testing.T) {
	p := New(ProviderOptions{})
	old := &domain.Cluster{Name: "demo"}
	p.ApplyDefaults(&old.Spec)
	n := *old
	n.Spec.EngineOptions = json.RawMessage(`{"unseal":{"shares":3,"threshold":2}}`)
	if err := p.Validate(t.Context(), old, &n); err == nil {
		t.Fatal("unseal settings changed")
	}
}
func TestStatePreservesOtherWorkflowKeys(t *testing.T) {
	c := &domain.Cluster{Internal: json.RawMessage(`{"unrelated":{"checkpoint":"kept"}}`)}
	s, err := loadState(c)
	if err != nil {
		t.Fatal(err)
	}
	s.InitOwner = "demom0"
	if err = s.save(c); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(c.Internal), `"checkpoint":"kept"`) {
		t.Fatal("foreign state lost")
	}
}
