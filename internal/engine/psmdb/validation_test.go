// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestProviderValidationFields(t *testing.T) {
	p := New(Options{})
	for _, tc := range []struct {
		name, field string
		change      func(*domain.ClusterSpec)
	}{
		{"engine count", "spec.components.engine.replicas", func(s *domain.ClusterSpec) { s.Components.Engine.Replicas = 0 }},
		{"config count", "spec.components.configServer.replicas", func(s *domain.ClusterSpec) { s.Components.ConfigServer.Replicas = 2 }},
		{"shards", "spec.topology.shards", func(s *domain.ClusterSpec) { s.Topology.Shards = 0 }},
		{"CPU zero", "spec.components.engine.resources.maxCpu", func(s *domain.ClusterSpec) { s.Components.Engine.Resources.MaxCPU = 0 }},
		{"CPU fractional", "spec.components.engine.resources.minCpu", func(s *domain.ClusterSpec) {
			s.Components.Engine.Resources.MinCPU = 1.5
			s.Components.Engine.Resources.MaxCPU = 2
		}},
		{"RAM minimum", "spec.components.engine.resources.minRam", func(s *domain.ClusterSpec) { s.Components.Engine.Resources.MinRAM = 0.25 }},
		{"disk minimum", "spec.components.engine.resources.minDisk", func(s *domain.ClusterSpec) { s.Components.Engine.Resources.MinDisk = 1 }},
		{"retain", "spec.backup.schedules.0.retainCount", func(s *domain.ClusterSpec) {
			s.Backup.Schedules = []domain.BackupSchedule{{Name: "daily", Cron: "0 0 * * *", Type: "logical", RetainCount: 0}}
		}},
		{"version", "spec.version", func(s *domain.ClusterSpec) { s.Version = "999.0" }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := domain.Cluster{Name: "sample", Spec: domain.ClusterSpec{Topology: domain.Topology{Type: "sharded"}}}
			p.ApplyDefaults(&c.Spec)
			tc.change(&c.Spec)
			err := p.Validate(context.Background(), nil, &c)
			var f *engine.ValidationError
			var d *domain.FieldError
			field := ""
			if errors.As(err, &f) {
				field = f.Field
			}
			if errors.As(err, &d) {
				field = d.Field
			}
			if field != tc.field {
				t.Fatalf("%s: %v", field, err)
			}
			if tc.name == "version" && (!strings.Contains(err.Error(), "psmdb") || !strings.Contains(err.Error(), "8.0.32-14")) {
				t.Fatal(err)
			}
		})
	}
	for _, count := range []int{1, 3, 5, 7} {
		c := domain.Cluster{Name: "sample", Spec: domain.ClusterSpec{Topology: domain.Topology{Type: "sharded"}}}
		p.ApplyDefaults(&c.Spec)
		c.Spec.Components.ConfigServer.Replicas = count
		if err := p.Validate(context.Background(), nil, &c); err != nil {
			t.Fatal(count, err)
		}
	}
}

func TestProviderOmittedDefaults(t *testing.T) {
	p := New(Options{})
	s := domain.ClusterSpec{Topology: domain.Topology{Type: "sharded"}}
	p.ApplyDefaults(&s)
	if s.Topology.Shards != 1 || s.Components.Engine.Replicas != 3 || s.Components.ConfigServer.Replicas != 3 || s.Components.Router.Replicas != 1 || s.Components.Engine.Resources.MaxCPU != 1 || s.Components.Engine.Resources.MinRAM != 1 || s.Components.Engine.Resources.MinDisk != 5 {
		t.Fatal(s)
	}
}
