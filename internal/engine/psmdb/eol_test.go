// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestEOLCreateAndExistingClusterAdmission(t *testing.T) {
	p := New(Options{})
	for _, topology := range []string{"replicaSet", "sharded"} {
		t.Run(topology, func(t *testing.T) {
			c := domain.Cluster{Name: "sample", Spec: domain.ClusterSpec{Version: "6.0.29-23", Topology: domain.Topology{Type: topology}}}
			p.ApplyDefaults(&c.Spec)
			err := p.Validate(t.Context(), nil, &c)
			var field *engine.ValidationError
			if !errors.As(err, &field) || field.Field != "spec.version" || field.Reason != domain.ReasonUnsupported || !strings.Contains(err.Error(), "new clusters must use 7.0 or 8.0") {
				t.Fatalf("wrong EOL rejection: %v", err)
			}
			old := c
			c.Spec.Components.Engine.Replicas = 5
			if err := p.Validate(t.Context(), &old, &c); err != nil {
				t.Fatalf("existing EOL update: %v", err)
			}
			old.Status.FCV = "6.0"
			for _, version := range []string{"7.0.40-22", "7.0.43-23", "7.0.45-24"} {
				c.Spec.Version = version
				if err := p.Validate(t.Context(), &old, &c); err != nil {
					t.Fatalf("EOL upgrade to %s: %v", version, err)
				}
				if err := p.Validate(t.Context(), nil, &c); err != nil {
					t.Fatalf("supported create %s: %v", version, err)
				}
			}
			c.Spec.Version = "8.0.34-15"
			if err := p.Validate(t.Context(), &old, &c); err == nil {
				t.Fatal("skipped major accepted")
			}
		})
	}
	for _, version := range p.Describe().Versions {
		if version.Major == "6.0" && version.Status != "deprecated" {
			t.Fatal(version)
		}
	}
}

func TestEOLRecoveryRequiresValidatedSource(t *testing.T) {
	p, rc, _, _ := setup(t)
	st := rc.Store.(*store.Store)
	target := *rc.Cluster
	target.Spec.Version = "6.0.29-23"
	target.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "saved"}
	if err := p.Validate(t.Context(), nil, &target); err == nil {
		t.Fatal("missing recovery source accepted")
	}
	backup := domain.Backup{Cluster: "source", Name: "saved", Phase: domain.PhaseSucceeded, SourceVersion: "6.0.29-23", SourceTopology: target.Spec.Topology}
	if err := st.CreateBackup(t.Context(), &backup); err != nil {
		t.Fatal(err)
	}
	if err := p.Validate(t.Context(), nil, &target); err != nil {
		t.Fatalf("retained EOL backup rejected: %v", err)
	}
	target.Spec.Topology.Type = "sharded"
	p.ApplyDefaults(&target.Spec)
	if err := p.Validate(t.Context(), nil, &target); err == nil {
		t.Fatal("mismatched recovery topology accepted")
	}
	target.Spec.Topology.Type = "replicaSet"
	target.Spec.Version = "7.0.45-24"
	if err := p.Validate(t.Context(), nil, &target); err == nil {
		t.Fatal("cross-major recovery accepted")
	}
	target.Spec.Version = "6.0.29-23"
	now := time.Now().UTC()
	source := domain.Cluster{Name: "source", Spec: target.Spec}
	source.Spec.DataSource = nil
	source.Status.Backup.PITRWindow = &domain.TimeWindow{Start: now.Add(-time.Hour), End: now.Add(time.Hour)}
	if err := st.CreateCluster(t.Context(), &source); err != nil {
		t.Fatal(err)
	}
	target.Spec.DataSource = &domain.DataSource{Cluster: "source", PITRTime: &now}
	if err := p.Validate(t.Context(), nil, &target); err != nil {
		t.Fatalf("EOL PITR recovery rejected: %v", err)
	}
	outside := now.Add(-2 * time.Hour)
	target.Spec.DataSource.PITRTime = &outside
	if err := p.Validate(t.Context(), nil, &target); err == nil {
		t.Fatal("outside-window EOL recovery accepted")
	}
}
