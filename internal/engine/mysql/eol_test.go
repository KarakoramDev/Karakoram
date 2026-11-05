// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestEOLAdmissionPreservesExistingClusters(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		p := New(Options{Provider: provider})
		for _, version := range p.Describe().Versions {
			if version.Major != "8.0" {
				continue
			}
			t.Run(provider+"/"+version.Version, func(t *testing.T) {
				if version.Status != "deprecated" || version.Default {
					t.Fatalf("EOL catalog entry: %+v", version)
				}
				c := &domain.Cluster{Name: "sample", Spec: domain.ClusterSpec{Version: version.Version}}
				p.ApplyDefaults(&c.Spec)
				err := p.Validate(t.Context(), nil, c)
				var field *engine.ValidationError
				if !errors.As(err, &field) || field.Field != "spec.version" || field.Reason != domain.ReasonUnsupported || !errors.Is(err, engine.ErrInvalid) || !strings.Contains(err.Error(), "8.0 reached end of life; new clusters must use 8.4") {
					t.Fatalf("expected structured EOL rejection, got %v", err)
				}
				next := *c
				next.Spec.Components.Engine.Resources.MaxRAM++
				if err := p.Validate(t.Context(), c, &next); err != nil {
					t.Fatalf("existing EOL update: %v", err)
				}
				for _, target := range p.Describe().Versions {
					if target.Major != "8.4" {
						continue
					}
					next.Spec.Version = target.Version
					if err := p.Validate(t.Context(), c, &next); err != nil {
						t.Fatalf("EOL upgrade to %s: %v", target.Version, err)
					}
					found := false
					for _, path := range p.Describe().UpgradePaths {
						found = found || path.From == version.Version && path.To == target.Version
					}
					if !found {
						t.Fatalf("missing catalog upgrade %s -> %s", version.Version, target.Version)
					}
				}
				_, backup, err := p.artifactPins(version.Version)
				if err != nil || backup != "percona-xtrabackup-8.0.35-36" {
					t.Fatalf("EOL backup compatibility pin: %s, %v", backup, err)
				}
			})
		}
	}
}

func TestEOLRecoveryCreatesRequireValidDataSource(t *testing.T) {
	for _, provider := range []string{"mysql", "pxc"} {
		t.Run(provider, func(t *testing.T) {
			_, rc, _ := setup(t, "single")
			p := New(Options{Provider: provider, Store: rc.Store})
			c := &domain.Cluster{Namespace: "default", Name: "recover"}
			p.ApplyDefaults(&c.Spec)
			for _, v := range p.versions() {
				if v.Major == "8.0" {
					c.Spec.Version = v.Version
					break
				}
			}
			storage := &domain.BackupStorage{Namespace: "default", Name: "recovery", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", VerifyTLS: true}
			if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
				t.Fatal(err)
			}
			c.Spec.Backup.Storage = storage.Name
			c.Spec.DataSource = &domain.DataSource{Cluster: "source", Backup: "saved"}
			if err := p.Validate(t.Context(), nil, c); err == nil {
				t.Fatal("forged backup bypassed EOL admission")
			}
			b := &domain.Backup{Namespace: "default", Cluster: "source", Name: "saved", Provider: provider, SourceVersion: c.Spec.Version, Phase: domain.PhaseSucceeded, Type: "full", Storage: storage.Name, MySQL: &domain.MySQLBackup{}}
			if err := rc.Store.CreateBackup(t.Context(), b); err != nil {
				t.Fatal(err)
			}
			if err := p.Validate(t.Context(), nil, c); err != nil {
				t.Fatalf("existing 8.0 backup recovery rejected: %v", err)
			}
			c.Spec.DataSource.Backup = ""
			now := time.Now()
			c.Spec.DataSource.PITRTime = &now
			if err := p.Validate(t.Context(), nil, c); err == nil {
				t.Fatal("missing PITR source bypassed EOL admission")
			}
			source := &domain.Cluster{Namespace: "default", Name: "source", Spec: c.Spec}
			source.Spec.DataSource = nil
			source.Spec.Backup.PITR.Enabled = true
			if err := rc.Store.(*store.Store).CreateCluster(t.Context(), source); err != nil {
				t.Fatal(err)
			}
			if err := p.Validate(t.Context(), nil, c); err != nil {
				t.Fatalf("existing 8.0 PITR recovery rejected: %v", err)
			}
			c.Spec.Version = p.versions()[0].Version
			if err := p.Validate(t.Context(), nil, c); err == nil {
				t.Fatal("cross-major physical recovery accepted")
			}
		})
	}
}
