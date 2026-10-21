// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"errors"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

func TestUpgradeBackupAdmissionOnlyPendingVersion(t *testing.T) {
	p, rc, backend := setup(t, "group")
	rc.Cluster.Spec.Version = "8.0.46-37"
	converge(t, p, rc, backend)
	rc.Cluster.Generation++
	rc.Cluster.Spec.Version = "8.4.11-11"
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if !p.UpgradeBackupReady(t.Context(), rc.Cluster, rc.Secrets) {
		t.Fatal("prerequisite backup blocked during version-only upgrade")
	}
	rc.Cluster.Spec.Components.Engine.Replicas++
	if p.UpgradeBackupReady(t.Context(), rc.Cluster, rc.Secrets) {
		t.Fatal("pending scale admitted")
	}
	rc.Cluster.Spec.Components.Engine.Replicas--
	originalTLS := rc.Cluster.Spec.TLS.Mode
	rc.Cluster.Spec.TLS.Mode = "disableTLS"
	if p.UpgradeBackupReady(t.Context(), rc.Cluster, rc.Secrets) {
		t.Fatal("pending TLS change admitted")
	}
	rc.Cluster.Spec.TLS.Mode = originalTLS
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.Formation.Reformation = &reformation{State: "formed"}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if !p.UpgradeBackupReady(t.Context(), rc.Cluster, rc.Secrets) {
		t.Fatal("completed reformation blocked prerequisite backup")
	}
	s.Formation.Reformation.State = "collect"
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if p.UpgradeBackupReady(t.Context(), rc.Cluster, rc.Secrets) {
		t.Fatal("active reformation admitted prerequisite backup")
	}
	s.Formation.Reformation.State = "formed"
	s.Rolling.Stage = "prepare"
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if p.UpgradeBackupReady(t.Context(), rc.Cluster, rc.Secrets) {
		t.Fatal("backup admitted after upgrade side effects became eligible")
	}
}

func TestGaleraTLSRotationFencesBackupAdmission(t *testing.T) {
	p, rc, backend := setup(t, "group")
	rc.Cluster.Spec.Version = "8.0.46-37"
	converge(t, p, rc, backend)
	rc.Cluster.Spec.Version = "8.4.11-11"
	rc.Cluster.Generation++
	if _, err := p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	if !p.UpgradeBackupReady(t.Context(), rc.Cluster, rc.Secrets) {
		t.Fatal("version-only prerequisite was not admitted")
	}
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.GaleraTLS = &galeraTLSRotation{}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	if p.UpgradeBackupReady(t.Context(), rc.Cluster, rc.Secrets) {
		t.Fatal("shared TLS rotation admitted prerequisite backup")
	}
}

func TestGaleraTLSRotationFencesBackupOperations(t *testing.T) {
	p, rc, backend := setup(t, "single")
	converge(t, p, rc, backend)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	s.GaleraTLS = &galeraTLSRotation{}
	if err = save(rc.Cluster, s); err != nil {
		t.Fatal(err)
	}
	for _, kind := range []string{domain.OperationBackup, domain.OperationDeleteBackup, domain.OperationRestore} {
		_, err = p.RunOperation(t.Context(), rc, &domain.Operation{ID: "tls-fenced", Kind: kind})
		if !errors.Is(err, engine.ErrUnavailable) {
			t.Fatalf("%s escaped shared TLS fence: %v", kind, err)
		}
	}
}
