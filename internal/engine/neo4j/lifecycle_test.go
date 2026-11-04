// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"reflect"
	"testing"
)

func TestExposureRevokesBeforeUnhealthyExpansion(t *testing.T) {
	p, rc, b, a := setup(t)
	rc.Cluster.Spec.Exposure.Public = true
	rc.Cluster.Spec.Exposure.Allowlist = []string{"2001:db8:1::/64"}
	converge(t, p, rc)
	routes, err := b.ListPublicPorts(t.Context())
	if err != nil || len(routes) != 1 || routes[0].Port != 7687 || routes[0].TargetPort != 7687 {
		t.Fatal(routes, err)
	}
	rc.Cluster.Spec.Exposure.Allowlist = []string{"2001:db8:2::/64"}
	a.credentialMismatch = true
	if _, err = p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	routes, err = b.ListPublicPorts(t.Context())
	if err != nil || len(routes) != 0 {
		t.Fatal("access expanded before readiness", routes, err)
	}
	a.credentialMismatch = false
	converge(t, p, rc)
	routes, err = b.ListPublicPorts(t.Context())
	if err != nil || len(routes) != 1 || !reflect.DeepEqual(routes[0].Allowlist, rc.Cluster.Spec.Exposure.Allowlist) {
		t.Fatal(routes, err)
	}
	rc.Cluster.Spec.Exposure.Public = false
	a.credentialMismatch = true
	if _, err = p.Sync(t.Context(), rc); err != nil {
		t.Fatal(err)
	}
	routes, err = b.ListPublicPorts(t.Context())
	if err != nil || len(routes) != 0 {
		t.Fatal("public revocation waited for health", routes, err)
	}
}
func TestUpgradeDoesNotPublishUnpreparedVersion(t *testing.T) {
	p, rc, _, _ := setup(t)
	rc.Cluster.Spec.Version = "2026.09.0"
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	rc.Cluster.Spec.Version = defaultVersion
	cfg, err := p.MemberConfig(t.Context(), rc, s.Member.Name)
	if err != nil {
		t.Fatal(err)
	}
	var sp neo4jspec.Spec
	json.Unmarshal(cfg.EngineSpec, &sp)
	if sp.Version != "2026.09.0" {
		t.Fatal("unprepared binary published")
	}
	converge(t, p, rc)
	cfg, err = p.MemberConfig(t.Context(), rc, s.Member.Name)
	if err != nil {
		t.Fatal(err)
	}
	json.Unmarshal(cfg.EngineSpec, &sp)
	if sp.Version != defaultVersion {
		t.Fatal("upgrade did not publish prepared release")
	}
}
func TestTLSRotationCheckpointReplay(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	op := &domain.Operation{ID: "tls-retry", Kind: domain.OperationRotateTLS}
	first, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	op.Checkpoint = first.Checkpoint
	if _, err = p.RunOperation(t.Context(), rc, op); err != nil {
		t.Fatal(err)
	}
	s, _ := load(rc.Cluster)
	before, _ := secret(t.Context(), rc, "tls/servers/"+s.Member.Name+".crt")
	// Lose the returned operation checkpoint after the cluster state was saved.
	p = New(Options{})
	r, err := p.RunOperation(t.Context(), rc, op)
	if err != nil {
		t.Fatal(err)
	}
	after, _ := secret(t.Context(), rc, "tls/servers/"+s.Member.Name+".crt")
	if !reflect.DeepEqual(before, after) {
		t.Fatal("durably completed issuance repeated")
	}
	op.Checkpoint = r.Checkpoint
	runOp(t, p, rc, op)
}
func TestRuntimeRecoveryRetainsVolumeIdentity(t *testing.T) {
	p, rc, b, _ := setup(t)
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	oldVolume := s.Member.VolumeID
	if _, err := b.DeleteService(t.Context(), s.Member.Name); err != nil {
		t.Fatal(err)
	}
	converge(t, p, rc)
	s, _ = load(rc.Cluster)
	if s.Member.VolumeID != oldVolume || !s.Member.RuntimeRecovery.Initialized {
		t.Fatal("runtime recovery replaced data identity")
	}
}
func TestBackupDispatchRequiresInventoryResource(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	for _, kind := range []string{domain.OperationBackup, domain.OperationRestore, domain.OperationDeleteBackup} {
		_, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "missing-resource", Kind: kind})
		if err == nil || errors.Is(err, engine.ErrNotSupported) {
			t.Fatal(kind, err)
		}
	}
}
func TestScheduledDowntimeConsent(t *testing.T) {
	p := New(Options{})
	c := &domain.Cluster{Name: "demo"}
	p.ApplyDefaults(&c.Spec)
	c.Spec.Backup.Schedules = []domain.BackupSchedule{{Enabled: true}}
	c.Spec.EngineOptions = json.RawMessage(`{"acceptBackupDowntime":true}`)
	if err := p.Validate(t.Context(), nil, c); err != nil {
		t.Fatal(err)
	}
}

func TestResumeRetainsDataAndClearsRetainedInventory(t *testing.T) {
	p, rc, _, _ := setup(t)
	converge(t, p, rc)
	s, _ := load(rc.Cluster)
	volumeID := s.Member.VolumeID
	done := false
	for range 20 {
		var err error
		done, err = p.Cleanup(t.Context(), rc, domain.DeleteOptions{KeepVolumes: true})
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	if !done {
		t.Fatal("suspension did not finish")
	}
	converge(t, p, rc)
	s, _ = load(rc.Cluster)
	retained, err := rc.Store.ListRetainedVolumes(t.Context(), rc.Cluster.Ref())
	if err != nil || len(retained) != 0 || s.Suspended || s.Member.VolumeID != volumeID {
		t.Fatal("resume changed data identity or left stale retained inventory", retained, err)
	}
}

func TestRestartAndMaintenanceClaim(t *testing.T) {
	p, rc, _, a := setup(t)
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
		t.Fatal(allowed, err)
	}
	converge(t, p, rc)
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || !allowed {
		t.Fatal(allowed, err)
	}
	op := &domain.Operation{ID: "restart", Kind: domain.OperationRestart}
	runOp(t, p, rc, op)
	runOp(t, p, rc, op)
	if a.restarts != 1 {
		t.Fatal("restart repeated", a.restarts)
	}
	s, _ := load(rc.Cluster)
	s.Maintenance = "offline-operation"
	save(rc.Cluster, s)
	if allowed, err := p.CanClaimOperation(rc.Cluster); err != nil || allowed {
		t.Fatal(allowed, err)
	}
}
