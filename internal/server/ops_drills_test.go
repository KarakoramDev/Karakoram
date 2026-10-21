// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/drills"
)

func TestDrillPolicyValidation(t *testing.T) {
	for _, tc := range []struct {
		name, schedule, timeout, backupSchedule string
		managed, valid                          bool
	}{
		{name: "defaults", schedule: "0 3 * * *", valid: true},
		{name: "minimum", schedule: "0 3 * * *", timeout: "30m", valid: true},
		{name: "maximum", schedule: "0 3 * * *", timeout: "48h", valid: true},
		{name: "short", schedule: "0 3 * * *", timeout: "29m"},
		{name: "long", schedule: "0 3 * * *", timeout: "49h"},
		{name: "cron", schedule: "invalid"},
		{name: "missing backup schedule", schedule: "0 3 * * *", backupSchedule: "missing"},
		{name: "known backup schedule", schedule: "0 3 * * *", backupSchedule: "daily", valid: true},
		{name: "managed", schedule: "0 3 * * *", managed: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			c := domain.Cluster{Spec: domain.ClusterSpec{Backup: domain.BackupConfig{Drill: &domain.BackupDrillPolicy{Schedule: tc.schedule, Timeout: tc.timeout, BackupSchedule: tc.backupSchedule}, Schedules: []domain.BackupSchedule{{Name: "daily"}}}}}
			if tc.managed {
				c.ManagedBy = &domain.ManagedBy{Kind: "backup-drill"}
			}
			r := validateDrillPolicy(&c)
			if (r == nil) != tc.valid {
				t.Fatalf("valid=%t response=%+v", tc.valid, r)
			}
			if tc.valid && tc.timeout == "" && c.Spec.Backup.Drill.Timeout != "6h" {
				t.Fatal("default timeout not persisted")
			}
		})
	}
}

func TestDrillAPIDefaultsAndReports(t *testing.T) {
	s, _, token := testServer(t)
	ctx := context.Background()
	path := "/api/v1/clusters/prod/backup-drills"
	if w := call(s, "POST", path, "", token); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	now := time.Now().UTC()
	for _, b := range []domain.Backup{
		{Name: "old", Cluster: "prod", Provider: "fake", Type: "full", Phase: domain.PhaseSucceeded, CreatedAt: now.Add(-time.Hour)},
		{Name: "latest", Cluster: "prod", Provider: "fake", Type: "full", Phase: domain.PhaseSucceeded, CreatedAt: now},
		{Name: "failed", Cluster: "prod", Provider: "fake", Type: "full", Phase: domain.PhaseFailed, CreatedAt: now.Add(time.Minute)},
	} {
		b := b
		if err := s.Store.CreateBackup(ctx, &b); err != nil {
			t.Fatal(err)
		}
	}
	w := call(s, "POST", path, "", token)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	var d domain.BackupDrill
	if err := json.Unmarshal(w.Body.Bytes(), &d); err != nil {
		t.Fatal(err)
	}
	if d.Backup != "latest" || d.Phase != "Pending" || d.Trigger != "manual" || d.ID == "" {
		t.Fatalf("%+v", d)
	}
	if w = call(s, "GET", path+"/"+d.ID, "", token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = call(s, "GET", path+"?limit=1", "", token); w.Code != 200 || !strings.Contains(w.Body.String(), d.ID) {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = call(s, "POST", path, `{"backup":"failed"}`, token); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = call(s, "POST", path, `{"backup":"missing"}`, token); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = call(s, "GET", path+"/missing", "", token); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestDrillTargetMutationGuard(t *testing.T) {
	s, _, _ := testServer(t)
	ctx := context.Background()
	d := domain.BackupDrill{ID: "guard", Namespace: domain.DefaultNamespace, Cluster: "prod", Phase: "Restoring", CreatedAt: time.Now()}
	if err := s.Store.CreateBackupDrill(ctx, d); err != nil {
		t.Fatal(err)
	}
	c := domain.Cluster{ManagedBy: &domain.ManagedBy{Kind: "backup-drill", ID: d.ID}}
	if r := s.drillMutationGuard(ctx, &c); r == nil || r.status != 409 {
		t.Fatalf("%+v", r)
	}
	if err := s.Store.DeleteCluster(ctx, domain.ClusterRef{Name: "prod"}); err != nil {
		t.Fatal(err)
	}
	if r := s.drillMutationGuard(ctx, &c); r == nil || r.status != 409 {
		t.Fatalf("source deletion released active managed target: %+v", r)
	}
	d.Phase = "Failed"
	if _, err := s.Store.UpdateBackupDrill(ctx, d, "Restoring"); err != nil {
		t.Fatal(err)
	}
	if r := s.drillMutationGuard(ctx, &c); r != nil {
		t.Fatalf("%+v", r)
	}
	c.ManagedBy.ID = "missing-report"
	if r := s.drillMutationGuard(ctx, &c); r == nil || r.status != 409 {
		t.Fatalf("missing report released managed target: %+v", r)
	}
}

func TestDrillCreatePathAndOwnership(t *testing.T) {
	s, _, token := testServer(t)
	ctx := context.Background()
	source, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	backup := domain.Backup{Name: "copy", Cluster: "prod", Provider: "fake", Type: "logical", Phase: domain.PhaseSucceeded}
	if err = s.Store.CreateBackup(ctx, &backup); err != nil {
		t.Fatal(err)
	}
	source.Spec.Backup.Drill = &domain.BackupDrillPolicy{Enabled: true, Schedule: "0 3 * * *"}
	source.Spec.Backup.Schedules = []domain.BackupSchedule{{Name: "daily"}}
	source.Spec.Exposure.Public = true
	source.Spec.Monitoring.Enabled = true
	d := domain.BackupDrill{ID: "owned-copy", Namespace: domain.DefaultNamespace, Cluster: "prod", Backup: "copy", Phase: "Provisioning", CreatedAt: time.Now()}
	if err = s.Store.CreateBackupDrill(ctx, d); err != nil {
		t.Fatal(err)
	}
	if err = s.createDrillTarget(ctx, *source, d); err != nil {
		t.Fatal(err)
	}
	if err = s.createDrillTarget(ctx, *source, d); err != nil {
		t.Fatal("replay", err)
	}
	ref := domain.ClusterRef{Name: drills.TargetName(d.ID)}
	target, err := s.Store.GetCluster(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	if target.ManagedBy == nil || target.ManagedBy.ID != d.ID || target.Spec.Backup.Drill != nil || len(target.Spec.Backup.Schedules) != 0 || target.Spec.Exposure.Public || target.Spec.Monitoring.Enabled || target.Spec.DataSource == nil || target.Spec.DataSource.Backup != "copy" {
		t.Fatalf("%+v", target)
	}
	if target.RequestedBy != "system:backup-drill" {
		t.Fatalf("drill target requester = %q", target.RequestedBy)
	}
	operations, err := s.Store.ListOperations(ctx, ref)
	if err != nil || len(operations) != 1 {
		t.Fatalf("drill target operations = %+v, error = %v", operations, err)
	}
	if operations[0].Kind != domain.OperationCreate || operations[0].Actor != "system:backup-drill" {
		t.Fatalf("drill create operation attribution = %+v", operations[0])
	}
	path := "/api/v1/clusters/" + target.Name
	for _, request := range []struct{ method, body string }{{"PATCH", `{"spec":{"version":"8.0"}}`}, {"PUT", `{"spec":{"provider":"fake"}}`}, {"DELETE", ""}} {
		if w := call(s, request.method, path, request.body, token); w.Code != 409 {
			t.Fatal(request.method, w.Code, w.Body.String())
		}
	}
	if w := call(s, "POST", path+"/backup-drills", "", token); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if err = s.deleteDrillTarget(ctx, *source); err == nil {
		t.Fatal("unowned source deletion accepted")
	}
	current, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
	if err != nil || current.Deleting {
		t.Fatal("source was changed", err)
	}
	if err = s.deleteDrillTarget(ctx, *target); err != nil {
		t.Fatal(err)
	}
	target, err = s.Store.GetCluster(ctx, ref)
	if err != nil || !target.Deleting || target.DeleteOptions.KeepVolumes || target.DeleteOptions.KeepBackups {
		t.Fatal(target, err)
	}
}

func TestDrillPolicyHTTPValidation(t *testing.T) {
	s, _, token := testServer(t)
	for _, request := range []struct{ method, path, body string }{
		{"POST", "/api/v1/clusters", `{"name":"baddrill","spec":{"provider":"fake","backup":{"drill":{"enabled":true,"schedule":"bad"}}}}`},
		{"PUT", "/api/v1/clusters/prod", `{"spec":{"provider":"fake","backup":{"drill":{"enabled":true,"schedule":"0 3 * * *","timeout":"2m"}}}}`},
		{"PATCH", "/api/v1/clusters/prod", `{"spec":{"backup":{"drill":{"enabled":true,"schedule":"0 3 * * *","backupSchedule":"missing"}}}}`},
	} {
		w := call(s, request.method, request.path, request.body, token)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "spec.backup.drill.") {
			t.Fatal(request.method, w.Code, w.Body.String())
		}
	}
}
