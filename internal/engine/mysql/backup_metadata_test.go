// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestBackupMetadataCheckpointTypes(t *testing.T) {
	for _, kind := range []string{"full", "incremental"} {
		t.Run(kind, func(t *testing.T) {
			b := &domain.Backup{Type: kind, SourceVersion: "8.4.11-11", MySQL: &domain.MySQLBackup{ObjectPaths: []string{"prefix/base/backup"}, PhysicalIdentity: "physical-secondary"}}
			r := myspec.BackupResult{BackupType: "full-backuped", ServerVersion: b.SourceVersion, ToolVersion: "8.4.0-7", EndTime: time.Now().UTC(), ToLSN: 10, ObjectPrefix: b.MySQL.ObjectPaths[0], PhysicalIdentity: b.MySQL.PhysicalIdentity}
			if kind == "incremental" {
				r.BackupType = "incremental"
			}
			if field := backupMetadataMismatch(b, &r); field != "" {
				t.Fatalf("native XtraBackup checkpoint rejected: %s", field)
			}
			for _, wrong := range []string{"full", "full-prepared", "log-applied", "unknown"} {
				r.BackupType = wrong
				if field := backupMetadataMismatch(b, &r); field != "backupType" {
					t.Fatalf("checkpoint %q: got %q, want backupType", wrong, field)
				}
			}
		})
	}
}

func TestBackupMetadataMismatchFields(t *testing.T) {
	b := &domain.Backup{Type: "full", SourceVersion: "8.4.11-11", MySQL: &domain.MySQLBackup{ObjectPaths: []string{"prefix/base/backup"}, PhysicalIdentity: "physical-secondary"}}
	valid := myspec.BackupResult{BackupType: "full-backuped", ServerVersion: b.SourceVersion, ToolVersion: "8.4.0-7", EndTime: time.Now().UTC(), ToLSN: 10, ObjectPrefix: b.MySQL.ObjectPaths[0], PhysicalIdentity: b.MySQL.PhysicalIdentity}
	if field := backupMetadataMismatch(b, nil); field != "result" {
		t.Fatalf("missing result: %q", field)
	}
	for _, test := range []struct {
		field  string
		change func(*myspec.BackupResult)
	}{
		{"endTime", func(r *myspec.BackupResult) { r.EndTime = time.Time{} }},
		{"toLSN", func(r *myspec.BackupResult) { r.ToLSN = 0 }},
		{"toLSN", func(r *myspec.BackupResult) { r.FromLSN = r.ToLSN + 1 }},
		{"toolVersion", func(r *myspec.BackupResult) { r.ToolVersion = "" }},
		{"serverVersion", func(r *myspec.BackupResult) { r.ServerVersion = "8.0.45-36" }},
		{"objectPrefix", func(r *myspec.BackupResult) { r.ObjectPrefix += "-other" }},
		{"backupType", func(r *myspec.BackupResult) { r.BackupType = "incremental" }},
		{"fromLSN", func(r *myspec.BackupResult) { r.FromLSN = 1 }},
		{"physicalIdentity", func(r *myspec.BackupResult) { r.PhysicalIdentity = "physical-primary" }},
	} {
		t.Run(test.field, func(t *testing.T) {
			r := valid
			test.change(&r)
			if field := backupMetadataMismatch(b, &r); field != test.field {
				t.Fatalf("got %q, want %q", field, test.field)
			}
		})
	}
}
