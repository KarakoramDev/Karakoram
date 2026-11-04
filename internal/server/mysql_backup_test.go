// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package server

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestMySQLBackupMetadataMapping(t *testing.T) {
	stamp := time.Date(2026, 10, 21, 12, 0, 0, 0, time.UTC)
	b := domain.Backup{Name: "incremental", Namespace: "default", Cluster: "demo", Provider: "mysql", Storage: "archive", Type: "incremental", Phase: domain.PhaseSucceeded, RecoveryIdentity: "sealed-identity", MySQL: &domain.MySQLBackup{Label: "label", Repository: "prefix/mysql/default/demo-uid-g2", Type: "incremental", ParentLabel: "parent", ChainRootLabel: "root", GTIDSet: "11111111-2222-3333-4444-000000000001:1-20", FromLSN: 9007199254740993, ToLSN: 9007199254740994, ServerVersion: "8.4.11-11", ToolVersion: "8.4.0-7", EndTime: stamp, ObjectPaths: []string{"prefix/mysql/default/demo-uid-g2/base/label"}, ClusterUID: "uid", Generation: 2}}
	b.MySQL.PhysicalIdentity = strings.Repeat("a", 64)
	result := backupAPI(b)
	if result.Mysql == nil || result.Mysql.ParentLabel == nil || *result.Mysql.ParentLabel != "parent" || result.Mysql.FromLSN != b.MySQL.FromLSN || result.Mysql.ToLSN != b.MySQL.ToLSN || result.Mysql.ClusterUID != "uid" || !result.Mysql.EndTime.Equal(stamp) {
		t.Fatalf("metadata mapping lost provenance: %+v", result.Mysql)
	}
	if result.Mysql.PhysicalIdentity == nil || *result.Mysql.PhysicalIdentity != b.MySQL.PhysicalIdentity {
		t.Fatal("physical backup chain identity was not exposed")
	}
	raw, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), "sealed-identity") {
		t.Fatal("sealed recovery identity leaked to public API")
	}
}
