// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/hex"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"testing"
	"time"
)

func assertGaleraProductionIdentity(t *testing.T, identity string) {
	t.Helper()
	if len(identity) != 32 || !myspec.ValidGaleraClusterName(identity) {
		t.Fatal("Galera handshake identity is not bounded to 32 bytes", identity)
	}
	if _, err := hex.DecodeString(identity); err != nil {
		t.Fatal("production Galera identity is not random hexadecimal", identity, err)
	}
}
func TestReplicationIdentityUsesProviderProtocolFormat(t *testing.T) {
	mysql := New(Options{})
	gr, err := mysql.newReplicationIdentity()
	if err != nil || !myspec.ValidUUID(gr) {
		t.Fatal("GR lost UUID identity", gr, err)
	}
	pxc := New(Options{Provider: "pxc"})
	first, err := pxc.newReplicationIdentity()
	if err != nil {
		t.Fatal(err)
	}
	assertGaleraProductionIdentity(t, first)
	second, err := pxc.newReplicationIdentity()
	if err != nil {
		t.Fatal(err)
	}
	assertGaleraProductionIdentity(t, second)
	if first == second {
		t.Fatal("new Galera histories reused the same identity")
	}
}
func TestPXCInitialAndRestoreRequestsProduceHandshakeBoundedIdentities(t *testing.T) {
	p, rc, backend := setupPXC(t)
	converge(t, p, rc, backend)
	s, err := load(rc.Cluster)
	if err != nil {
		t.Fatal(err)
	}
	assertGaleraProductionIdentity(t, s.Formation.GroupName)
	storage := &domain.BackupStorage{Namespace: rc.Cluster.Namespace, Name: "archive", Type: "s3", EndpointURL: "https://objects.example", Bucket: "backups", Region: "region", Prefix: "prefix", ForcePathStyle: true, VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "access", SecretAccessKey: "secret"}}
	if err := rc.Store.(*store.Store).CreateBackupStorage(t.Context(), storage); err != nil {
		t.Fatal(err)
	}
	rc.Cluster.Spec.Backup.Storage = storage.Name
	prefix := repoPrefix(storage, rc.Cluster, s, 0)
	stamp := time.Now().UTC()
	backup := &domain.Backup{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "base", Provider: "pxc", Storage: storage.Name, Type: "full", SourceVersion: rc.Cluster.Spec.Version, SourceTopology: rc.Cluster.Spec.Topology, Phase: domain.PhaseSucceeded, CompletedAt: &stamp, RecoveryIdentity: "backup-source", MySQL: &domain.MySQLBackup{Label: "base", Repository: prefix, Type: "full", ChainRootLabel: "base", ClusterUID: s.UID, ServerVersion: rc.Cluster.Spec.Version, ToolVersion: "8.4.0-7", PhysicalIdentity: "fixture-source", ToLSN: 10, EndTime: stamp, ObjectPaths: []string{prefix + "/base/base"}}}
	if err := captureFixtureBackupAccess(t.Context(), rc, backup); err != nil {
		t.Fatal(err)
	}
	if err := rc.Store.CreateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	restore := &domain.Restore{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Name: "identity-restore", Phase: domain.PhasePending, Source: domain.RestoreSource{Backup: backup.Name}}
	if err := rc.Store.CreateRestore(t.Context(), restore); err != nil {
		t.Fatal(err)
	}
	result, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "identity-restore", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"identity-restore"}`)})
	if err != nil {
		t.Fatal(err)
	}
	var checkpoint backupCheckpoint
	if err := json.Unmarshal(result.Checkpoint, &checkpoint); err != nil {
		t.Fatal(err)
	}
	if checkpoint.Step != "preflight" || checkpoint.Request == nil {
		t.Fatal("restore did not freeze new history", checkpoint)
	}
	assertGaleraProductionIdentity(t, checkpoint.Request.NewGroupName)
	if checkpoint.Request.NewGroupName == s.Formation.GroupName {
		t.Fatal("restore reused predecessor history")
	}
}
