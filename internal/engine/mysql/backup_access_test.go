// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

func TestRestoreHydratesCredentialsWithoutCheckpointSecrets(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	credentials := map[string]accessCredential{"app": {Current: "current-password", Secondary: "previous-password"}, "dropped": {Current: "dropped-password"}}
	users := []myspec.ProxyUser{{Username: "app", Password: "current-password", RequireTLS: true}, {Username: "dropped", Password: "dropped-password", RequireTLS: true}}
	raw, _ := json.Marshal(credentials)
	if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), "mysql/access/credentials", raw); err != nil {
		t.Fatal(err)
	}
	raw, _ = json.Marshal(users)
	if err := rc.Secrets.SetClusterSecret(t.Context(), rc.Cluster.Ref(), "mysql/access/proxy-users", raw); err != nil {
		t.Fatal(err)
	}
	base := inventoryBackup(t, rc, "base", "full", "", 10, time.Unix(10, 0))
	base.RecoveryIdentity = "source-snapshot"
	if err := captureFixtureBackupAccess(t.Context(), rc, &base); err != nil {
		t.Fatal(err)
	}
	if err := rc.Store.UpdateBackup(t.Context(), &base); err != nil {
		t.Fatal(err)
	}
	restore := &domain.Restore{Namespace: "default", Cluster: "demo", Name: "accounts", Source: domain.RestoreSource{Backup: base.Name}}
	if err := rc.Store.CreateRestore(t.Context(), restore); err != nil {
		t.Fatal(err)
	}
	result, err := p.RunOperation(t.Context(), rc, &domain.Operation{ID: "accounts", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"accounts"}`)})
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"current-password", "previous-password", "dropped-password", `"secretKey":"secret"`} {
		if strings.Contains(string(result.Checkpoint), secret) || strings.Contains(string(rc.Cluster.Internal), secret) {
			t.Fatal("restore checkpoint exposed credential")
		}
	}
	var cp backupCheckpoint
	if err = json.Unmarshal(result.Checkpoint, &cp); err != nil {
		t.Fatal(err)
	}
	request, err := p.hydrateRestore(t.Context(), rc, cp)
	if err != nil || len(request.RecoveryUsers) != 2 || request.RecoveryUsers[0].Current != "current-password" || request.RecoveryUsers[0].Secondary != "previous-password" {
		t.Fatal(request.RecoveryUsers, err)
	}
	cp.RecoveredUsers = []string{"app"}
	if err = restoreBackupAccess(t.Context(), rc, cp); err != nil {
		t.Fatal(err)
	}
	raw, err = rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "mysql/access/credentials")
	if err != nil {
		t.Fatal(err)
	}
	credentials = nil
	if err = json.Unmarshal(raw, &credentials); err != nil || len(credentials) != 1 || credentials["app"].Current != "current-password" {
		t.Fatal(credentials, err)
	}
	raw, err = rc.Secrets.GetClusterSecret(t.Context(), rc.Cluster.Ref(), "mysql/access/proxy-users")
	if err != nil {
		t.Fatal(err)
	}
	users = nil
	if err = json.Unmarshal(raw, &users); err != nil || len(users) != 1 || users[0].Username != "app" {
		t.Fatal(users, err)
	}
}

func TestRestorePhysicalManifestIsSealedAndCheckpointBound(t *testing.T) {
	p, rc, _ := backupFixture(t, "single")
	base := inventoryBackup(t, rc, "sealed-physical", "full", "", 10, time.Unix(10, 0))
	source := restoreSource{Namespace: base.Namespace, Cluster: base.Cluster, Backup: base.Name}
	_, request, err := p.resolveRestore(t.Context(), rc, source)
	if err != nil {
		t.Fatal(err)
	}
	if len(request.ObjectManifests) != 1 || request.ObjectManifests[0].ObjectPrefix != base.MySQL.ObjectPaths[0] || request.ObjectManifests[0].Digest != fixtureManifestDigest {
		t.Fatal("restore lost sealed physical object identity")
	}
	if err := sealBackupManifest(t.Context(), rc, &base, strings.Repeat("b", 64), 1); err == nil {
		t.Fatal("sealed manifest was replaced by a repository writer's digest")
	}
	request.Source.S3 = myspec.S3{}
	cp := backupCheckpoint{Source: &source, Request: &request}
	if _, err := p.hydrateRestore(t.Context(), rc, cp); err != nil {
		t.Fatal(err)
	}
	cp.Request.ObjectManifests[0].Digest = strings.Repeat("b", 64)
	if _, err := p.hydrateRestore(t.Context(), rc, cp); err == nil {
		t.Fatal("checkpoint broadened sealed physical input")
	}
	raw, err := rc.Secrets.GetClusterSecret(t.Context(), domain.ClusterRef{Namespace: base.Namespace, Name: base.RecoveryIdentity}, "mysql/access/snapshot")
	if err != nil {
		t.Fatal(err)
	}
	var values map[string][]byte
	if err := json.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	delete(values, backupManifestKey)
	raw, err = json.Marshal(values)
	if err != nil {
		t.Fatal(err)
	}
	if err := rc.Secrets.SetClusterSecret(t.Context(), domain.ClusterRef{Namespace: base.Namespace, Name: base.RecoveryIdentity}, "mysql/access/snapshot", raw); err != nil {
		t.Fatal(err)
	}
	if _, _, err := p.resolveRestore(t.Context(), rc, source); err == nil {
		t.Fatal("unsealed legacy physical objects were trusted")
	}
}
