// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"sort"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

var backupAccessKeys = []string{"mysql/access/credentials", "mysql/access/proxy-users", "mysql/access/revision"}

// One sealed manifest makes missing values explicit and prevents a partial
// snapshot from being mistaken for an empty managed-account inventory.
func captureBackupAccess(ctx context.Context, rc *engine.Context, b *domain.Backup) error {
	values := map[string][]byte{}
	if previous, err := rc.Secrets.GetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "mysql/access/snapshot"); err == nil {
		var sealed map[string][]byte
		if err := json.Unmarshal(previous, &sealed); err != nil {
			return err
		}
		if manifest, ok := sealed[backupManifestKey]; ok {
			values[backupManifestKey] = manifest
		}
	} else if !missing(err) {
		return err
	}
	for _, key := range backupAccessKeys {
		v, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), key)
		if err != nil && !missing(err) {
			return err
		}
		values[key] = v
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return err
	}
	return rc.Secrets.SetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "mysql/access/snapshot", raw)
}
func restoreBackupAccess(ctx context.Context, rc *engine.Context, cp backupCheckpoint) error {
	if cp.Source == nil || cp.Request == nil {
		return engine.Invalid("restore has no frozen source")
	}
	b, err := rc.Store.GetBackup(ctx, domain.ClusterRef{Namespace: cp.Source.Namespace, Name: cp.Source.Cluster}, cp.Request.BackupID)
	if err != nil {
		return err
	}
	if b.RecoveryIdentity == "" {
		return engine.Invalid("MySQL backup has no sealed managed-account snapshot")
	}
	raw, err := rc.Secrets.GetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "mysql/access/snapshot")
	if err != nil {
		return err
	}
	var values map[string][]byte
	if err = json.Unmarshal(raw, &values); err != nil {
		return err
	}
	allowed := make(map[string]bool, len(cp.RecoveredUsers))
	for _, name := range cp.RecoveredUsers {
		allowed[name] = true
	}
	credentials := map[string]accessCredential{}
	if len(values["mysql/access/credentials"]) > 0 {
		if err = json.Unmarshal(values["mysql/access/credentials"], &credentials); err != nil {
			return err
		}
	}
	for name := range credentials {
		if !allowed[name] {
			delete(credentials, name)
		}
	}
	users := []myspec.ProxyUser{}
	if len(values["mysql/access/proxy-users"]) > 0 {
		var source []myspec.ProxyUser
		if err = json.Unmarshal(values["mysql/access/proxy-users"], &source); err != nil {
			return err
		}
		for _, user := range source {
			if allowed[user.Username] {
				users = append(users, user)
			}
		}
	}
	values["mysql/access/credentials"], err = json.Marshal(credentials)
	if err != nil {
		return err
	}
	values["mysql/access/proxy-users"], err = json.Marshal(users)
	if err != nil {
		return err
	}
	for _, key := range backupAccessKeys {
		v, ok := values[key]
		if !ok {
			return engine.Invalid("MySQL account snapshot is incomplete")
		}
		if len(v) == 0 {
			if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
				return err
			}
		} else if err = rc.Secrets.SetClusterSecret(ctx, rc.Cluster.Ref(), key, v); err != nil {
			return err
		}
	}
	return nil
}
func deleteBackupAccess(ctx context.Context, secrets engine.SecretStore, b domain.Backup) error {
	if b.RecoveryIdentity == "" || secrets == nil {
		return nil
	}
	err := secrets.DeleteClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "mysql/access/snapshot")
	if missing(err) {
		return nil
	}
	return err
}

func backupRecoveryUsers(ctx context.Context, rc *engine.Context, b *domain.Backup) ([]myspec.RecoveryUser, error) {
	raw, err := rc.Secrets.GetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "mysql/access/snapshot")
	if err != nil {
		return nil, err
	}
	var values map[string][]byte
	if err = json.Unmarshal(raw, &values); err != nil {
		return nil, err
	}
	credentials := map[string]accessCredential{}
	if len(values["mysql/access/credentials"]) > 0 {
		if err = json.Unmarshal(values["mysql/access/credentials"], &credentials); err != nil {
			return nil, err
		}
	}
	if len(values["mysql/access/proxy-users"]) > 0 {
		var users []myspec.ProxyUser
		if err = json.Unmarshal(values["mysql/access/proxy-users"], &users); err != nil {
			return nil, err
		}
		for _, user := range users {
			if credentials[user.Username].Current == "" {
				credentials[user.Username] = accessCredential{Current: user.Password}
			}
		}
	}
	users := make([]myspec.RecoveryUser, 0, len(credentials))
	for name, value := range credentials {
		user := myspec.RecoveryUser{Name: name, Current: value.Current, Secondary: value.Secondary}
		if err = user.Validate(); err != nil {
			return nil, err
		}
		users = append(users, user)
	}
	sort.Slice(users, func(i, j int) bool { return users[i].Name < users[j].Name })
	return users, nil
}

const backupManifestKey = "mysql/backup/manifest"

func sealBackupManifest(ctx context.Context, rc *engine.Context, b *domain.Backup, digest string, size int64) error {
	if b.MySQL == nil || len(b.MySQL.ObjectPaths) != 1 || !myspec.ValidDigest(digest) || size <= 0 || b.RecoveryIdentity == "" {
		return engine.Invalid("backup has no immutable physical object manifest")
	}
	ref := myspec.BackupManifestRef{ObjectPrefix: b.MySQL.ObjectPaths[0], Digest: digest, Size: size}
	scope := domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}
	raw, err := rc.Secrets.GetClusterSecret(ctx, scope, "mysql/access/snapshot")
	if err != nil {
		return err
	}
	var values map[string][]byte
	if err = json.Unmarshal(raw, &values); err != nil {
		return err
	}
	encoded, err := json.Marshal(ref)
	if err != nil {
		return err
	}
	if previous, ok := values[backupManifestKey]; ok && string(previous) != string(encoded) {
		return engine.Invalid("sealed physical backup manifest changed")
	}
	values[backupManifestKey] = encoded
	raw, err = json.Marshal(values)
	if err != nil {
		return err
	}
	return rc.Secrets.SetClusterSecret(ctx, scope, "mysql/access/snapshot", raw)
}

func backupManifestRefs(ctx context.Context, rc *engine.Context, items []domain.Backup, prefixes []string) ([]myspec.BackupManifestRef, error) {
	refs := make([]myspec.BackupManifestRef, 0, len(prefixes))
	for _, prefix := range prefixes {
		var backup *domain.Backup
		for i := range items {
			item := &items[i]
			if item.Phase == domain.PhaseSucceeded && item.MySQL != nil && len(item.MySQL.ObjectPaths) == 1 && item.MySQL.ObjectPaths[0] == prefix {
				backup = item
				break
			}
		}
		if backup == nil || backup.RecoveryIdentity == "" {
			return nil, engine.Invalid("backup lacks sealed physical object provenance; take a verified full backup")
		}
		raw, err := rc.Secrets.GetClusterSecret(ctx, domain.ClusterRef{Namespace: backup.Namespace, Name: backup.RecoveryIdentity}, "mysql/access/snapshot")
		if err != nil {
			return nil, err
		}
		var values map[string][]byte
		if err = json.Unmarshal(raw, &values); err != nil {
			return nil, err
		}
		var ref myspec.BackupManifestRef
		if err = json.Unmarshal(values[backupManifestKey], &ref); err != nil || ref.ObjectPrefix != prefix || !myspec.ValidDigest(ref.Digest) || ref.Size <= 0 {
			return nil, engine.Invalid("backup lacks sealed physical object provenance; take a verified full backup")
		}
		refs = append(refs, ref)
	}
	return refs, nil
}
