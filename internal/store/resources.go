// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

// CreateBackup inserts a new backup.
func (s *Store) CreateBackup(ctx context.Context, v *domain.Backup) error {
	if err := (domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}).Validate(); err != nil {
		return err
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	if v.Phase == "" {
		v.Phase = domain.PhasePending
	}
	return put(ctx, s.writer, "backup", domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}.Key(), v.Name, v, "", "", string(v.Phase), true)
}

// UpdateBackup persists changes to backup.
func (s *Store) UpdateBackup(ctx context.Context, v *domain.Backup) error {
	if err := (domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}).Validate(); err != nil {
		return err
	}
	return s.update(ctx, "backup", domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}.Key(), v.Name, v, "", "", string(v.Phase))
}

// GetBackup loads backup.
func (s *Store) GetBackup(ctx context.Context, ref domain.ClusterRef, name string) (*domain.Backup, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	cluster := ref.Key()
	var v domain.Backup
	_, _, err := s.get(ctx, "backup", cluster, name, &v)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// ListBackups returns ordered backups.
func (s *Store) ListBackups(ctx context.Context, ref domain.ClusterRef) ([]domain.Backup, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	return listClusterRecords[domain.Backup](ctx, s, "backup", ref)
}

// DeleteBackup removes backup.
func (s *Store) DeleteBackup(ctx context.Context, ref domain.ClusterRef, name string) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	cluster := ref.Key()
	return s.remove(ctx, "backup", cluster, name)
}

// CreateRestore inserts a new restore.
func (s *Store) CreateRestore(ctx context.Context, v *domain.Restore) error {
	if err := (domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}).Validate(); err != nil {
		return err
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	if v.Phase == "" {
		v.Phase = domain.PhasePending
	}
	return put(ctx, s.writer, "restore", domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}.Key(), v.Name, v, "", "", string(v.Phase), true)
}

// UpdateRestore persists changes to restore.
func (s *Store) UpdateRestore(ctx context.Context, v *domain.Restore) error {
	if err := (domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}).Validate(); err != nil {
		return err
	}
	return s.update(ctx, "restore", domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}.Key(), v.Name, v, "", "", string(v.Phase))
}

// GetRestore loads restore.
func (s *Store) GetRestore(ctx context.Context, ref domain.ClusterRef, name string) (*domain.Restore, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	cluster := ref.Key()
	var v domain.Restore
	_, _, err := s.get(ctx, "restore", cluster, name, &v)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// ListRestores returns ordered restores.
func (s *Store) ListRestores(ctx context.Context, ref domain.ClusterRef) ([]domain.Restore, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	return listClusterRecords[domain.Restore](ctx, s, "restore", ref)
}

// DeleteRestore removes restore.
func (s *Store) DeleteRestore(ctx context.Context, ref domain.ClusterRef, name string) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	cluster := ref.Key()
	return s.remove(ctx, "restore", cluster, name)
}

// CreateRetainedVolume inserts a new retained volume.
func (s *Store) CreateRetainedVolume(ctx context.Context, v *domain.RetainedVolume) error {
	if err := (domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}).Validate(); err != nil {
		return err
	}
	return put(ctx, s.writer, "volume", domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}.Key(), v.Member, v, "", "", "", true)
}

// UpdateRetainedVolume persists changes to retained volume.
func (s *Store) UpdateRetainedVolume(ctx context.Context, v *domain.RetainedVolume) error {
	if err := (domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}).Validate(); err != nil {
		return err
	}
	return s.update(ctx, "volume", domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}.Key(), v.Member, v, "", "", "")
}

// GetRetainedVolume loads retained volume.
func (s *Store) GetRetainedVolume(ctx context.Context, ref domain.ClusterRef, name string) (*domain.RetainedVolume, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	cluster := ref.Key()
	var v domain.RetainedVolume
	_, _, err := s.get(ctx, "volume", cluster, name, &v)
	if err != nil {
		return nil, err
	}
	return &v, nil
}

// ListRetainedVolumes returns ordered retained volumes.
func (s *Store) ListRetainedVolumes(ctx context.Context, ref domain.ClusterRef) ([]domain.RetainedVolume, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	return listClusterRecords[domain.RetainedVolume](ctx, s, "volume", ref)
}

// DeleteRetainedVolume removes retained volume.
func (s *Store) DeleteRetainedVolume(ctx context.Context, ref domain.ClusterRef, name string) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	cluster := ref.Key()
	return s.remove(ctx, "volume", cluster, name)
}

// CreateBackupStorage inserts a new backup storage.
func (s *Store) CreateBackupStorage(ctx context.Context, v *domain.BackupStorage) error {
	if v.Name == "" || strings.ContainsAny(v.Name, ":/") {
		return fmt.Errorf("invalid backup storage name: %w", ErrInvalid)
	}
	v.Namespace = domain.NormalizedNamespace(v.Namespace)
	if err := domain.ValidateNamespaceName(v.Namespace); err != nil {
		return err
	}
	sealed, err := s.seal(storageAAD(v.Namespace, v.Name), v.Credentials)
	if err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = namespaceForInsert(ctx, tx, v.Namespace); err != nil {
		return err
	}
	if err = put(ctx, tx, "storage", namespaceScope(v.Namespace), v.Name, v, sealed, "", "", true); err != nil {
		return err
	}
	return tx.Commit()
}

// UpdateBackupStorage persists changes to backup storage.
func (s *Store) UpdateBackupStorage(ctx context.Context, v *domain.BackupStorage) error {
	v.Namespace = domain.NormalizedNamespace(v.Namespace)
	if err := domain.ValidateNamespaceName(v.Namespace); err != nil {
		return err
	}
	sealed, err := s.seal(storageAAD(v.Namespace, v.Name), v.Credentials)
	if err != nil {
		return err
	}
	return s.update(ctx, "storage", namespaceScope(v.Namespace), v.Name, v, sealed, "", "")
}

// GetBackupStorage loads backup storage.
func (s *Store) GetBackupStorage(ctx context.Context, namespace, name string) (*domain.BackupStorage, error) {
	var v domain.BackupStorage
	sealed, _, err := s.get(ctx, "storage", namespaceScope(namespace), name, &v)
	if err != nil {
		return nil, err
	}
	if err = s.unseal(storageAAD(namespace, name), sealed, &v.Credentials); err != nil {
		return nil, err
	}
	return &v, nil
}

// ListBackupStorages intentionally omits credentials.
func (s *Store) ListBackupStorages(ctx context.Context, namespace string) ([]domain.BackupStorage, error) {
	scope := "*"
	if namespace != "" {
		scope = namespaceScope(namespace)
	}
	return list[domain.BackupStorage](ctx, s, "storage", scope)
}

// DeleteBackupStorage removes backup storage.
func (s *Store) DeleteBackupStorage(ctx context.Context, namespace, name string) error {
	return s.remove(ctx, "storage", namespaceScope(namespace), name)
}

// CreateAccount inserts a new account.
func (s *Store) CreateAccount(ctx context.Context, v *domain.Account) error {
	if err := domain.ValidateAccountName(v.Name); err != nil {
		return err
	}
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	return put(ctx, s.writer, "account", "", v.Name, v, "", v.PasswordHash, "", true)
}

// UpdateAccount persists changes to account.
func (s *Store) UpdateAccount(ctx context.Context, v *domain.Account) error {
	return s.update(ctx, "account", "", v.Name, v, "", v.PasswordHash, "")
}

// GetAccount loads account.
func (s *Store) GetAccount(ctx context.Context, key string) (*domain.Account, error) {
	var v domain.Account
	_, hash, err := s.get(ctx, "account", "", key, &v)
	if err != nil {
		return nil, err
	}
	v.PasswordHash = hash
	return &v, nil
}

// ListAccounts returns ordered accounts.
func (s *Store) ListAccounts(ctx context.Context) ([]domain.Account, error) {
	return list[domain.Account](ctx, s, "account", "")
}

// DeleteAccount removes account.
func (s *Store) DeleteAccount(ctx context.Context, key string) error {
	return s.remove(ctx, "account", "", key)
}

// CreateApiToken inserts a new api token.
func (s *Store) CreateApiToken(ctx context.Context, v *domain.ApiToken) error {
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	return put(ctx, s.writer, "api-token", "", v.ID, v, "", v.Hash, "", true)
}

// UpdateApiToken persists changes to api token.
func (s *Store) UpdateApiToken(ctx context.Context, v *domain.ApiToken) error {
	return s.update(ctx, "api-token", "", v.ID, v, "", v.Hash, "")
}

// GetApiToken loads api token.
func (s *Store) GetApiToken(ctx context.Context, key string) (*domain.ApiToken, error) {
	var v domain.ApiToken
	_, hash, err := s.get(ctx, "api-token", "", key, &v)
	if err != nil {
		return nil, err
	}
	v.Hash = hash
	return &v, nil
}

// ListApiTokens returns ordered api tokens.
func (s *Store) ListApiTokens(ctx context.Context) ([]domain.ApiToken, error) {
	return list[domain.ApiToken](ctx, s, "api-token", "")
}

// DeleteApiToken removes api token.
func (s *Store) DeleteApiToken(ctx context.Context, key string) error {
	return s.remove(ctx, "api-token", "", key)
}

// FindApiTokenByHash looks up api token by hash.
func (s *Store) FindApiTokenByHash(ctx context.Context, hash string) (*domain.ApiToken, error) {
	var key string
	err := s.reader.QueryRowContext(ctx, "SELECT key FROM records WHERE kind=? AND hash=?", "api-token", hash).Scan(&key)
	if err != nil {
		return nil, typed(err)
	}
	return s.GetApiToken(ctx, key)
}

// CreateSession inserts a new session.
func (s *Store) CreateSession(ctx context.Context, v *domain.Session) error {
	if v.CreatedAt.IsZero() {
		v.CreatedAt = time.Now().UTC()
	}
	return put(ctx, s.writer, "session", "", v.ID, v, "", v.RefreshHash, "", true)
}

// UpdateSession persists changes to session.
func (s *Store) UpdateSession(ctx context.Context, v *domain.Session) error {
	return s.update(ctx, "session", "", v.ID, v, "", v.RefreshHash, "")
}

// GetSession loads session.
func (s *Store) GetSession(ctx context.Context, key string) (*domain.Session, error) {
	var v domain.Session
	_, hash, err := s.get(ctx, "session", "", key, &v)
	if err != nil {
		return nil, err
	}
	v.RefreshHash = hash
	return &v, nil
}

// ListSessions returns ordered sessions.
func (s *Store) ListSessions(ctx context.Context) ([]domain.Session, error) {
	return list[domain.Session](ctx, s, "session", "")
}

// DeleteSession removes session.
func (s *Store) DeleteSession(ctx context.Context, key string) error {
	return s.remove(ctx, "session", "", key)
}

// FindSessionByHash looks up session by hash.
func (s *Store) FindSessionByHash(ctx context.Context, hash string) (*domain.Session, error) {
	var key string
	err := s.reader.QueryRowContext(ctx, "SELECT key FROM records WHERE kind=? AND hash=?", "session", hash).Scan(&key)
	if err != nil {
		return nil, typed(err)
	}
	return s.GetSession(ctx, key)
}

// TouchApiToken updates usage metadata for api token.
func (s *Store) TouchApiToken(ctx context.Context, id string, at time.Time) error {
	v, err := s.GetApiToken(ctx, id)
	if err != nil {
		return err
	}
	v.LastUsedAt = &at
	return s.UpdateApiToken(ctx, v)
}

// RotateSession atomically consumes the old refresh hash; replay fails.
func (s *Store) RotateSession(ctx context.Context, id, oldHash, newHash string, expiry time.Time) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var data []byte
	err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='session' AND key=? AND hash=?", id, oldHash).Scan(&data)
	if err != nil {
		return typed(err)
	}
	var v domain.Session
	if err = json.Unmarshal(data, &v); err != nil {
		return err
	}
	if !v.ExpiresAt.After(time.Now()) {
		return ErrNotFound
	}
	v.ExpiresAt = expiry
	v.RefreshHash = newHash
	if err = put(ctx, tx, "session", "", id, &v, "", newHash, "", false); err != nil {
		return err
	}
	return tx.Commit()
}

// SetClusterSecret persists cluster secret.
func (s *Store) SetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string, value []byte) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	cluster := ref.Key()
	sealed, err := s.sealer.SealFor("cluster/"+cluster+"/"+key, value)
	if err != nil {
		return err
	}
	return put(ctx, s.writer, "secret", cluster, key, struct{}{}, sealed, "", "", false)
}

// SetClusterSecrets persists several cluster secrets in one transaction, so
// related material (e.g. a certificate and its key) is never stored half-way.
func (s *Store) SetClusterSecrets(ctx context.Context, ref domain.ClusterRef, values map[string][]byte) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	cluster := ref.Key()
	sealed := make(map[string]string, len(values))
	for key, value := range values {
		v, err := s.sealer.SealFor("cluster/"+cluster+"/"+key, value)
		if err != nil {
			return err
		}
		sealed[key] = v
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	for key, v := range sealed {
		if err := put(ctx, tx, "secret", cluster, key, struct{}{}, v, "", "", false); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// GetClusterSecret loads cluster secret.
func (s *Store) GetClusterSecret(ctx context.Context, ref domain.ClusterRef, key string) ([]byte, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	cluster := ref.Key()
	var v struct{}
	sealed, _, err := s.get(ctx, "secret", cluster, key, &v)
	if err != nil {
		return nil, err
	}
	return s.sealer.OpenFor("cluster/"+cluster+"/"+key, sealed)
}

// DeleteClusterSecret removes cluster secret.
func (s *Store) DeleteClusterSecret(ctx context.Context, ref domain.ClusterRef, key string) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	cluster := ref.Key()
	return s.remove(ctx, "secret", cluster, key)
}

// SetAgentToken persists agent token.
func (s *Store) SetAgentToken(ctx context.Context, v *domain.AgentToken) error {
	if err := (domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}).Validate(); err != nil {
		return err
	}
	if v.Token != "" {
		v.Hash = secrets.Hash(v.Token)
	}
	sealed, err := s.seal("agent/"+domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}.Key()+"/"+v.Member, v.Token)
	if err != nil {
		return err
	}
	return put(ctx, s.writer, "agent-token", domain.ClusterRef{Namespace: v.Namespace, Name: v.Cluster}.Key(), v.Member, v, sealed, v.Hash, "", false)
}

// GetAgentToken loads agent token.
func (s *Store) GetAgentToken(ctx context.Context, ref domain.ClusterRef, member string) (*domain.AgentToken, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
	cluster := ref.Key()
	var v domain.AgentToken
	sealed, hash, err := s.get(ctx, "agent-token", cluster, member, &v)
	if err != nil {
		return nil, err
	}
	v.Hash = hash
	if err = s.unseal("agent/"+cluster+"/"+member, sealed, &v.Token); err != nil {
		return nil, err
	}
	return &v, nil
}

// FindAgentTokenByHash looks up agent token by hash.
func (s *Store) FindAgentTokenByHash(ctx context.Context, hash string) (*domain.AgentToken, error) {
	var cluster, member string
	err := s.reader.QueryRowContext(ctx, "SELECT scope,key FROM records WHERE kind='agent-token' AND hash=?", hash).Scan(&cluster, &member)
	if err != nil {
		return nil, typed(err)
	}
	return s.GetAgentToken(ctx, domain.ParseClusterKey(cluster), member)
}

// DeleteAgentToken removes agent token.
func (s *Store) DeleteAgentToken(ctx context.Context, ref domain.ClusterRef, member string) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	cluster := ref.Key()
	return s.remove(ctx, "agent-token", cluster, member)
}

// SetSetting persists setting.
func (s *Store) SetSetting(ctx context.Context, key string, value json.RawMessage) error {
	if !json.Valid(value) {
		return errors.New("setting must contain valid JSON")
	}
	return put(ctx, s.writer, "setting", "", key, value, "", "", "", false)
}

// GetSetting loads setting.
func (s *Store) GetSetting(ctx context.Context, key string) (json.RawMessage, error) {
	var value json.RawMessage
	_, _, err := s.get(ctx, "setting", "", key, &value)
	return value, err
}

// DeleteSetting removes setting.
func (s *Store) DeleteSetting(ctx context.Context, key string) error {
	return s.remove(ctx, "setting", "", key)
}

// ListSettings returns ordered settings.
func (s *Store) ListSettings(ctx context.Context) (map[string]json.RawMessage, error) {
	rows, err := s.reader.QueryContext(ctx, "SELECT key,data FROM records WHERE kind='setting' ORDER BY key")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]json.RawMessage{}
	for rows.Next() {
		var key string
		var data []byte
		if err = rows.Scan(&key, &data); err != nil {
			return nil, err
		}
		out[key] = data
	}
	return out, rows.Err()
}

// ListPolicyRules returns ordered policy rules.
func (s *Store) ListPolicyRules(ctx context.Context) ([]domain.PolicyRule, error) {
	rows, err := s.reader.QueryContext(ctx, "SELECT data FROM policies ORDER BY position")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.PolicyRule, 0)
	for rows.Next() {
		var data []byte
		var v domain.PolicyRule
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &v); err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, rows.Err()
}

// ReplacePolicyRules atomically replaces policy rules.
func (s *Store) ReplacePolicyRules(ctx context.Context, rules []domain.PolicyRule) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "DELETE FROM policies"); err != nil {
		return err
	}
	for i, rule := range rules {
		data, err := json.Marshal(rule)
		if err != nil {
			return err
		}
		if _, err = tx.ExecContext(ctx, "INSERT INTO policies(position,data) VALUES(?,?)", i, data); err != nil {
			return err
		}
	}
	return tx.Commit()
}
