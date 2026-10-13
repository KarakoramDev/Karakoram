// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func (s *Store) GetNamespace(ctx context.Context, name string) (*domain.Namespace, error) {
	return getNamespace(ctx, s.reader, name)
}

// GetNamespaceForUpdate reads through the same connection as lifecycle writes.
func (s *Store) GetNamespaceForUpdate(ctx context.Context, name string) (*domain.Namespace, error) {
	return getNamespace(ctx, s.writer, name)
}

func getNamespace(ctx context.Context, db *sql.DB, name string) (*domain.Namespace, error) {
	var data []byte
	if err := db.QueryRowContext(ctx, "SELECT data FROM namespaces WHERE name=?", name).Scan(&data); err != nil {
		return nil, typed(err)
	}
	var n domain.Namespace
	if err := json.Unmarshal(data, &n); err != nil {
		return nil, err
	}
	return &n, nil
}
func (s *Store) ListNamespaces(ctx context.Context) ([]domain.Namespace, error) {
	return listNamespaces(ctx, s.reader)
}

// ListNamespacesForUpdate reads authoritative inventory for registration and startup.
func (s *Store) ListNamespacesForUpdate(ctx context.Context) ([]domain.Namespace, error) {
	return listNamespaces(ctx, s.writer)
}

func listNamespaces(ctx context.Context, db *sql.DB) ([]domain.Namespace, error) {
	rows, err := db.QueryContext(ctx, "SELECT data FROM namespaces ORDER BY name")
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.Namespace, 0)
	for rows.Next() {
		var data []byte
		var n domain.Namespace
		if err = rows.Scan(&data); err != nil {
			return nil, err
		}
		if err = json.Unmarshal(data, &n); err != nil {
			return nil, err
		}
		out = append(out, n)
	}
	return out, rows.Err()
}

// ErrStale indicates a snapshot of a removed or replaced namespace registration.
var ErrStale = fmt.Errorf("%w: stale namespace registration", ErrConflict)

func writeNamespace(ctx context.Context, db executor, n *domain.Namespace, create bool) error {
	data, err := json.Marshal(n)
	if err != nil {
		return err
	}
	if create {
		_, err = db.ExecContext(ctx, "INSERT INTO namespaces(name,data,updated_at) VALUES(?,?,?)", n.Name, data, n.UpdatedAt.UTC().Format(time.RFC3339Nano))
		return typed(err)
	}
	result, err := db.ExecContext(ctx, "UPDATE namespaces SET data=?,updated_at=? WHERE name=? AND json_extract(data,'$.createdAt')=? AND (json_extract(data,'$.phase')!='Deleting' OR ?='Deleting')", data, n.UpdatedAt.UTC().Format(time.RFC3339Nano), n.Name, n.CreatedAt.Format(time.RFC3339Nano), n.Phase)
	if err != nil {
		return typed(err)
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrStale
	}
	return nil
}

// CreateNamespace inserts a registration without replacing an existing record.
func (s *Store) CreateNamespace(ctx context.Context, n *domain.Namespace) error {
	return writeNamespace(ctx, s.writer, n, true)
}

// UpdateNamespace persists only the same registration, preserving deletion intent.
func (s *Store) UpdateNamespace(ctx context.Context, n *domain.Namespace) error {
	return writeNamespace(ctx, s.writer, n, false)
}

// CreateNamespaceCredentials atomically creates a registration and sealed credentials.
func (s *Store) CreateNamespaceCredentials(ctx context.Context, n *domain.Namespace, token string, key []byte) error {
	return s.writeNamespaceCredentials(ctx, n, token, key, true)
}

// UpdateNamespaceCredentials atomically updates credentials for the same registration.
func (s *Store) UpdateNamespaceCredentials(ctx context.Context, n *domain.Namespace, token string, key []byte) error {
	return s.writeNamespaceCredentials(ctx, n, token, key, false)
}

func (s *Store) writeNamespaceCredentials(ctx context.Context, n *domain.Namespace, token string, key []byte, create bool) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = writeNamespace(ctx, tx, n, create); err != nil {
		return err
	}
	for kind, value := range map[string][]byte{"zerops-token": []byte(token), "relay-key": key} {
		if value == nil {
			continue
		}
		sealed, e := s.sealer.SealFor("namespace/"+n.Name+"/"+kind, value)
		if e != nil {
			return e
		}
		if err = put(ctx, tx, "namespace-secret", n.Name, kind, struct{}{}, sealed, "", "", false); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// DeleteNamespace removes the record and all its credentials in one transaction.
func (s *Store) DeleteNamespace(ctx context.Context, n *domain.Namespace) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, "DELETE FROM namespaces WHERE name=? AND json_extract(data,'$.createdAt')=?", n.Name, n.CreatedAt.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrStale
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='namespace-secret' AND scope=?", n.Name); err != nil {
		return err
	}
	return tx.Commit()
}
func (s *Store) namespaceSecret(ctx context.Context, name, kind string) ([]byte, error) {
	var v struct{}
	sealed, _, err := s.get(ctx, "namespace-secret", name, kind, &v)
	if err != nil {
		return nil, err
	}
	return s.sealer.OpenFor("namespace/"+name+"/"+kind, sealed)
}

func (s *Store) namespaceSecretForUpdate(ctx context.Context, name, kind string) ([]byte, error) {
	var sealed string
	if err := s.writer.QueryRowContext(ctx, "SELECT secret FROM records WHERE kind='namespace-secret' AND scope=? AND key=?", name, kind).Scan(&sealed); err != nil {
		return nil, typed(err)
	}
	return s.sealer.OpenFor("namespace/"+name+"/"+kind, sealed)
}
func (s *Store) GetNamespaceToken(ctx context.Context, name string) (string, error) {
	v, err := s.namespaceSecret(ctx, name, "zerops-token")
	return string(v), err
}

// GetNamespaceTokenForUpdate reads current credentials for lifecycle operations.
func (s *Store) GetNamespaceTokenForUpdate(ctx context.Context, name string) (string, error) {
	v, err := s.namespaceSecretForUpdate(ctx, name, "zerops-token")
	return string(v), err
}
func (s *Store) DeleteNamespaceToken(ctx context.Context, n *domain.Namespace) error {
	result, err := s.writer.ExecContext(ctx, "DELETE FROM records WHERE kind='namespace-secret' AND scope=? AND key='zerops-token' AND EXISTS (SELECT 1 FROM namespaces WHERE name=? AND json_extract(data,'$.createdAt')=?)", n.Name, n.Name, n.CreatedAt.Format(time.RFC3339Nano))
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrStale
	}
	return nil
}
func (s *Store) GetNamespaceRelayKey(ctx context.Context, name string) ([]byte, error) {
	return s.namespaceSecret(ctx, name, "relay-key")
}

// GetNamespaceRelayKeyForUpdate reads current enrollment for relay provisioning.
func (s *Store) GetNamespaceRelayKeyForUpdate(ctx context.Context, name string) ([]byte, error) {
	return s.namespaceSecretForUpdate(ctx, name, "relay-key")
}

// RelayPKI is installation-wide trust material, always sealed as a setting.
type RelayPKI struct{ CACert, CAKey, HubCert, HubKey []byte }

func (s *Store) GetRelayPKI(ctx context.Context) (*RelayPKI, error) {
	raw, err := s.GetSetting(ctx, "server/relay-pki")
	if err != nil {
		return nil, err
	}
	var sealed string
	if err = json.Unmarshal(raw, &sealed); err != nil {
		return nil, err
	}
	var p RelayPKI
	if err = s.unseal("server/relay-pki", sealed, &p); err != nil {
		return nil, err
	}
	return &p, nil
}
func (s *Store) PutRelayPKI(ctx context.Context, p *RelayPKI) error {
	sealed, err := s.seal("server/relay-pki", p)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(sealed)
	if err != nil {
		return err
	}
	return s.SetSetting(ctx, "server/relay-pki", raw)
}
