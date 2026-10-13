// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"encoding/json"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func (s *Store) GetNamespace(ctx context.Context, name string) (*domain.Namespace, error) {
	var data []byte
	if err := s.reader.QueryRowContext(ctx, "SELECT data FROM namespaces WHERE name=?", name).Scan(&data); err != nil {
		return nil, typed(err)
	}
	var n domain.Namespace
	if err := json.Unmarshal(data, &n); err != nil {
		return nil, err
	}
	return &n, nil
}
func (s *Store) ListNamespaces(ctx context.Context) ([]domain.Namespace, error) {
	rows, err := s.reader.QueryContext(ctx, "SELECT data FROM namespaces ORDER BY name")
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
func putNamespace(ctx context.Context, db executor, n *domain.Namespace) error {
	data, err := json.Marshal(n)
	if err != nil {
		return err
	}
	_, err = db.ExecContext(ctx, "INSERT INTO namespaces(name,data,updated_at) VALUES(?,?,?) ON CONFLICT(name) DO UPDATE SET data=excluded.data,updated_at=excluded.updated_at", n.Name, data, n.UpdatedAt.UTC().Format(time.RFC3339Nano))
	return typed(err)
}
func (s *Store) PutNamespace(ctx context.Context, n *domain.Namespace) error {
	return putNamespace(ctx, s.writer, n)
}

// PutNamespaceCredentials atomically enrolls a record and its sealed credentials.
func (s *Store) PutNamespaceCredentials(ctx context.Context, n *domain.Namespace, token string, key []byte) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = putNamespace(ctx, tx, n); err != nil {
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
func (s *Store) DeleteNamespace(ctx context.Context, name string) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, "DELETE FROM namespaces WHERE name=?", name)
	if err != nil {
		return err
	}
	count, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if count == 0 {
		return ErrNotFound
	}
	if _, err = tx.ExecContext(ctx, "DELETE FROM records WHERE kind='namespace-secret' AND scope=?", name); err != nil {
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
func (s *Store) putNamespaceSecret(ctx context.Context, name, kind string, value []byte) error {
	sealed, err := s.sealer.SealFor("namespace/"+name+"/"+kind, value)
	if err != nil {
		return err
	}
	return put(ctx, s.writer, "namespace-secret", name, kind, struct{}{}, sealed, "", "", false)
}
func (s *Store) GetNamespaceToken(ctx context.Context, name string) (string, error) {
	v, err := s.namespaceSecret(ctx, name, "zerops-token")
	return string(v), err
}
func (s *Store) PutNamespaceToken(ctx context.Context, name, token string) error {
	return s.putNamespaceSecret(ctx, name, "zerops-token", []byte(token))
}
func (s *Store) DeleteNamespaceToken(ctx context.Context, name string) error {
	return s.remove(ctx, "namespace-secret", name, "zerops-token")
}
func (s *Store) GetNamespaceRelayKey(ctx context.Context, name string) ([]byte, error) {
	return s.namespaceSecret(ctx, name, "relay-key")
}
func (s *Store) PutNamespaceRelayKey(ctx context.Context, name string, key []byte) error {
	return s.putNamespaceSecret(ctx, name, "relay-key", key)
}
func (s *Store) DeleteNamespaceRelayKey(ctx context.Context, name string) error {
	return s.remove(ctx, "namespace-secret", name, "relay-key")
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
