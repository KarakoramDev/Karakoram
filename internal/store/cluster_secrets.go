// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"time"
)

func (s *Store) sealClusterValues(cluster string, values map[string][]byte) (map[string]string, error) {
	sealed := make(map[string]string, len(values))
	for key, value := range values {
		ciphertext, err := s.sealer.SealFor("cluster/"+cluster+"/"+key, value)
		if err != nil {
			return nil, err
		}
		sealed[key] = ciphertext
	}
	return sealed, nil
}
func writeClusterValues(ctx context.Context, tx *sql.Tx, cluster string, sealed map[string]string) error {
	for key, ciphertext := range sealed {
		if err := put(ctx, tx, "secret", cluster, key, struct{}{}, ciphertext, "", "", false); err != nil {
			return err
		}
	}
	return nil
}

// CreateClusterWithSecrets atomically inserts desired state and its write-only
// secret inputs. A duplicate create cannot replace the existing cluster's keys.
func (s *Store) CreateClusterWithSecrets(ctx context.Context, c *domain.Cluster, values map[string][]byte) error {
	if len(values) == 0 {
		return s.CreateCluster(ctx, c)
	}
	if err := domain.ValidateClusterName(c.Name); err != nil {
		return err
	}
	c.Generation = 1
	c.CreatedAt = time.Now().UTC()
	c.UpdatedAt = c.CreatedAt
	if c.Status.Phase == "" {
		c.Status.Phase = domain.PhaseCreating
	}
	spec, err := json.Marshal(c.Spec)
	if err != nil {
		return err
	}
	status, err := json.Marshal(c.Status)
	if err != nil {
		return err
	}
	opts, err := json.Marshal(c.DeleteOptions)
	if err != nil {
		return err
	}
	internal := c.Internal
	if len(internal) == 0 {
		internal = json.RawMessage(`{}`)
	}
	if !json.Valid(internal) {
		return errors.New("invalid provider internal JSON")
	}
	sealed, err := s.sealClusterValues(c.Name, values)
	if err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "INSERT INTO clusters(name,spec,status,internal,generation,deleting,delete_options,created,updated) VALUES(?,?,?,?,?,?,?,?,?)", c.Name, spec, status, []byte(internal), c.Generation, c.Deleting, opts, c.CreatedAt.UnixNano(), c.UpdatedAt.UnixNano()); err != nil {
		return typed(err)
	}
	if err = writeClusterValues(ctx, tx, c.Name, sealed); err != nil {
		return err
	}
	return tx.Commit()
}

// UpdateClusterSpecWithSecrets atomically updates desired state and related
// certificate/key inputs. Changed secret material advances the generation even
// when the public spec is unchanged; repeating identical inputs remains idempotent.
func (s *Store) UpdateClusterSpecWithSecrets(ctx context.Context, name string, spec domain.ClusterSpec, values map[string][]byte) error {
	if len(values) == 0 {
		return s.UpdateClusterSpec(ctx, name, spec)
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	sealed, err := s.sealClusterValues(name, values)
	if err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	changed := false
	for key, value := range values {
		var ciphertext string
		err := tx.QueryRowContext(ctx, "SELECT secret FROM records WHERE kind='secret' AND scope=? AND key=?", name, key).Scan(&ciphertext)
		if errors.Is(err, sql.ErrNoRows) {
			changed = true
			continue
		}
		if err != nil {
			return err
		}
		plaintext, err := s.sealer.OpenFor("cluster/"+name+"/"+key, ciphertext)
		if err != nil {
			return err
		}
		if !bytes.Equal(plaintext, value) {
			changed = true
		}
	}
	result, err := tx.ExecContext(ctx, "UPDATE clusters SET generation=generation+CASE WHEN spec<>? OR ? THEN 1 ELSE 0 END,spec=?,updated=? WHERE name=?", data, changed, data, time.Now().UTC().UnixNano(), name)
	if err = affected(result, err); err != nil {
		return err
	}
	if err = writeClusterValues(ctx, tx, name, sealed); err != nil {
		return err
	}
	return tx.Commit()
}
