// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// CreateCluster inserts a new cluster.
func (s *Store) CreateCluster(ctx context.Context, c *domain.Cluster) error {
	if err := domain.ValidateClusterName(c.Name); err != nil {
		return err
	}
	c.Namespace = domain.NormalizedNamespace(c.Namespace)
	if err := domain.ValidateNamespaceName(c.Namespace); err != nil {
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
	opts, _ := json.Marshal(c.DeleteOptions)
	internal := c.Internal
	if len(internal) == 0 {
		internal = json.RawMessage(`{}`)
	}
	if !json.Valid(internal) {
		return errors.New("invalid provider internal JSON")
	}
	_, err = s.writer.ExecContext(ctx, "INSERT INTO clusters(namespace,name,spec,status,internal,generation,deleting,delete_options,created,updated) VALUES(?,?,?,?,?,?,?,?,?,?)", c.Namespace, c.Name, spec, status, []byte(internal), c.Generation, c.Deleting, opts, c.CreatedAt.UnixNano(), c.UpdatedAt.UnixNano())
	return typed(err)
}

type scanner interface{ Scan(...any) error }

func scanCluster(row scanner) (*domain.Cluster, error) {
	var c domain.Cluster
	var spec, status, internal, opts []byte
	var created, updated int64
	err := row.Scan(&c.Namespace, &c.Name, &spec, &status, &internal, &c.Generation, &c.Deleting, &opts, &created, &updated)
	if err != nil {
		return nil, typed(err)
	}
	for _, pair := range []struct {
		data []byte
		out  any
	}{{spec, &c.Spec}, {status, &c.Status}, {opts, &c.DeleteOptions}} {
		if err = json.Unmarshal(pair.data, pair.out); err != nil {
			return nil, err
		}
	}
	c.Internal = internal
	c.CreatedAt = time.Unix(0, created).UTC()
	c.UpdatedAt = time.Unix(0, updated).UTC()
	return &c, nil
}

const clusterColumns = "namespace,name,spec,status,internal,generation,deleting,delete_options,created,updated"

// GetCluster loads cluster.
func (s *Store) GetCluster(ctx context.Context, ref domain.ClusterRef) (*domain.Cluster, error) {
	return scanCluster(s.reader.QueryRowContext(ctx, "SELECT "+clusterColumns+" FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name))
}

// ListClusters returns ordered clusters.
func (s *Store) ListClusters(ctx context.Context, namespace string) ([]domain.Cluster, error) {
	query := "SELECT " + clusterColumns + " FROM clusters"
	var args []any
	if namespace != "" {
		query += " WHERE namespace=?"
		args = append(args, namespace)
	}
	rows, err := s.reader.QueryContext(ctx, query+" ORDER BY namespace,name", args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := make([]domain.Cluster, 0)
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			return nil, err
		}
		out = append(out, *c)
	}
	return out, rows.Err()
}

// UpdateClusterSpec persists changes to cluster spec.
func (s *Store) UpdateClusterSpec(ctx context.Context, ref domain.ClusterRef, spec domain.ClusterSpec) error {
	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	result, err := s.writer.ExecContext(ctx, "UPDATE clusters SET generation=generation+CASE WHEN spec<>? THEN 1 ELSE 0 END,spec=?,updated=? WHERE namespace=? AND name=?", data, data, time.Now().UTC().UnixNano(), domain.NormalizedNamespace(ref.Namespace), ref.Name)
	return affected(result, err)
}
func affected(result sql.Result, err error) error {
	if err != nil {
		return typed(err)
	}
	n, err := result.RowsAffected()
	if err == nil && n == 0 {
		return ErrNotFound
	}
	return err
}

// UpdateClusterState never overwrites concurrent spec edits or deletion flags.
func (s *Store) UpdateClusterState(ctx context.Context, c *domain.Cluster) error {
	return updateClusterState(ctx, s.writer, c)
}

func updateClusterState(ctx context.Context, db executor, c *domain.Cluster) error {
	status, err := json.Marshal(c.Status)
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
	r, err := db.ExecContext(ctx, "UPDATE clusters SET status=?,internal=?,updated=? WHERE namespace=? AND name=?", status, []byte(internal), time.Now().UTC().UnixNano(), domain.NormalizedNamespace(c.Namespace), c.Name)
	return affected(r, err)
}

// MarkClusterDeleting sets deletion intent and retires work before cleanup starts.
func (s *Store) MarkClusterDeleting(ctx context.Context, ref domain.ClusterRef, opts domain.DeleteOptions) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	data, _ := json.Marshal(opts)
	r, err := tx.ExecContext(ctx, "UPDATE clusters SET deleting=1,delete_options=?,updated=? WHERE namespace=? AND name=?", data, time.Now().UTC().UnixNano(), domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err = affected(r, err); err != nil {
		return err
	}
	if err = retireOperations(ctx, tx, ref.Key(), "Cluster deletion requested"); err != nil {
		return err
	}
	return tx.Commit()
}

// SuspendCluster retains state and suspends cluster.
func (s *Store) SuspendCluster(ctx context.Context, c *domain.Cluster) error {
	c.Status.Phase = domain.PhaseSuspended
	status, err := json.Marshal(c.Status)
	if err != nil {
		return err
	}
	r, err := s.writer.ExecContext(ctx, "UPDATE clusters SET deleting=0,status=?,internal=?,updated=? WHERE namespace=? AND name=?", status, []byte(c.Internal), time.Now().UTC().UnixNano(), domain.NormalizedNamespace(c.Namespace), c.Name)
	return affected(r, err)
}

// ResumeCluster increments generation and resumes cluster.
func (s *Store) ResumeCluster(ctx context.Context, ref domain.ClusterRef) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	c, err := scanCluster(tx.QueryRowContext(ctx, "SELECT "+clusterColumns+" FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name))
	if err != nil {
		return err
	}
	if c.Status.Phase != domain.PhaseSuspended {
		return ErrConflict
	}
	c.Status.Phase = domain.PhaseCreating
	data, _ := json.Marshal(c.Status)
	_, err = tx.ExecContext(ctx, "UPDATE clusters SET status=?,generation=generation+1,deleting=0,updated=? WHERE namespace=? AND name=?", data, time.Now().UTC().UnixNano(), domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteCluster removes cluster and atomically fails unfinished operations so
// recreating its name cannot resume work belonging to the removed cluster.
func (s *Store) DeleteCluster(ctx context.Context, ref domain.ClusterRef) error {
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = retireOperations(ctx, tx, ref.Key(), "Cluster deleted"); err != nil {
		return err
	}
	r, err := tx.ExecContext(ctx, "DELETE FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err = affected(r, err); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "DELETE FROM records WHERE scope=? AND kind IN ('secret','agent-token','volume')", ref.Key())
	if err != nil {
		return err
	}
	return tx.Commit()
}
