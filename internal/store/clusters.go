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
	c.RequestedBy = Actor(ctx)
	c.CreatedAt = time.Now().UTC()
	c.UpdatedAt = c.CreatedAt
	if c.Status.Phase == "" {
		c.Status.Phase = domain.PhaseCreating
	}
	if c.Status.Phase == domain.PhaseCreating && c.Status.Message == "" {
		c.Status.Message = "Creating: queued platform provisioning"
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
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = namespaceForInsert(ctx, tx, c.Namespace); err != nil {
		return err
	}
	if _, err = tx.ExecContext(ctx, "INSERT INTO clusters(namespace,name,spec,status,internal,generation,deleting,delete_options,created,updated,requested_by) VALUES(?,?,?,?,?,?,?,?,?,?,?)", c.Namespace, c.Name, spec, status, []byte(internal), c.Generation, c.Deleting, opts, c.CreatedAt.UnixNano(), c.UpdatedAt.UnixNano(), c.RequestedBy); err != nil {
		return typed(err)
	}
	if err = acceptClusterOperation(ctx, tx, c.Ref()); err != nil {
		return err
	}
	return tx.Commit()
}

type scanner interface{ Scan(...any) error }

func scanCluster(row scanner) (*domain.Cluster, error) {
	var c domain.Cluster
	var spec, status, internal, opts []byte
	var created, updated int64
	err := row.Scan(&c.Namespace, &c.Name, &spec, &status, &internal, &c.Generation, &c.Deleting, &opts, &created, &updated, &c.RequestedBy)
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

const clusterColumns = "namespace,name,spec,status,internal,generation,deleting,delete_options,created,updated,requested_by"

// GetCluster loads cluster.
func (s *Store) GetCluster(ctx context.Context, ref domain.ClusterRef) (*domain.Cluster, error) {
	if err := ref.Validate(); err != nil {
		return nil, err
	}
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
	if err := ref.Validate(); err != nil {
		return err
	}
	data, err := json.Marshal(spec)
	if err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	result, err := tx.ExecContext(ctx, "UPDATE clusters SET generation=generation+CASE WHEN spec<>? THEN 1 ELSE 0 END,requested_by=CASE WHEN spec<>? THEN ? ELSE requested_by END,spec=?,updated=? WHERE namespace=? AND name=?", data, data, Actor(ctx), data, time.Now().UTC().UnixNano(), domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err = affected(result, err); err != nil {
		return err
	}
	if err = acceptClusterOperation(ctx, tx, ref); err != nil {
		return err
	}
	return tx.Commit()
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
	if err := c.Ref().Validate(); err != nil {
		return err
	}
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
	if err := ref.Validate(); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	var previous domain.Phase
	if err = tx.QueryRowContext(ctx, "SELECT json_extract(status,'$.phase') FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name).Scan(&previous); err != nil {
		return typed(err)
	}
	data, _ := json.Marshal(opts)
	r, err := tx.ExecContext(ctx, "UPDATE clusters SET generation=generation+CASE WHEN deleting=0 OR delete_options<>? THEN 1 ELSE 0 END,requested_by=CASE WHEN deleting=0 OR delete_options<>? THEN ? ELSE requested_by END,deleting=1,delete_options=?,status=json_set(status,'$.phase','Deleting','$.message','Deleting: queued resource cleanup'),updated=? WHERE namespace=? AND name=?", data, data, Actor(ctx), data, time.Now().UTC().UnixNano(), domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err = affected(r, err); err != nil {
		return err
	}
	if err = retireOperations(ctx, tx, ref.Key(), "Cluster deletion requested"); err != nil {
		return err
	}
	if err = acceptClusterOperation(ctx, tx, ref); err != nil {
		return err
	}
	if previous != domain.PhaseDeleting {
		event := domain.Event{Namespace: domain.NormalizedNamespace(ref.Namespace), Cluster: ref.Name, Type: "Normal", Reason: "PhaseChanged", Message: string(previous) + " → Deleting"}
		if op, ok := ctx.Value(operationContextKey{}).(*domain.Operation); ok {
			event.Operation = op.ID
		}
		if err = appendEvent(ctx, tx, &event); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SuspendCluster retains state and suspends cluster.
func (s *Store) SuspendCluster(ctx context.Context, c *domain.Cluster) error {
	if err := c.Ref().Validate(); err != nil {
		return err
	}
	c.Status.Phase = domain.PhaseSuspended
	c.Status.ObservedGeneration = c.Generation
	c.Status.Message = "Runtime suspended; retained volumes are available for resume"
	for i := range c.Status.Members {
		member := &c.Status.Members[i]
		member.State, member.Health = "SUSPENDED", false
		member.Agent.Healthy, member.Agent.Message = false, "Runtime suspended"
		member.PublicHost = ""
	}
	for i := range c.Status.Conditions {
		if c.Status.Conditions[i].Status != "Unknown" || c.Status.Conditions[i].Reason != "Suspended" {
			c.Status.Conditions[i].LastTransitionTime = time.Now().UTC()
		}
		c.Status.Conditions[i].Status = "Unknown"
		c.Status.Conditions[i].Reason = "Suspended"
		c.Status.Conditions[i].Message = "Runtime observations are historical; cluster is suspended"
	}
	status, err := json.Marshal(c.Status)
	if err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	r, err := tx.ExecContext(ctx, "UPDATE clusters SET deleting=0,status=?,internal=?,updated=? WHERE namespace=? AND name=?", status, []byte(c.Internal), time.Now().UTC().UnixNano(), domain.NormalizedNamespace(c.Namespace), c.Name)
	if err = affected(r, err); err != nil {
		return err
	}
	if err = completeDeletionOperations(ctx, tx, c.Ref(), "Cluster runtime suspended; volumes retained"); err != nil {
		return err
	}
	return tx.Commit()
}

// ResumeCluster increments generation and resumes cluster.
func (s *Store) ResumeCluster(ctx context.Context, ref domain.ClusterRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
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
	c.Status.Message = "Creating: queued runtime resume from retained volumes"
	data, _ := json.Marshal(c.Status)
	_, err = tx.ExecContext(ctx, "UPDATE clusters SET status=?,generation=generation+1,requested_by=?,deleting=0,updated=? WHERE namespace=? AND name=?", data, Actor(ctx), time.Now().UTC().UnixNano(), domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err != nil {
		return err
	}
	if err = acceptClusterOperation(ctx, tx, ref); err != nil {
		return err
	}
	return tx.Commit()
}

// DeleteCluster removes cluster and atomically fails unfinished operations so
// recreating its name cannot resume work belonging to the removed cluster.
func (s *Store) DeleteCluster(ctx context.Context, ref domain.ClusterRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = completeDeletionOperations(ctx, tx, ref, "Cluster deleted"); err != nil {
		return err
	}
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
