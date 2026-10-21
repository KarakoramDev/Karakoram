// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/redact"
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
	if d := c.Spec.DataSource; d != nil {
		if err = admitRestoreBackup(ctx, tx, c.Ref(), domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}, d.Backup, d.PITRTime != nil); err != nil {
			return err
		}
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

// ClusterDataError identifies an unreadable authoritative row. Callers may
// continue managing independently decoded rows but must never adopt or mutate
// the damaged cluster using an invented spec or checkpoint.
type ClusterDataError struct {
	Namespace, Name, Field string
	Err                    error
}

func (e *ClusterDataError) Error() string {
	return fmt.Sprintf("cluster %s/%s has invalid %s state: %v; preserve the database, WAL and master key and restore verified control-plane state before managing this cluster; do not delete state or adopt platform services", e.Namespace, e.Name, e.Field, e.Err)
}

func (e *ClusterDataError) Unwrap() error { return e.Err }

func scanCluster(row scanner) (*domain.Cluster, error) {
	var c domain.Cluster
	var spec, status, internal, opts []byte
	var created, updated int64
	err := row.Scan(&c.Namespace, &c.Name, &spec, &status, &internal, &c.Generation, &c.Deleting, &opts, &created, &updated, &c.RequestedBy)
	if err != nil {
		return nil, typed(err)
	}
	status = redact.DiagnosticsJSON(status)
	for _, pair := range []struct {
		field string
		data  []byte
		out   any
	}{{"spec", spec, &c.Spec}, {"status", status, &c.Status}, {"delete_options", opts, &c.DeleteOptions}} {
		if err = json.Unmarshal(pair.data, pair.out); err != nil {
			return nil, &ClusterDataError{Namespace: c.Namespace, Name: c.Name, Field: pair.field, Err: err}
		}
	}
	if !json.Valid(internal) {
		return nil, &ClusterDataError{Namespace: c.Namespace, Name: c.Name, Field: "internal", Err: errors.New("invalid JSON")}
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

// ListClusters returns ordered, independently decoded clusters. Logical row
// damage returns those valid rows alongside ClusterDataError diagnostics.
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
	var damaged error
	for rows.Next() {
		c, err := scanCluster(rows)
		if err != nil {
			var rowErr *ClusterDataError
			if !errors.As(err, &rowErr) {
				return nil, err
			}
			damaged = errors.Join(damaged, err)
			continue
		}
		out = append(out, *c)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, damaged
}

// UpdateClusterSpec persists changes to cluster spec.
func (s *Store) UpdateClusterSpec(ctx context.Context, ref domain.ClusterRef, spec domain.ClusterSpec) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = checkSpecSnapshot(ctx, tx, ref); err != nil {
		return err
	}
	// Capture under the writer transaction: a prior handler read is not a
	// conditional write snapshot and may have been superseded by another edit.
	before, err := specAuditBefore(ctx, tx, ref)
	if err != nil {
		return err
	}
	data, spec, err := preserveRetryEpoch(ctx, tx, ref, spec)
	if err != nil {
		return err
	}
	ctx, err = admitInitialSpecAmendment(ctx, tx, ref, before, spec, false)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE clusters SET generation=generation+CASE WHEN spec<>? THEN 1 ELSE 0 END,requested_by=CASE WHEN spec<>? THEN ? ELSE requested_by END,spec=?,updated=? WHERE namespace=? AND name=?", data, data, Actor(ctx), data, time.Now().UTC().UnixNano(), domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err = affected(result, err); err != nil {
		return err
	}
	if err = acceptClusterOperation(ctx, tx, ref); err != nil {
		return err
	}
	return commitSpecAudit(ctx, tx, ref, before, spec, nil)
}

// preserveRetryEpoch keeps deployment budgets exclusively under the action's control.
func preserveRetryEpoch(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef, spec domain.ClusterSpec) ([]byte, domain.ClusterSpec, error) {
	if err := guardBindingMajor(ctx, tx, ref, spec); err != nil {
		return nil, spec, err
	}
	if err := tx.QueryRowContext(ctx, "SELECT coalesce(json_extract(spec,'$.retryEpoch'),0) FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name).Scan(&spec.RetryEpoch); err != nil {
		return nil, spec, typed(err)
	}
	data, err := json.Marshal(spec)
	return data, spec, err
}

// IncrementClusterRetryEpoch commits the new budget, generation, operation and audit together.
func (s *Store) IncrementClusterRetryEpoch(ctx context.Context, ref domain.ClusterRef) error {
	if err := ref.Validate(); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = checkSpecSnapshot(ctx, tx, ref); err != nil {
		return err
	}
	var data []byte
	if err := tx.QueryRowContext(ctx, "SELECT spec FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name).Scan(&data); err != nil {
		return typed(err)
	}
	var before domain.ClusterSpec
	if err := json.Unmarshal(data, &before); err != nil {
		return err
	}
	after := before
	after.RetryEpoch++
	if _, initial, err := initialDataSourceOperation(ctx, tx, ref); err != nil {
		return err
	} else if initial != nil {
		return initialRestoreConflict(initial)
	}
	// Use the same bytes as ordinary spec updates: SQLite json_set returns
	// TEXT and preserves property ordering, making unchanged updates differ.
	data, err = json.Marshal(after)
	if err != nil {
		return err
	}
	result, err := tx.ExecContext(ctx, "UPDATE clusters SET spec=?,generation=generation+1,requested_by=?,updated=? WHERE namespace=? AND name=?", data, Actor(ctx), time.Now().UTC().UnixNano(), domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err = affected(result, err); err != nil {
		return err
	}
	if err = acceptClusterOperation(ctx, tx, ref); err != nil {
		return err
	}
	return commitSpecAudit(ctx, tx, ref, before, after, nil)
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

// MarkClusterDeleting sets deletion intent, reusing matching in-flight tracking,
// and retires work before cleanup starts.
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
	var deleting bool
	var cleanupStarted bool
	var currentOptions []byte
	var generation int64
	var created int64
	if err = tx.QueryRowContext(ctx, "SELECT json_extract(status,'$.phase'),deleting,delete_options,generation,created,COALESCE(json_extract(status,'$.cleanupStarted'),0) FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name).Scan(&previous, &deleting, &currentOptions, &generation, &created, &cleanupStarted); err != nil {
		return typed(err)
	}
	if err = checkClusterSnapshot(ctx, tx, &domain.Cluster{Namespace: ref.Namespace, Name: ref.Name, Generation: generation, CreatedAt: time.Unix(0, created)}); err != nil {
		return err
	}
	if err = refuseBackupDependents(ctx, tx, ref, "", true, opts.KeepBackups); err != nil {
		return err
	}
	if err = guardPostgresBindings(ctx, tx, ref); err != nil {
		return err
	}
	if op, ok := ctx.Value(operationContextKey{}).(*domain.Operation); ok && op != nil && op.Kind == domain.OperationDelete && deleting {
		var current domain.DeleteOptions
		if err = json.Unmarshal(currentOptions, &current); err != nil {
			return err
		}
		if current == opts {
			var existing []byte
			err = tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='operation' AND scope=? AND phase IN ('Pending','Running') AND json_extract(data,'$.kind')=? AND json_extract(data,'$.targetGeneration')=? ORDER BY created DESC,key DESC LIMIT 1", ref.Key(), domain.OperationDelete, generation).Scan(&existing)
			if err == nil {
				return json.Unmarshal(existing, op)
			}
			if !errors.Is(err, sql.ErrNoRows) {
				return err
			}
		}
	}
	if deleting && cleanupStarted {
		var current domain.DeleteOptions
		if err = json.Unmarshal(currentOptions, &current); err != nil {
			return err
		}
		if (!current.KeepVolumes && opts.KeepVolumes) || (!current.KeepBackups && opts.KeepBackups) {
			return domain.Conflict("cluster", ref.Name, domain.ReasonInvalidState, ErrConflict, "Deletion of cluster %s already permits resource removal; retention cannot be promised after cleanup has started", ref.Name)
		}
	}
	data, _ := json.Marshal(opts)
	r, err := tx.ExecContext(ctx, "UPDATE clusters SET generation=generation+CASE WHEN deleting=0 OR delete_options<>? THEN 1 ELSE 0 END,requested_by=CASE WHEN deleting=0 OR delete_options<>? THEN ? ELSE requested_by END,deleting=1,delete_options=?,status=json_set(CASE WHEN deleting=0 THEN json_remove(status,'$.cleanupStarted') ELSE status END,'$.phase','Deleting','$.message','Deleting: queued resource cleanup'),updated=? WHERE namespace=? AND name=?", data, data, Actor(ctx), data, time.Now().UTC().UnixNano(), domain.NormalizedNamespace(ref.Namespace), ref.Name)
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

// StartClusterCleanup persists the retention fence before any provider cleanup
// call. The generation predicate prevents a worker holding superseded deletion
// options from beginning cleanup after a newer request has been accepted.
func (s *Store) StartClusterCleanup(ctx context.Context, c *domain.Cluster) error {
	if err := c.Ref().Validate(); err != nil {
		return err
	}
	tx, err := s.writer.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if err = guardPostgresBindings(ctx, tx, c.Ref()); err != nil {
		return err
	}
	r, err := tx.ExecContext(ctx, "UPDATE clusters SET status=json_set(status,'$.cleanupStarted',json('true')) WHERE namespace=? AND name=? AND generation=? AND created=? AND deleting=1", domain.NormalizedNamespace(c.Namespace), c.Name, c.Generation, c.CreatedAt.UnixNano())
	if err != nil {
		return typed(err)
	}
	n, err := r.RowsAffected()
	if err != nil {
		return err
	}
	if n == 0 {
		return ErrConflict
	}
	if err = tx.Commit(); err != nil {
		return err
	}
	c.Status.CleanupStarted = true
	return nil
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
	r, err := tx.ExecContext(ctx, "UPDATE clusters SET deleting=0,status=?,internal=?,updated=? WHERE namespace=? AND name=? AND generation=? AND created=? AND deleting=1 AND json_extract(delete_options,'$.keepVolumes')=1", status, []byte(c.Internal), time.Now().UTC().UnixNano(), domain.NormalizedNamespace(c.Namespace), c.Name, c.Generation, c.CreatedAt.UnixNano())
	if err == nil {
		if n, countErr := r.RowsAffected(); countErr != nil {
			return countErr
		} else if n == 0 {
			return ErrConflict
		}
	}
	if err = affected(r, err); err != nil {
		return err
	}
	if err = completeDeletionOperations(ctx, tx, c.Ref(), true, "Cluster runtime suspended; volumes retained"); err != nil {
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
	if err = checkClusterSnapshot(ctx, tx, c); err != nil {
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

// DeleteCluster removes cluster-owned restore records and atomically fails unfinished
// operations so recreating its name cannot resume work belonging to the removed cluster.
func (s *Store) DeleteCluster(ctx context.Context, ref domain.ClusterRef) error {
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
	if err = checkClusterSnapshot(ctx, tx, c); err != nil {
		return err
	}
	if err = completeDeletionOperations(ctx, tx, ref, false, "Cluster deleted"); err != nil {
		return err
	}
	if err = retireOperations(ctx, tx, ref.Key(), "Cluster deleted"); err != nil {
		return err
	}
	r, err := tx.ExecContext(ctx, "DELETE FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name)
	if err = affected(r, err); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, "DELETE FROM records WHERE scope=? AND kind IN ('secret','agent-token','volume','restore','schedule-cursor')", ref.Key())
	if err != nil {
		return err
	}
	return tx.Commit()
}
