// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

// All admission and destructive intents use the serialized writer transaction.
// Whichever commits first fences the other, including create-before-EnqueueRestore.
func admitRestoreBackup(ctx context.Context, tx *sql.Tx, target, source domain.ClusterRef, name string, pitr bool) error {
	if name == "" && !pitr {
		return nil
	}
	if source.Namespace == "" {
		source.Namespace = target.Namespace
	}
	if source.Name == "" {
		source.Name = target.Name
	}
	var b domain.Backup
	if name != "" {
		var raw []byte
		if err := tx.QueryRowContext(ctx, "SELECT data FROM records WHERE kind='backup' AND scope=? AND key=?", source.Key(), name).Scan(&raw); err != nil {
			if errors.Is(err, sql.ErrNoRows) {
				return domain.Conflict("backup", name, domain.ReasonInvalidState, ErrConflict, "Source backup %s/%s is no longer available; select another successful backup", source.Name, name)
			}
			return err
		}
		if err := json.Unmarshal(raw, &b); err != nil {
			return err
		}
		if b.Phase == domain.PhaseDeleting {
			return domain.Conflict("backup", name, domain.ReasonInvalidState, ErrConflict, "Source backup %s/%s is deleting; select another available backup", source.Name, name)
		}
	}
	if pitr || b.Postgres != nil {
		items, err := restoreSourceBackups(ctx, tx, source)
		if err != nil {
			return err
		}
		for _, item := range items {
			if item.Phase != domain.PhaseDeleting {
				continue
			}
			if pitr || backupDeletionClosure(items, item.Name)[name] {
				return domain.Conflict("backup", name, domain.ReasonInvalidState, ErrConflict, "Source backup %s/%s is deleting and may remove the requested base set or PITR history; wait for deletion to finish and select an available recovery point", source.Name, item.Name)
			}
		}
	}
	var deleting bool
	var keep bool
	err := tx.QueryRowContext(ctx, "SELECT deleting,COALESCE(json_extract(delete_options,'$.keepBackups'),0) FROM clusters WHERE namespace=? AND name=?", domain.NormalizedNamespace(source.Namespace), source.Name).Scan(&deleting, &keep)
	if err != nil && !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if pitr && (deleting || errors.Is(err, sql.ErrNoRows)) {
		return domain.Conflict("cluster", source.Name, domain.ReasonInvalidState, ErrConflict, "PITR source cluster %s is deleting or absent; select an active source with available base backups and PITR history", source.Name)
	}
	if deleting && !keep {
		return domain.Conflict("backup", name, domain.ReasonInvalidState, ErrConflict, "Source cluster %s is deleting its backups; select another source or wait for deletion with keepBackups=true", source.Name)
	}
	return nil
}

// name empty protects the whole source prefix. Cluster deletion may retire its
// own restores, but must not retire another cluster's dependency.
func refuseBackupDependents(ctx context.Context, tx *sql.Tx, source domain.ClusterRef, name string, deletingCluster, keepBackups bool) error {
	var removed map[string]bool
	if name != "" {
		items, err := restoreSourceBackups(ctx, tx, source)
		if err != nil {
			return err
		}
		removed = backupDeletionClosure(items, name)
	}
	rows, err := tx.QueryContext(ctx, `SELECT data FROM records WHERE kind='restore' AND phase IN ('Pending','Running')`)
	if err != nil {
		return err
	}
	var restores []domain.Restore
	for rows.Next() {
		var raw []byte
		var r domain.Restore
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &r)
		}
		if err != nil {
			rows.Close()
			return err
		}
		restores = append(restores, r)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	check := func(target, ref domain.ClusterRef, backup, operation string, pitr bool) error {
		if ref.Namespace == "" {
			ref.Namespace = target.Namespace
		}
		if ref.Name == "" {
			ref.Name = target.Name
		}
		if (!pitr && (backup == "" || keepBackups || (name != "" && !removed[backup]))) || ref.Key() != source.Key() || (deletingCluster && target.Key() == source.Key()) {
			return nil
		}
		if pitr {
			return domain.Conflict("cluster", source.Name, domain.ReasonInvalidState, ErrConflict, "Source cluster %s, its base backups and PITR history are required by in-flight PITR restore/create operation %s on cluster %s/%s; wait for completion or safe cancellation before deleting source resources (keepBackups alone cannot preserve the live PITR source)", source.Name, operation, domain.NormalizedNamespace(target.Namespace), target.Name)
		}
		return domain.Conflict("backup", backup, domain.ReasonInvalidState, ErrConflict, "Backup %s/%s is required by in-flight restore/create operation %s on cluster %s/%s; wait for that operation to finish or cancel it safely before deleting the backup (cluster deletion can use keepBackups=true)", source.Name, backup, operation, domain.NormalizedNamespace(target.Namespace), target.Name)
	}
	for _, r := range restores {
		if err = check(domain.ClusterRef{Namespace: r.Namespace, Name: r.Cluster}, domain.ClusterRef{Namespace: r.SourceNamespace, Name: r.Source.SourceCluster}, r.Source.Backup, r.OperationID, r.Source.PITRTime != nil); err != nil {
			return err
		}
	}
	// A durable data source is a dependency even when a legacy/programmatic
	// cluster has no create tracker, or its tracker was prematurely retired.
	rows, err = tx.QueryContext(ctx, `SELECT namespace,name FROM clusters WHERE deleting=0 AND json_type(spec,'$.dataSource')='object'`)
	if err != nil {
		return err
	}
	var targets []domain.ClusterRef
	for rows.Next() {
		var target domain.ClusterRef
		if err = rows.Scan(&target.Namespace, &target.Name); err != nil {
			rows.Close()
			return err
		}
		targets = append(targets, target)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return err
	}
	for _, target := range targets {
		c, initial, err := initialDataSourceOperation(ctx, tx, target)
		if err != nil {
			return err
		}
		if initial == nil {
			continue
		}
		d := c.Spec.DataSource
		if err = check(target, domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}, d.Backup, initial.ID, d.PITRTime != nil); err != nil {
			return err
		}
	}
	return nil
}

func restoreSourceBackups(ctx context.Context, tx *sql.Tx, source domain.ClusterRef) ([]domain.Backup, error) {
	rows, err := tx.QueryContext(ctx, "SELECT data FROM records WHERE kind='backup' AND scope=?", source.Key())
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var items []domain.Backup
	for rows.Next() {
		var raw []byte
		var b domain.Backup
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &b)
		}
		if err != nil {
			return nil, err
		}
		items = append(items, b)
	}
	return items, rows.Err()
}

// pgBackRest expiration removes descendants as well as the named set. Use the
// same closure in both admission directions, independent of inventory order.
func backupDeletionClosure(items []domain.Backup, name string) map[string]bool {
	removed := map[string]bool{name: true}
	labels := map[string]bool{}
	for changed := true; changed; {
		changed = false
		for _, b := range items {
			if b.Postgres == nil {
				continue
			}
			dependent := labels[b.Postgres.Prior]
			for _, ref := range b.Postgres.References {
				dependent = dependent || labels[ref]
			}
			if dependent && !removed[b.Name] {
				removed[b.Name], changed = true, true
			}
			if removed[b.Name] && !labels[b.Postgres.Label] {
				labels[b.Postgres.Label], changed = true, true
			}
		}
	}
	return removed
}
