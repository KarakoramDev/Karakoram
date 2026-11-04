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
	"github.com/KarakoramDev/Karakoram/internal/maintenance"
)

type maintenanceClockKey struct{}

// WithMaintenanceTime supplies a worker's injectable clock for queue admission.
func WithMaintenanceTime(ctx context.Context, now time.Time) context.Context {
	return context.WithValue(ctx, maintenanceClockKey{}, now)
}
func maintenanceTime(ctx context.Context) time.Time {
	if now, ok := ctx.Value(maintenanceClockKey{}).(time.Time); ok {
		return now
	}
	return time.Now().UTC()
}
func maintenanceOverride(ctx context.Context) bool {
	op, _ := ctx.Value(operationContextKey{}).(*domain.Operation)
	return op != nil && op.IgnoreMaintenanceWindow
}

// applySpecMaintenanceOverride releases the accepted generation without replacing
// its convergence receipt when desired state itself is unchanged.
func applySpecMaintenanceOverride(ctx context.Context, tx *sql.Tx, ref domain.ClusterRef) error {
	if !maintenanceOverride(ctx) {
		return nil
	}
	_, err := tx.ExecContext(ctx, "UPDATE clusters SET maintenance_override_generation=generation WHERE namespace=? AND name=?", domain.NormalizedNamespace(ref.Namespace), ref.Name)
	return err
}

// MaintenanceWindowWait means all otherwise claimable actions await a window.
// No attempt, StartedAt or execution budget has been consumed.
type MaintenanceWindowWait struct{ NextStart time.Time }

func (w *MaintenanceWindowWait) Error() string { return maintenanceWaitMessage(w.NextStart) }
func maintenanceWaitMessage(next time.Time) string {
	if next.IsZero() {
		return "Waiting for maintenance window; no next start is scheduled"
	}
	return "Waiting for maintenance window; next start " + next.UTC().Format(time.RFC3339)
}

// maintenanceCandidate skips closed-window actions without head-of-line blocking
// exempt work. Wait messages and deduplicated events commit with queue admission.
func maintenanceCandidate(ctx context.Context, tx *sql.Tx, cluster string, includePending bool, kind string) (*domain.Operation, *MaintenanceWindowWait, error) {
	ref := domain.ParseClusterKey(cluster)
	c, err := scanCluster(tx.QueryRowContext(ctx, "SELECT "+clusterColumns+" FROM clusters WHERE namespace=? AND name=?", ref.Namespace, ref.Name))
	if err != nil {
		return nil, nil, err
	}
	now := maintenanceTime(ctx)
	open, _, next, err := maintenance.Open(c.Spec.Maintenance, now)
	if err != nil {
		return nil, nil, err
	}
	rows, err := tx.QueryContext(ctx, "SELECT data FROM records WHERE kind='operation' AND scope=? AND phase IN ('Pending','Running') AND json_extract(data,'$.targetGeneration') IS NULL AND (? OR phase='Running') AND (?='' OR json_extract(data,'$.kind')=?) ORDER BY CASE phase WHEN 'Running' THEN 0 ELSE 1 END,created,key", cluster, includePending, kind, kind)
	if err != nil {
		return nil, nil, err
	}
	var candidates []domain.Operation
	for rows.Next() {
		var raw []byte
		var op domain.Operation
		if err = rows.Scan(&raw); err == nil {
			err = json.Unmarshal(raw, &op)
		}
		if err != nil {
			rows.Close()
			return nil, nil, err
		}
		candidates = append(candidates, op)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil {
		return nil, nil, err
	}
	if closeErr != nil {
		return nil, nil, closeErr
	}
	var selected *domain.Operation
	var wait *MaintenanceWindowWait
	for i := range candidates {
		op := &candidates[i]
		if op.Phase == domain.PhaseRunning || open || !maintenance.Gated(op) {
			if selected == nil {
				selected = op
			}
			continue
		}
		wait = &MaintenanceWindowWait{NextStart: next}
		op.Message = wait.Error()
		op.UpdatedAt = now
		if err = put(ctx, tx, "operation", cluster, op.ID, op, "", "", string(op.Phase), false); err != nil {
			return nil, nil, err
		}
		if err = recordMaintenanceWait(ctx, tx, op); err != nil {
			return nil, nil, err
		}
	}
	return selected, wait, nil
}

// recordMaintenanceWait shares the durable, per-operation wait receipt between
// queued imperative actions and desired-generation tracking.
func recordMaintenanceWait(ctx context.Context, tx *sql.Tx, op *domain.Operation) error {
	var exists bool
	ref := domain.ClusterRef{Namespace: op.Namespace, Name: op.Cluster}
	if err := tx.QueryRowContext(ctx, "SELECT EXISTS(SELECT 1 FROM events WHERE namespace=? AND cluster=? AND json_extract(data,'$.operation')=? AND json_extract(data,'$.reason')='MaintenanceWindowWait')", domain.NormalizedNamespace(ref.Namespace), ref.Name, op.ID).Scan(&exists); err != nil {
		return err
	}
	if exists {
		return nil
	}
	event := domain.Event{Namespace: domain.NormalizedNamespace(ref.Namespace), Cluster: ref.Name, Operation: op.ID, Actor: op.Actor, Resource: "clusters", Target: ref.Name, Type: "Normal", Reason: "MaintenanceWindowWait", Message: op.Message, CreatedAt: op.UpdatedAt}
	if err := appendEvent(ctx, tx, &event); err != nil {
		return fmt.Errorf("recording maintenance wait: %w", err)
	}
	return nil
}
