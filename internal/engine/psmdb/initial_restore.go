// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
)

// initialRestore is independent of desired generation. Queueing a restore or
// observing healthy mongods is not evidence that the source data was restored.
type initialRestore struct {
	OperationID string `json:"operationID"`
	NativeName  string `json:"nativeName,omitempty"`
	Completed   bool   `json:"completed"`
	Failure     string `json:"failure,omitempty"`
}

func initialRestoreID(c *domain.Cluster, s *internalState) string {
	return c.Ref().Key() + "-" + s.UID + "-initial-restore"
}

// InitialDataSourceApplied implements the shared readiness and source-retention
// contract. Neither generation changes nor a control-plane restart clear it.
func (*Provider) InitialDataSourceApplied(c *domain.Cluster) (bool, error) {
	s, err := load(c)
	if err != nil {
		return false, err
	}
	if c.Spec.DataSource != nil && s.InitialRestore != nil && s.InitialRestore.Failure != "" {
		return false, fmt.Errorf("%s", s.InitialRestore.Failure)
	}
	return c.Spec.DataSource == nil || (s.InitialRestore != nil && s.InitialRestore.Completed && s.InitialRestore.NativeName != "" && s.InitialRestore.Failure == ""), nil
}

// MigrateInitialDataSource imports only matching, successful legacy restore
// receipts. It performs no provisioning or native work and runs before shared
// admission interprets an old superseded create tracker as an aborted restore.
func (*Provider) MigrateInitialDataSource(ctx context.Context, rc *engine.Context) error {
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	if err := migrateInitialRestore(ctx, rc, s); err != nil {
		return err
	}
	return save(rc.Cluster, s)
}

func migrateInitialRestore(ctx context.Context, rc *engine.Context, s *internalState) error {
	d := rc.Cluster.Spec.DataSource
	if d == nil || !s.CloneQueued || s.InitialRestore != nil {
		return nil
	}
	id := initialRestoreID(rc.Cluster, s)
	op, err := rc.Store.GetOperation(ctx, id)
	if missing(err) {
		return nil
	}
	if err != nil {
		return err
	}
	if op.Kind != domain.OperationRestore || op.Cluster != rc.Cluster.Name ||
		domain.NormalizedNamespace(op.Namespace) != rc.Cluster.Ref().Namespace || op.Phase != domain.PhaseSucceeded ||
		op.CancelRequested || (op.ClusterIncarnation != "" && op.ClusterIncarnation != fmt.Sprint(rc.Cluster.CreatedAt.UnixNano())) {
		return nil
	}
	var payload struct {
		Restore string `json:"restore"`
	}
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		return err
	}
	if payload.Restore != "initial-restore" {
		return nil
	}
	r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), "initial-restore")
	if missing(err) {
		return nil
	}
	if err != nil {
		return err
	}
	sourceNamespace, expectedNamespace := r.SourceNamespace, d.Namespace
	if sourceNamespace == "" {
		sourceNamespace = rc.Cluster.Ref().Namespace
	}
	if expectedNamespace == "" {
		expectedNamespace = rc.Cluster.Ref().Namespace
	}
	if r.Phase != domain.PhaseSucceeded || r.Error != "" || r.CompletedAt == nil ||
		r.Cluster != rc.Cluster.Name || domain.NormalizedNamespace(r.Namespace) != rc.Cluster.Ref().Namespace ||
		r.OperationID != id || r.Source.SourceCluster != d.Cluster ||
		r.Source.Backup != d.Backup || sourceNamespace != expectedNamespace ||
		(r.Source.PITRTime == nil) != (d.PITRTime == nil) {
		return nil
	}
	if d.PITRTime != nil && !r.Source.PITRTime.Equal(*d.PITRTime) {
		return nil
	}
	raw, err := pbm.MigrateRestoreCheckpoint(op, r)
	if err != nil {
		return err
	}
	if raw == nil {
		raw = op.Checkpoint
	}
	outcome, err := pbm.ReadRestoreOutcome(raw)
	if err != nil {
		return err
	}
	if outcome.RecoveredSuccessfully {
		s.InitialRestore = &initialRestore{OperationID: id, NativeName: outcome.NativeName, Completed: true}
	}
	return nil
}

func recordInitialRestore(c *domain.Cluster, s *internalState, op *domain.Operation, result engine.OpResult, runErr error) error {
	if c.Spec.DataSource == nil || op.Kind != domain.OperationRestore || op.ID != initialRestoreID(c, s) {
		return nil
	}
	if s.InitialRestore == nil {
		s.InitialRestore = &initialRestore{OperationID: op.ID}
	}
	if s.InitialRestore.Failure != "" {
		return nil
	}
	raw := result.Checkpoint
	if raw == nil {
		raw = op.Checkpoint
	}
	outcome, err := pbm.ReadRestoreOutcome(raw)
	if err != nil {
		return err
	}
	if outcome.Failure != "" {
		s.InitialRestore.Completed = false
		s.InitialRestore.Failure = fmt.Sprintf("Initial restore operation %s failed: %s", op.ID, outcome.Failure)
	} else if result.Done && runErr == nil && outcome.RecoveredSuccessfully {
		s.InitialRestore.NativeName = outcome.NativeName
		s.InitialRestore.Completed = true
		s.InitialRestore.Failure = ""
	}
	return nil
}

// reconcileInitialRestore never implicitly retries a terminal initial restore.
// In particular CloneQueued from an older controller is not completion evidence.
func reconcileInitialRestore(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	// A legacy create superseded before a restore receipt existed cannot be
	// replayed safely. Keep ordinary runtime convergence, not data application.
	if engine.InitialDataSourceUnproven(rc.Cluster) {
		return true, nil
	}
	d := rc.Cluster.Spec.DataSource
	if d == nil {
		return true, nil
	}
	id := initialRestoreID(rc.Cluster, s)
	if err := migrateInitialRestore(ctx, rc, s); err != nil {
		return false, err
	}
	if s.InitialRestore == nil {
		s.InitialRestore = &initialRestore{OperationID: id}
	}
	if s.InitialRestore.Completed && s.InitialRestore.NativeName != "" && s.InitialRestore.Failure == "" {
		return true, nil
	}
	op, err := rc.Store.GetOperation(ctx, id)
	if missing(err) && !s.CloneQueued && s.InitialRestore.Failure == "" {
		payload, _ := json.Marshal(map[string]string{"restore": "initial-restore"})
		op = &domain.Operation{Namespace: rc.Cluster.Ref().Namespace, ID: id, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Payload: payload, Phase: domain.PhasePending}
		err = rc.Store.EnqueueRestore(ctx, &domain.Restore{Namespace: rc.Cluster.Ref().Namespace, SourceNamespace: d.Namespace, Name: "initial-restore", Cluster: rc.Cluster.Name, Source: domain.RestoreSource{Backup: d.Backup, PITRTime: d.PITRTime, SourceCluster: d.Cluster}, Phase: domain.PhasePending}, op)
		if err != nil {
			return false, err
		}
		s.CloneQueued = true
	} else if missing(err) {
		s.InitialRestore.Failure = fmt.Sprintf("Initial restore operation %s is missing; completion cannot be proven; recreate the target from a verified backup", id)
	} else if err != nil {
		return false, err
	}
	if op != nil {
		s.CloneQueued = true
		switch op.Phase {
		case domain.PhaseFailed, domain.PhaseCancelled, domain.PhaseSucceeded:
			s.InitialRestore.Failure = fmt.Sprintf("Initial restore operation %s ended %s without durable completion evidence: %s; recreate the target from a verified backup", id, op.Phase, op.Message)
		}
	}
	s.Phase = "restore"
	rc.Cluster.Status.Phase = domain.PhaseRestoring
	rc.Cluster.Status.Message = "Waiting for initial restore operation " + id
	if s.InitialRestore.Failure != "" {
		s.Phase = "initial-restore-failed"
		rc.Cluster.Status.Phase = domain.PhaseFailed
		rc.Cluster.Status.Message = s.InitialRestore.Failure
	}
	return false, nil
}
