// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// This is intent only: it must never be used as recovery coverage evidence.
type reanchorState struct {
	Writer        string    `json:"writer,omitempty"`
	FirstSeen     time.Time `json:"firstSeen,omitempty"`
	LastTriggered time.Time `json:"lastTriggered,omitempty"`
	LastReason    string    `json:"lastReason,omitempty"`
	Baseline      string    `json:"baseline,omitempty"`
	SourceUUID    string    `json:"sourceUUID,omitempty"`
	VerifyAfter   time.Time `json:"verifyAfter,omitempty"`
}

type reanchorOperationStore interface {
	ListOperations(context.Context, domain.ClusterRef, ...domain.Phase) ([]domain.Operation, error)
}

func reanchorEvent(ctx context.Context, rc *engine.Context, s *reanchorState, reason, message string) error {
	if s.LastReason == reason {
		return nil
	}
	if rc.Events != nil {
		if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Normal", Reason: reason, Message: message, CreatedAt: now(rc)}); err != nil {
			return err
		}
	}
	s.LastReason = reason
	return nil
}

// reconcilePITRReanchor runs only from Sync. Status/admission cannot enqueue work.
func (p *Core) reconcilePITRReanchor(ctx context.Context, rc *engine.Context, s *state) error {
	r := &s.PITRReanchor
	policy := rc.Cluster.Spec.Backup
	if !policy.PITR.Enabled || policy.PITR.AutoReanchor != nil && !*policy.PITR.AutoReanchor {
		r.Writer, r.FirstSeen, r.LastReason = "", time.Time{}, ""
		return nil
	}
	skip := func(reason, message string) error {
		return reanchorEvent(ctx, rc, r, "PITRReanchorSkipped"+reason, message)
	}
	if policy.Storage == "" {
		r.FirstSeen = time.Time{}
		return skip("NoStorage", "Automatic PITR full backup deferred: no backup storage configured")
	}
	writer, uuid, err := archiveWriter(ctx, rc, s)
	if err != nil || !myspec.ValidUUID(uuid) {
		r.Writer, r.FirstSeen = "", time.Time{}
		return skip("WriterUnavailable", "Automatic PITR full backup deferred: archive writer identity unavailable")
	}
	at := now(rc)
	if r.Writer != uuid || r.FirstSeen.IsZero() {
		r.Writer, r.FirstSeen = uuid, at
	}
	operations, ok := rc.Store.(reanchorOperationStore)
	if !ok {
		r.FirstSeen = at
		return skip("OperationsUnavailable", "Automatic PITR full backup deferred: operation inventory unavailable")
	}
	ops, err := operations.ListOperations(ctx, rc.Cluster.Ref())
	if err != nil {
		return err
	}
	for _, op := range ops {
		if !domain.OperationDefersPITRReanchor(op, at) {
			continue
		}
		if op.Phase == domain.PhasePending || op.Phase == domain.PhaseRunning {
			r.FirstSeen = at
			return skip("OperationInProgress", "Automatic PITR full backup deferred: cluster operation in progress")
		}
		if op.CompletedAt != nil && op.CompletedAt.After(r.FirstSeen) {
			r.FirstSeen = *op.CompletedAt
		}
	}
	for _, m := range s.Members {
		if m.ConfirmedVolumeLost || m.RuntimeRecovery.VolumeLost || m.ProxyDraining || m.ProxyDrainOperation != "" {
			r.FirstSeen = at
			return skip("OperationInProgress", "Automatic PITR full backup deferred: cluster maintenance in progress")
		}
	}
	if s.Rolling != nil || s.Pending != nil || s.Replacement != nil || s.TLSRotation != nil || s.GaleraTLS != nil {
		r.FirstSeen = at
		return skip("OperationInProgress", "Automatic PITR full backup deferred: cluster maintenance in progress")
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return err
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	storage, err := p.backupStorage(ctx, rc, "", rc.Cluster.Ref().Namespace)
	if err != nil {
		return err
	}
	var newest *domain.Backup
	for i := range items {
		b := &items[i]
		if b.Trigger == domain.BackupTriggerPITRReanchor && b.CreatedAt.After(r.LastTriggered) {
			r.LastTriggered = b.CreatedAt
		}
		m := b.MySQL
		if b.Phase != domain.PhaseSucceeded || m == nil || m.Type != "full" || b.Storage != storage.Name || m.Repository != repoPrefix(storage, rc.Cluster, s, bs.Generation) || m.ClusterUID != s.UID {
			continue
		}
		if newest == nil || m.EndTime.After(newest.MySQL.EndTime) || m.EndTime.Equal(newest.MySQL.EndTime) && b.Name > newest.Name {
			newest = b
		}
	}
	baseline := ""
	if newest == nil {
		r.Baseline, r.SourceUUID, r.VerifyAfter = "", "", time.Time{}
	} else {
		baseline = newest.Name
		// Names are reusable after deletion. Bind the scheduling proof to the
		// immutable backup incarnation, repository and complete physical
		// provenance, including generation and sealed object-path identity.
		identity, e := json.Marshal(struct {
			Name             string
			OperationID      string
			CreatedAt        time.Time
			Storage          string
			RecoveryIdentity string
			MySQL            *domain.MySQLBackup
		}{newest.Name, newest.OperationID, newest.CreatedAt, newest.Storage, newest.RecoveryIdentity, newest.MySQL})
		if e != nil {
			return e
		}
		key := fmt.Sprintf("%x", sha256.Sum256(identity))
		if r.Baseline != key {
			r.Baseline, r.SourceUUID, r.VerifyAfter = key, "", time.Time{}
		}
	}
	for _, b := range items {
		if b.Phase == domain.PhasePending || b.Phase == domain.PhaseRunning {
			return skip("BackupInProgress", "Automatic PITR full backup deferred: another backup is pending or running")
		}
	}
	for _, op := range ops {
		if op.Kind == domain.OperationBackup && (op.Phase == domain.PhasePending || op.Phase == domain.PhaseRunning) {
			return skip("BackupInProgress", "Automatic PITR full backup deferred: another backup is pending or running")
		}
	}
	if !r.LastTriggered.IsZero() && at.Sub(r.LastTriggered) < time.Hour {
		return skip("Cooldown", "Automatic PITR full backup deferred: one-hour re-anchor cooldown")
	}
	if at.Sub(r.FirstSeen) < domain.PITRReanchorStabilityPeriod {
		return skip("WriterNotStable", "Automatic PITR full backup deferred: writer must remain stable for five minutes after cluster operations")
	}
	if newest != nil {
		if !myspec.ValidUUID(r.SourceUUID) {
			if at.Before(r.VerifyAfter) {
				return skip("BaselineUnavailable", "Automatic PITR full backup deferred: baseline verification unavailable")
			}
			r.VerifyAfter = at.Add(domain.PITRReanchorStabilityPeriod)
			refs, e := backupManifestRefs(ctx, rc, items, newest.MySQL.ObjectPaths)
			if e != nil {
				return skip("BaselineUnavailable", "Automatic PITR full backup deferred: baseline verification unavailable")
			}
			repo, e := repository(storage, newest.MySQL.Repository)
			if e != nil {
				return e
			}
			var coverage myspec.PITRCoverageResponse
			e = mysqlAction(ctx, rc, writer, myspec.ActionPITRCoverage, myspec.PITRCoverageRequest{Source: repo, ObjectPaths: newest.MySQL.ObjectPaths, ObjectManifests: refs, AcceptPending: true}, &coverage)
			if e != nil || coverage.Pending || !myspec.ValidUUID(coverage.SourceUUID) {
				return skip("BaselineUnavailable", "Automatic PITR full backup deferred: baseline verification unavailable")
			}
			r.SourceUUID = coverage.SourceUUID
		}
		if r.SourceUUID == uuid {
			r.LastReason = ""
			return nil
		}
	}
	name := "full-" + secretsHash(rc.Cluster.Ref().Namespace+"/"+rc.Cluster.Name+"/"+uuid+"/"+baseline)
	for _, b := range items {
		if b.Name == name && b.Phase != domain.PhasePending && b.Phase != domain.PhaseRunning {
			return nil
		}
	}
	b := &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: name, Provider: rc.Cluster.Spec.Provider, Storage: policy.Storage, Type: "full", Trigger: domain.BackupTriggerPITRReanchor, Phase: domain.PhasePending, CreatedAt: at}
	payload, _ := json.Marshal(map[string]string{"backup": name})
	if err = rc.Store.EnqueueBackup(ctx, b, &domain.Operation{Namespace: b.Namespace, Cluster: b.Cluster, Kind: domain.OperationBackup, Payload: payload, Actor: "pitr-reanchor", CreatedAt: at}); err != nil {
		var busy *domain.BackupAdmissionBusyError
		if errors.As(err, &busy) {
			if busy.Operation {
				r.FirstSeen = at
				return skip("OperationInProgress", "Automatic PITR full backup deferred: cluster operation in progress")
			}
			return skip("BackupInProgress", "Automatic PITR full backup deferred: another backup is pending or running")
		}
		return err
	}
	r.LastTriggered = at
	r.LastReason = "" // Each distinct epoch emits its own trigger event.
	return reanchorEvent(ctx, rc, r, "PITRReanchorTriggered", fmt.Sprintf("Automatic full backup %s queued to establish PITR coverage from the current archive writer; existing coverage remains unchanged until verification succeeds", name))
}
