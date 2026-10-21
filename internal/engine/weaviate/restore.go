// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"path"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
	"github.com/minio/minio-go/v7"
)

func sourceRef(target domain.ClusterRef, namespace, cluster string) domain.ClusterRef {
	if namespace == "" {
		namespace = target.Namespace
	}
	if cluster == "" {
		cluster = target.Name
	}
	return domain.ClusterRef{Namespace: namespace, Name: cluster}
}
func (p *Provider) restoreAdmission(ctx context.Context, st engine.ClusterStore, c *domain.Cluster, b *domain.Backup) (*domain.WeaviateBackup, error) {
	if b.Provider != "weaviate" || b.Type != "full" || b.Phase != domain.PhaseSucceeded || !UpgradeAllowed(b.SourceVersion, c.Spec.Version) {
		return nil, invalid("restore requires a succeeded full Weaviate backup and same or newer version within a supported upgrade path")
	}
	if b.Weaviate == nil || b.Weaviate.MemberCount != len(b.Weaviate.Members) || len(b.Weaviate.Members) != c.Spec.Components.Engine.Replicas {
		return nil, invalid("restore target member count must equal the recorded source member count")
	}
	storage, err := st.GetBackupStorage(ctx, b.Namespace, b.Storage)
	if err != nil {
		return nil, err
	}
	m, err := recordedBackup(storage, b)
	if err != nil {
		return nil, err
	}
	target, err := st.GetBackupStorage(ctx, c.Ref().Namespace, c.Spec.Backup.Storage)
	if err != nil {
		return nil, err
	}
	if target.EndpointURL != m.Endpoint || target.Bucket != m.Bucket {
		return nil, invalid("restore target bound storage must have the source backup endpoint and bucket")
	}
	seen := map[string]bool{}
	for _, name := range m.Members {
		if name == "" || seen[name] {
			return nil, invalid("backup has invalid source member identities")
		}
		seen[name] = true
	}
	return m, nil
}
func (p *Provider) validateDataSource(ctx context.Context, c *domain.Cluster) error {
	d := c.Spec.DataSource
	if d == nil {
		return nil
	}
	if d.PITRTime != nil || d.Backup == "" || d.Cluster == "" {
		return invalid("Weaviate DataSource requires a full backup and source cluster; PITR is unsupported")
	}
	if p.options.Store == nil {
		return invalid("DataSource validation requires store")
	}
	ref := sourceRef(c.Ref(), d.Namespace, d.Cluster)
	b, err := p.options.Store.GetBackup(ctx, ref, d.Backup)
	if err != nil {
		return err
	}
	if c.Spec.Backup.Storage == "" {
		c.Spec.Backup.Storage = b.Storage
	}
	if c.Spec.Backup.Storage != b.Storage {
		return invalid("DataSource target must bind the source backup's storage")
	}
	_, err = p.restoreAdmission(ctx, p.options.Store, c, b)
	return err
}
func nodeMapping(source []string, s *state) map[string]string {
	var mapping map[string]string
	targets := ordered(s)
	for i, name := range source {
		if name != targets[i].Name {
			if mapping == nil {
				mapping = map[string]string{}
			}
			mapping[name] = targets[i].Name
		}
	}
	return mapping
}

// Restore GET does not expose timestamps in v1.40.1. The coordinator's S3
// restore_config.json does: ResetStatus assigns a fresh startedAt per attempt.
type restoreIdentity struct {
	ID          string            `json:"id"`
	StartedAt   time.Time         `json:"startedAt"`
	CompletedAt time.Time         `json:"completedAt"`
	Status      wvapi.BackupState `json:"status"`
}

type restoreReceipt struct {
	Source           string               `json:"source"`
	Request          wvapi.RestoreRequest `json:"request"`
	Accepted         bool                 `json:"accepted"`
	BaselineCaptured bool                 `json:"baselineCaptured"`
	Baseline         *restoreIdentity     `json:"baseline,omitempty"`
}

func restoreMetadata(ctx context.Context, rc *engine.Context, b *domain.Backup, m *domain.WeaviateBackup) (*restoreIdentity, error) {
	storage, err := rc.Store.GetBackupStorage(ctx, b.Namespace, b.Storage)
	if err != nil {
		return nil, err
	}
	client, closeClient, err := backupstorage.NewClient(*storage, storage.Credentials)
	if err != nil {
		return nil, errors.New("cannot read Weaviate restore identity; check source S3 configuration and permissions")
	}
	defer closeClient()
	obj, err := client.GetObject(ctx, m.Bucket, path.Join(m.Path, m.ID, "restore_config.json"), minio.GetObjectOptions{})
	if err == nil {
		defer obj.Close()
		var data []byte
		data, err = io.ReadAll(io.LimitReader(obj, 16*1024*1024+1))
		if err == nil {
			var identity restoreIdentity
			if len(data) > 16*1024*1024 || json.Unmarshal(data, &identity) != nil || identity.ID != m.ID || identity.StartedAt.IsZero() || identity.Status == "" {
				return nil, invalid("ambiguous Weaviate restore identity; inspect source S3 restore_config.json before retrying")
			}
			return &identity, nil
		}
	}
	if minio.ToErrorResponse(err).Code == "NoSuchKey" {
		return nil, nil
	}
	return nil, errors.New("cannot read Weaviate restore identity; check source S3 restore_config.json access before retrying")
}

type backupRestoreOwnership interface {
	AcquireBackupRestore(context.Context, domain.ClusterRef, string, domain.ClusterRef, string, string) (bool, error)
	ReleaseBackupRestore(context.Context, domain.ClusterRef, string, domain.ClusterRef, string) error
}

func (p *Provider) runRestore(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp operationState, name string) (result engine.OpResult, runErr error) {
	finish := func(done bool, err error) (engine.OpResult, error) {
		r, e := operationResult(cp, done)
		return r, errors.Join(err, e)
	}
	if name == "" {
		return finish(true, invalid("restore name is required"))
	}
	r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return finish(false, err)
	}
	defer func() {
		if runErr != nil && result.Done {
			r.Phase = domain.PhaseFailed
			r.Error = "Weaviate restore failed"
			at := now(rc)
			r.CompletedAt = &at
			runErr = errors.Join(runErr, rc.Store.UpdateRestore(ctx, r))
		}
	}()
	if r.Source.PITRTime != nil || r.Source.Backup == "" {
		return finish(true, invalid("Weaviate restore requires a full backup; PITR is unsupported"))
	}
	ref := sourceRef(rc.Cluster.Ref(), r.SourceNamespace, r.Source.SourceCluster)
	b, err := rc.Store.GetBackup(ctx, ref, r.Source.Backup)
	if err != nil {
		return finish(false, err)
	}
	m, err := p.restoreAdmission(ctx, rc.Store, rc.Cluster, b)
	if err != nil {
		return finish(true, err)
	}
	if len(s.Members) != len(m.Members) {
		return finish(true, invalid("restore target member count must equal source member count"))
	}
	if op.ID == "" {
		return finish(true, invalid("restore requires a durable operation identity"))
	}
	ownership, ok := rc.Store.(backupRestoreOwnership)
	if !ok {
		return finish(true, invalid("restore requires atomic source-backup ownership support"))
	}
	if cp.Stage != "done" {
		acquired, e := ownership.AcquireBackupRestore(ctx, ref, b.Name, rc.Cluster.Ref(), name, op.ID)
		if e != nil {
			return finish(false, e)
		}
		if !acquired {
			const waiting = "Waiting for another target's restore of this source backup to finish"
			if r.Error != waiting {
				r.Error = waiting
				if e = rc.Store.UpdateRestore(ctx, r); e != nil {
					return finish(false, e)
				}
			}
			out, e := finish(false, nil)
			out.Step = "waiting-for-source-backup"
			return out, e
		}
		if r.Error == "Waiting for another target's restore of this source backup to finish" {
			r.Error = ""
			if e = rc.Store.UpdateRestore(ctx, r); e != nil {
				return finish(false, e)
			}
		}
		defer func() {
			if result.Done {
				runErr = errors.Join(runErr, ownership.ReleaseBackupRestore(ctx, ref, b.Name, rc.Cluster.Ref(), op.ID))
			}
		}()
	}
	client, err := p.native(ctx, rc, s)
	if err != nil {
		return finish(false, err)
	}
	defer client.Close()
	key := "operations/" + op.ID + "/restore-submission"
	req := wvapi.RestoreRequest{Config: wvapi.RestoreConfig{Bucket: m.Bucket, Path: m.Path, RolesOptions: wvapi.RestoreAll, UsersOptions: wvapi.RestoreAll}, NodeMapping: nodeMapping(m.Members, s)}
	if name == "initial-restore" || ref != rc.Cluster.Ref() || m.ClusterUID != s.UID {
		req.Config.RolesOptions = wvapi.RestoreNone
		req.Config.UsersOptions = wvapi.RestoreNone
	}
	receipt := restoreReceipt{Source: b.Location, Request: req}
	saveReceipt := func() error {
		raw, e := json.Marshal(receipt)
		if e != nil {
			return e
		}
		return putSecret(ctx, rc, key, raw)
	}
	if cp.Stage == "" || cp.Stage == "clear" || cp.Stage == "submit" || cp.Stage == "poll" {
		raw, e := secret(ctx, rc, key)
		if e == nil {
			if json.Unmarshal(raw, &receipt) != nil || receipt.Source != b.Location || hash(receipt.Request) != hash(req) || !receipt.BaselineCaptured {
				return finish(true, invalid("restore receipt lacks a matching baseline identity; inspect source S3 restore_config.json before retrying"))
			}
		} else if !missing(e) {
			return finish(false, e)
		} else {
			if cp.Stage != "" {
				return finish(true, invalid("restore baseline receipt is missing; refusing to adopt an unidentified native restore"))
			}
			receipt.Baseline, e = restoreMetadata(ctx, rc, b, m)
			if e != nil {
				return finish(false, e)
			}
			if receipt.Baseline != nil && !terminalBackup(receipt.Baseline.Status) {
				return finish(true, invalid("source backup already has an active native restore; wait for it to finish before retrying"))
			}
			receipt.BaselineCaptured = true
			if e = saveReceipt(); e != nil {
				return finish(false, e)
			}
		}
	}
	switch cp.Stage {
	case "":
		ready, e := p.clusterReady(ctx, rc, s)
		if e != nil {
			return finish(false, e)
		}
		if !ready {
			return finish(false, fmt.Errorf("%w: restore requires healthy target members", engine.ErrConflict))
		}
		at := now(rc)
		r.Phase = domain.PhaseRunning
		r.StartedAt = &at
		r.Error = ""
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return finish(false, err)
		}
		cp.Stage = "clear"
	case "clear":
		// Delete one collection per checkpoint; replaying this stage only observes
		// remaining collections, and can never clear restored data after submission.
		collections, e := client.Collections(ctx)
		if e != nil {
			return finish(false, e)
		}
		if len(collections) > 0 {
			if e = client.DeleteCollection(ctx, collections[0].Class); e != nil && !wvapi.IsNotFound(e) {
				return finish(false, e)
			}
			return finish(false, nil)
		}
		cp.Stage = "submit"
	case "submit":
		identity, e := restoreMetadata(ctx, rc, b, m)
		if e != nil {
			return finish(false, e)
		}
		if identity != nil && (receipt.Baseline == nil || !identity.StartedAt.Equal(receipt.Baseline.StartedAt)) {
			receipt.Accepted = true
			if e = saveReceipt(); e != nil {
				return finish(false, e)
			}
			cp.Stage = "poll"
			return finish(false, nil)
		}
		if receipt.Accepted || (identity == nil && receipt.Baseline != nil) {
			return finish(true, invalid("ambiguous Weaviate restore acceptance; inspect source S3 restore_config.json and target collections before retrying"))
		}
		// An unchanged baseline is the previous restore, not acceptance of this
		// intent. This includes a crash after receipt persistence but before POST.
		if _, err = client.Restore(ctx, m.ID, req); err != nil {
			return finish(false, err)
		}
		receipt.Accepted = true
		if err = saveReceipt(); err != nil {
			return finish(false, err)
		}
		cp.Stage = "poll"
	case "poll":
		identity, e := restoreMetadata(ctx, rc, b, m)
		if e != nil {
			return finish(false, e)
		}
		if identity == nil || (receipt.Baseline != nil && identity.StartedAt.Equal(receipt.Baseline.StartedAt)) {
			return finish(true, invalid("ambiguous Weaviate restore status; inspect source S3 restore_config.json and target collections before retrying"))
		}
		info, e := client.RestoreStatus(ctx, m.ID, nativeLocation(m))
		if e != nil {
			return finish(false, e)
		}
		if info.Status == wvapi.BackupFailed || info.Status == wvapi.BackupCanceled {
			return finish(true, errors.New(backupFailure("restore", info.Status)))
		}
		if info.Status != wvapi.BackupSuccess {
			return finish(false, nil)
		}
		cp.Stage = "health"
	case "health":
		ready, e := p.clusterReady(ctx, rc, s)
		if e != nil {
			return finish(false, e)
		}
		if !ready {
			return finish(false, nil)
		}
		at := now(rc)
		r.Phase = domain.PhaseSucceeded
		r.CompletedAt = &at
		r.Error = ""
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return finish(false, err)
		}
		if name == "initial-restore" {
			s.InitialRestore = "done"
		}
		cp.Stage = "done"
		return finish(true, nil)
	case "done":
		return finish(true, nil)
	default:
		return finish(false, invalid("unknown restore checkpoint"))
	}
	return finish(false, nil)
}

func (p *Provider) initialRestore(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	d := rc.Cluster.Spec.DataSource
	if d == nil || s.InitialRestore == "done" {
		return false, nil
	}
	ref := sourceRef(rc.Cluster.Ref(), d.Namespace, d.Cluster)
	existing, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), "initial-restore")
	if err == nil {
		if existing.Source.Backup != d.Backup || existing.Source.SourceCluster != d.Cluster || domain.NormalizedNamespace(existing.SourceNamespace) != domain.NormalizedNamespace(ref.Namespace) {
			return true, engine.ErrConflict
		}
		s.InitialRestore = "initial-restore"
		if existing.Phase == domain.PhaseSucceeded {
			s.InitialRestore = "done"
		}
		return true, nil
	}
	if !missing(err) {
		return true, err
	}
	at := now(rc)
	r := &domain.Restore{Namespace: rc.Cluster.Ref().Namespace, SourceNamespace: ref.Namespace, Cluster: rc.Cluster.Name, Name: "initial-restore", Phase: domain.PhasePending, Source: domain.RestoreSource{SourceCluster: d.Cluster, Backup: d.Backup}, CreatedAt: at}
	payload, _ := json.Marshal(map[string]string{"restore": "initial-restore"})
	op := &domain.Operation{ID: "initial-restore-" + s.UID, Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Kind: domain.OperationRestore, Phase: domain.PhasePending, Payload: payload, CreatedAt: at}
	if err = rc.Store.EnqueueRestore(ctx, r, op); err != nil {
		return true, err
	}
	s.InitialRestore = "initial-restore"
	return true, nil
}
