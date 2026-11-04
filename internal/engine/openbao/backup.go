// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/minio/minio-go/v7"
)

// Only object identities and workflow stages belong in checkpoints, never snapshot or credentials.
type snapshotCheckpoint struct {
	Step    string `json:"step"`
	Failure string `json:"failure,omitempty"`
}

var snapshotS3Client = backupstorage.NewClient

func snapshotLoadCheckpoint(op *domain.Operation) (snapshotCheckpoint, error) {
	var cp snapshotCheckpoint
	if len(op.Checkpoint) != 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return cp, errors.New("invalid snapshot operation checkpoint")
		}
	}
	return cp, nil
}
func snapshotResult(cp snapshotCheckpoint) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{Done: cp.Step == "done", Step: cp.Step, Message: cp.Step, Checkpoint: raw, RequeueAfter: 2 * time.Second}
}
func snapshotPayload(op *domain.Operation, field string) (string, error) {
	var payload map[string]string
	if json.Unmarshal(op.Payload, &payload) != nil || payload[field] == "" {
		return "", fmt.Errorf("operation requires %s", field)
	}
	return payload[field], nil
}
func snapshotEvent(ctx context.Context, rc *engine.Context, op *domain.Operation, reason, message string) error {
	if rc.Events == nil {
		return nil
	}
	kind := "Normal"
	if strings.HasSuffix(reason, "Failed") {
		kind = "Warning"
	}
	return rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Operation: op.ID, Type: kind, Reason: reason, Message: message, CreatedAt: now(rc)})
}
func snapshotStorage(ctx context.Context, rc *engine.Context, b *domain.Backup) (*domain.BackupStorage, error) {
	s, err := rc.Store.GetBackupStorage(ctx, domain.NormalizedNamespace(b.Namespace), b.Storage)
	if err != nil {
		return nil, err
	}
	if s.Name != b.Storage || domain.NormalizedNamespace(s.Namespace) != domain.NormalizedNamespace(b.Namespace) {
		return nil, errors.New("backup storage identity does not match recorded backup")
	}
	return s, nil
}
func safeObjectPart(part string) bool {
	return part != "" && part != "." && part != ".." && !strings.ContainsAny(part, "/\\")
}
func snapshotKey(prefix, uid, name string) (string, error) {
	if !safeObjectPart(uid) || !safeObjectPart(name) {
		return "", errors.New("backup has no safe cluster UID or name")
	}
	prefix = strings.TrimRight(prefix, "/")
	if prefix != "" {
		for _, part := range strings.Split(prefix, "/") {
			if !safeObjectPart(part) {
				return "", errors.New("storage prefix is not a safe object namespace")
			}
		}
		prefix += "/"
	}
	return prefix + uid + "/raft/" + name + ".snap", nil
}

// Recorded locations remain authoritative even if the configured storage prefix changes.
func snapshotLocation(s domain.BackupStorage, b domain.Backup) error {
	if b.Provider != "openbao" || b.Type != "raft-snapshot" || b.Storage != s.Name || domain.NormalizedNamespace(b.Namespace) != domain.NormalizedNamespace(s.Namespace) {
		return errors.New("backup is not a OpenBao snapshot backup in this storage")
	}
	parts := strings.Split(b.Location, "/")
	if len(parts) < 3 || parts[len(parts)-2] != "raft" || !safeObjectPart(b.Name) || parts[len(parts)-1] != b.Name+".snap" {
		return errors.New("backup has no safe recorded snapshot object identity")
	}
	for _, part := range parts {
		if !safeObjectPart(part) {
			return errors.New("backup has an unsafe recorded snapshot object identity")
		}
	}
	return nil
}
func snapshotS3Error(action string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	code := minio.ToErrorResponse(err).Code
	// Responses can contain object contents or credentials; expose only fixed S3 error codes.
	switch code {
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "NoSuchBucket", "NoSuchKey", "NoSuchObject":
		return fmt.Errorf("S3 snapshot %s failed (%s)", action, code)
	default:
		return fmt.Errorf("S3 snapshot %s failed", action)
	}
}
func snapshotMissingObject(err error) bool {
	code := minio.ToErrorResponse(err).Code
	return code == "NoSuchKey" || code == "NoSuchObject" || code == "NotFound"
}

func (p *Provider) backupOp(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	if p.postgresStorage(rc.Cluster) {
		return engine.OpResult{}, invalid("PostgreSQL-storage OpenBao clusters do not support raft-snapshot backups")
	}
	cp, err := snapshotLoadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	if cp.Step == "done" {
		if cp.Failure != "" {
			return snapshotResult(cp), errors.New(cp.Failure)
		}
		return snapshotResult(cp), nil
	}
	name, err := snapshotPayload(op, "backup")
	if err != nil {
		return snapshotResult(cp), err
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return snapshotResult(cp), err
	}
	if b.Type != "raft-snapshot" {
		return snapshotResult(cp), errors.New("OpenBao only supports snapshot backups")
	}
	s, err := snapshotStorage(ctx, rc, b)
	if err != nil {
		return snapshotResult(cp), err
	}
	switch cp.Step {
	case "":
		if b.Location == "" {
			state, e := loadState(rc.Cluster)
			if e != nil {
				return snapshotResult(cp), e
			}
			b.Location, err = snapshotKey(s.Prefix, state.UID, b.Name)
			if err != nil {
				return snapshotResult(cp), err
			}
			b.Provider, b.SourceTopology, b.SourceVersion = "openbao", rc.Cluster.Spec.Topology, rc.Cluster.Spec.Version
		}
		if err = snapshotLocation(*s, *b); err != nil {
			return snapshotResult(cp), err
		}
		stamp := now(rc)
		if b.StartedAt == nil {
			b.StartedAt = &stamp
		}
		b.Phase, b.Error = domain.PhaseRunning, ""
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return snapshotResult(cp), err
		}
		if err = snapshotEvent(ctx, rc, op, "BackupStarted", "OpenBao snapshot backup started"); err != nil {
			return snapshotResult(cp), err
		}
		cp.Step = "upload"
	case "upload", "complete":
		if err = snapshotLocation(*s, *b); err != nil {
			return snapshotResult(cp), err
		}
		client, closeClient, e := snapshotS3Client(*s, s.Credentials)
		if e != nil {
			return snapshotResult(cp), e
		}
		defer closeClient()
		info, e := client.StatObject(ctx, s.Bucket, b.Location, minio.StatObjectOptions{})
		if e != nil && !snapshotMissingObject(e) {
			return snapshotResult(cp), snapshotS3Error("stat", e)
		}
		if e != nil {
			if cp.Step == "complete" {
				return snapshotResult(cp), errors.New("uploaded snapshot object is missing")
			}
			state, e := loadState(rc.Cluster)
			if e != nil {
				return snapshotResult(cp), e
			}
			token, e := p.ensureManagementToken(ctx, rc, state)
			if e != nil {
				return snapshotResult(cp), e
			}
			native, _, e := p.activeClient(ctx, rc, state, token)
			if e != nil {
				return snapshotResult(cp), e
			}
			health, e := native.Health(ctx)
			if e != nil {
				return snapshotResult(cp), e
			}
			if !health.Initialized || health.Sealed {
				return snapshotResult(cp), fmt.Errorf("%w: snapshot requires a healthy unsealed OpenBao member", engine.ErrUnavailable)
			}
			stream, e := native.WithTimeout(10 * time.Minute).Snapshot(ctx)
			if e != nil {
				return snapshotResult(cp), e
			}
			defer stream.Close()
			_, e = client.PutObject(ctx, s.Bucket, b.Location, &snapshotBoundedReader{r: stream, remaining: maxSnapshotBytes}, -1, minio.PutObjectOptions{ContentType: "application/gzip", PartSize: 16 << 20, NumThreads: 1})
			if e != nil {
				return snapshotResult(cp), snapshotS3Error("upload", e)
			}
			cp.Step = "complete"
			return snapshotResult(cp), nil
		}
		// A successful PUT whose checkpoint write was interrupted is recovered by HEAD.
		if info.Size <= 0 || info.Size > maxSnapshotBytes {
			return snapshotResult(cp), invalid("stored snapshot is empty or exceeds size limit")
		}
		b.Size, b.Phase, b.Error = info.Size, domain.PhaseSucceeded, ""
		stamp := now(rc)
		b.CompletedAt = &stamp
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return snapshotResult(cp), err
		}
		if err = snapshotEvent(ctx, rc, op, "BackupCompleted", "OpenBao snapshot backup succeeded"); err != nil {
			return snapshotResult(cp), err
		}
		cp.Step = "done"
	case "failed":
		b.Phase, b.Error = domain.PhaseFailed, cp.Failure
		stamp := now(rc)
		b.CompletedAt = &stamp
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return snapshotResult(cp), err
		}
		if err = snapshotEvent(ctx, rc, op, "BackupFailed", cp.Failure); err != nil {
			return snapshotResult(cp), err
		}
		cp.Step = "done"
		return snapshotResult(cp), errors.New(cp.Failure)
	default:
		return snapshotResult(cp), errors.New("unknown snapshot backup step")
	}
	return snapshotResult(cp), nil
}

func (p *Provider) deleteBackupOp(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	cp, err := snapshotLoadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	if cp.Step == "done" {
		return snapshotResult(cp), nil
	}
	name, err := snapshotPayload(op, "backup")
	if err != nil {
		return snapshotResult(cp), err
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if missing(err) {
		cp.Step = "done"
		return snapshotResult(cp), nil
	}
	if err != nil {
		return snapshotResult(cp), err
	}
	if b.Type != "raft-snapshot" || (b.Provider != "" && b.Provider != "openbao") {
		return snapshotResult(cp), errors.New("backup is not a OpenBao snapshot backup")
	}
	var s *domain.BackupStorage
	if b.Location != "" && cp.Step != "record" {
		s, err = snapshotStorage(ctx, rc, b)
		if err != nil {
			return snapshotResult(cp), err
		}
		if err = snapshotLocation(*s, *b); err != nil {
			return snapshotResult(cp), err
		}
	}
	switch cp.Step {
	case "":
		b.Phase = domain.PhaseDeleting
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return snapshotResult(cp), err
		}
		cp.Step = "delete"
		if b.Location == "" {
			// Object creation is impossible until its location is persisted.
			cp.Step = "record"
		}
	case "delete":
		if s != nil {
			if err = p.DeleteRetainedBackup(ctx, *s, *b); err != nil {
				return snapshotResult(cp), err
			}
		}
		cp.Step = "record"
	case "record":
		if err = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), name); err != nil && !missing(err) {
			return snapshotResult(cp), err
		}
		if err = snapshotEvent(ctx, rc, op, "BackupDeleted", "OpenBao snapshot backup deleted"); err != nil {
			return snapshotResult(cp), err
		}
		cp.Step = "done"
	default:
		return snapshotResult(cp), errors.New("unknown snapshot delete step")
	}
	return snapshotResult(cp), nil
}

// DeleteRetainedBackup deletes exactly the recorded snapshot object, without needing its source cluster.
func (p *Provider) DeleteRetainedBackup(ctx context.Context, storage domain.BackupStorage, b domain.Backup) error {
	if err := snapshotLocation(storage, b); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client, closeClient, err := snapshotS3Client(storage, storage.Credentials)
	if err != nil {
		return err
	}
	defer closeClient()
	if err = client.RemoveObject(ctx, storage.Bucket, b.Location, minio.RemoveObjectOptions{}); err != nil && !snapshotMissingObject(err) {
		return snapshotS3Error("delete", err)
	}
	return nil
}

var _ engine.RetainedBackupDeleter = (*Provider)(nil)

const maxSnapshotBytes int64 = 4 << 30

// Fail rather than silently producing a truncated, apparently successful object.
type snapshotBoundedReader struct {
	r         io.Reader
	remaining int64
}

func (r *snapshotBoundedReader) Read(p []byte) (int, error) {
	if r.remaining == 0 {
		var probe [1]byte
		n, err := r.r.Read(probe[:])
		if n != 0 {
			return 0, errors.New("OpenBao snapshot exceeds size limit")
		}
		return 0, err
	}
	if int64(len(p)) > r.remaining {
		p = p[:r.remaining]
	}
	n, err := r.r.Read(p)
	r.remaining -= int64(n)
	return n, err
}
func (p *Provider) runBackupOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, bool, error) {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	var result engine.OpResult
	var err error
	switch op.Kind {
	case domain.OperationBackup:
		result, err = p.backupOp(ctx, rc, op)
	case domain.OperationDeleteBackup:
		result, err = p.deleteBackupOp(ctx, rc, op)
	case domain.OperationRestore:
		result, err = p.restoreOp(ctx, rc, op)
	default:
		return engine.OpResult{}, false, nil
	}
	return result, true, err
}
