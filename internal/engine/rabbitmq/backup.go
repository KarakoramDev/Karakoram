// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/minio/minio-go/v7"
)

// Only object identities and workflow stages belong in checkpoints, never definitions or credentials.
type definitionsCheckpoint struct {
	Step    string `json:"step"`
	Failure string `json:"failure,omitempty"`
}

var definitionsS3Client = backupstorage.NewClient

func definitionsLoadCheckpoint(op *domain.Operation) (definitionsCheckpoint, error) {
	var cp definitionsCheckpoint
	if len(op.Checkpoint) != 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return cp, errors.New("invalid definitions operation checkpoint")
		}
	}
	return cp, nil
}
func definitionsResult(cp definitionsCheckpoint) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{Done: cp.Step == "done", Step: cp.Step, Message: cp.Step, Checkpoint: raw, RequeueAfter: 2 * time.Second}
}
func definitionsPayload(op *domain.Operation, field string) (string, error) {
	var payload map[string]string
	if json.Unmarshal(op.Payload, &payload) != nil || payload[field] == "" {
		return "", fmt.Errorf("operation requires %s", field)
	}
	return payload[field], nil
}
func definitionsEvent(ctx context.Context, rc *engine.Context, op *domain.Operation, reason, message string) error {
	if rc.Events == nil {
		return nil
	}
	kind := "Normal"
	if strings.HasSuffix(reason, "Failed") {
		kind = "Warning"
	}
	return rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Operation: op.ID, Type: kind, Reason: reason, Message: message, CreatedAt: now(rc)})
}
func definitionsStorage(ctx context.Context, rc *engine.Context, b *domain.Backup) (*domain.BackupStorage, error) {
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
func definitionsKey(prefix, uid, name string) (string, error) {
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
	return prefix + uid + "/definitions/" + name + ".json.gz", nil
}

// Recorded locations remain authoritative even if the configured storage prefix changes.
func definitionsLocation(s domain.BackupStorage, b domain.Backup) error {
	if b.Provider != "rabbitmq" || b.Type != "definitions" || b.Storage != s.Name || domain.NormalizedNamespace(b.Namespace) != domain.NormalizedNamespace(s.Namespace) {
		return errors.New("backup is not a RabbitMQ definitions backup in this storage")
	}
	parts := strings.Split(b.Location, "/")
	if len(parts) < 3 || parts[len(parts)-2] != "definitions" || !safeObjectPart(b.Name) || parts[len(parts)-1] != b.Name+".json.gz" {
		return errors.New("backup has no safe recorded definitions object identity")
	}
	for _, part := range parts {
		if !safeObjectPart(part) {
			return errors.New("backup has an unsafe recorded definitions object identity")
		}
	}
	return nil
}
func definitionsS3Error(action string, err error) error {
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	code := minio.ToErrorResponse(err).Code
	// Responses can contain object contents or credentials; expose only fixed S3 error codes.
	switch code {
	case "AccessDenied", "InvalidAccessKeyId", "SignatureDoesNotMatch", "NoSuchBucket", "NoSuchKey", "NoSuchObject":
		return fmt.Errorf("S3 definitions %s failed (%s)", action, code)
	default:
		return fmt.Errorf("S3 definitions %s failed", action)
	}
}
func definitionsMissingObject(err error) bool {
	code := minio.ToErrorResponse(err).Code
	return code == "NoSuchKey" || code == "NoSuchObject" || code == "NotFound"
}

func (p *Provider) backupOp(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	cp, err := definitionsLoadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	if cp.Step == "done" {
		if cp.Failure != "" {
			return definitionsResult(cp), errors.New(cp.Failure)
		}
		return definitionsResult(cp), nil
	}
	name, err := definitionsPayload(op, "backup")
	if err != nil {
		return definitionsResult(cp), err
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return definitionsResult(cp), err
	}
	if b.Type != "definitions" {
		return definitionsResult(cp), errors.New("RabbitMQ only supports definitions backups")
	}
	s, err := definitionsStorage(ctx, rc, b)
	if err != nil {
		return definitionsResult(cp), err
	}
	switch cp.Step {
	case "":
		if b.Location == "" {
			state, e := loadState(rc.Cluster)
			if e != nil {
				return definitionsResult(cp), e
			}
			b.Location, err = definitionsKey(s.Prefix, state.UID, b.Name)
			if err != nil {
				return definitionsResult(cp), err
			}
			b.Provider, b.SourceTopology, b.SourceVersion = "rabbitmq", rc.Cluster.Spec.Topology, rc.Cluster.Spec.Version
		}
		if err = definitionsLocation(*s, *b); err != nil {
			return definitionsResult(cp), err
		}
		stamp := now(rc)
		if b.StartedAt == nil {
			b.StartedAt = &stamp
		}
		b.Phase, b.Error = domain.PhaseRunning, ""
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return definitionsResult(cp), err
		}
		if err = definitionsEvent(ctx, rc, op, "BackupStarted", "RabbitMQ definitions backup started"); err != nil {
			return definitionsResult(cp), err
		}
		cp.Step = "upload"
	case "upload", "complete":
		if err = definitionsLocation(*s, *b); err != nil {
			return definitionsResult(cp), err
		}
		client, closeClient, e := definitionsS3Client(*s, s.Credentials)
		if e != nil {
			return definitionsResult(cp), e
		}
		defer closeClient()
		info, e := client.StatObject(ctx, s.Bucket, b.Location, minio.StatObjectOptions{})
		if e != nil && !definitionsMissingObject(e) {
			return definitionsResult(cp), definitionsS3Error("stat", e)
		}
		if e != nil {
			if cp.Step == "complete" {
				return definitionsResult(cp), errors.New("uploaded definitions object is missing")
			}
			admin, e := p.adminClient(ctx, rc)
			if e != nil {
				return definitionsResult(cp), e
			}
			defer admin.Close()
			data, e := admin.ExportDefinitions(ctx)
			if e != nil {
				return definitionsResult(cp), e
			}
			if !validDefinitions(data) {
				cp.Step, cp.Failure = "failed", "management API returned invalid definitions JSON"
				return definitionsResult(cp), nil
			}
			var compressed bytes.Buffer
			writer := gzip.NewWriter(&compressed)
			if _, e = writer.Write(data); e != nil {
				return definitionsResult(cp), e
			}
			if e = writer.Close(); e != nil {
				return definitionsResult(cp), e
			}
			_, e = client.PutObject(ctx, s.Bucket, b.Location, bytes.NewReader(compressed.Bytes()), int64(compressed.Len()), minio.PutObjectOptions{ContentType: "application/gzip", DisableMultipart: true})
			if e != nil {
				return definitionsResult(cp), definitionsS3Error("upload", e)
			}
			cp.Step = "complete"
			return definitionsResult(cp), nil
		}
		// A successful PUT whose checkpoint write was interrupted is recovered by HEAD.
		b.Size, b.Phase, b.Error = info.Size, domain.PhaseSucceeded, ""
		stamp := now(rc)
		b.CompletedAt = &stamp
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return definitionsResult(cp), err
		}
		if err = definitionsEvent(ctx, rc, op, "BackupCompleted", "RabbitMQ definitions backup succeeded"); err != nil {
			return definitionsResult(cp), err
		}
		cp.Step = "done"
	case "failed":
		b.Phase, b.Error = domain.PhaseFailed, cp.Failure
		stamp := now(rc)
		b.CompletedAt = &stamp
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return definitionsResult(cp), err
		}
		if err = definitionsEvent(ctx, rc, op, "BackupFailed", cp.Failure); err != nil {
			return definitionsResult(cp), err
		}
		cp.Step = "done"
		return definitionsResult(cp), errors.New(cp.Failure)
	default:
		return definitionsResult(cp), errors.New("unknown definitions backup step")
	}
	return definitionsResult(cp), nil
}

func (p *Provider) deleteBackupOp(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	cp, err := definitionsLoadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	if cp.Step == "done" {
		return definitionsResult(cp), nil
	}
	name, err := definitionsPayload(op, "backup")
	if err != nil {
		return definitionsResult(cp), err
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if missing(err) {
		cp.Step = "done"
		return definitionsResult(cp), nil
	}
	if err != nil {
		return definitionsResult(cp), err
	}
	if b.Type != "definitions" || (b.Provider != "" && b.Provider != "rabbitmq") {
		return definitionsResult(cp), errors.New("backup is not a RabbitMQ definitions backup")
	}
	var s *domain.BackupStorage
	if b.Location != "" && cp.Step != "record" {
		s, err = definitionsStorage(ctx, rc, b)
		if err != nil {
			return definitionsResult(cp), err
		}
		if err = definitionsLocation(*s, *b); err != nil {
			return definitionsResult(cp), err
		}
	}
	switch cp.Step {
	case "":
		b.Phase = domain.PhaseDeleting
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return definitionsResult(cp), err
		}
		cp.Step = "delete"
		if b.Location == "" {
			// Object creation is impossible until its location is persisted.
			cp.Step = "record"
		}
	case "delete":
		if s != nil {
			if err = p.DeleteRetainedBackup(ctx, *s, *b); err != nil {
				return definitionsResult(cp), err
			}
		}
		cp.Step = "record"
	case "record":
		if err = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), name); err != nil && !missing(err) {
			return definitionsResult(cp), err
		}
		if err = definitionsEvent(ctx, rc, op, "BackupDeleted", "RabbitMQ definitions backup deleted"); err != nil {
			return definitionsResult(cp), err
		}
		cp.Step = "done"
	default:
		return definitionsResult(cp), errors.New("unknown definitions delete step")
	}
	return definitionsResult(cp), nil
}

// DeleteRetainedBackup deletes exactly the recorded definitions object, without needing its source cluster.
func (p *Provider) DeleteRetainedBackup(ctx context.Context, storage domain.BackupStorage, b domain.Backup) error {
	if err := definitionsLocation(storage, b); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Minute)
	defer cancel()
	client, closeClient, err := definitionsS3Client(storage, storage.Credentials)
	if err != nil {
		return err
	}
	defer closeClient()
	if err = client.RemoveObject(ctx, storage.Bucket, b.Location, minio.RemoveObjectOptions{}); err != nil && !definitionsMissingObject(err) {
		return definitionsS3Error("delete", err)
	}
	return nil
}

var _ engine.RetainedBackupDeleter = (*Provider)(nil)
