// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/KarakoramDev/Karakoram/internal/s3endpoint"
)

func operationResource(op *domain.Operation, key string) (string, error) {
	var payload map[string]string
	if json.Unmarshal(op.Payload, &payload) != nil || payload[key] == "" {
		return "", engine.Invalid("operation has no %s", key)
	}
	return payload[key], nil
}
func (p *Provider) backupStorage(ctx context.Context, rc *engine.Context, name, namespace string) (*domain.BackupStorage, error) {
	st := rc.Store
	if st == nil {
		st = p.options.Store
	}
	if st == nil {
		return nil, engine.ErrUnavailable
	}
	if name == "" {
		return nil, engine.Invalid("backup storage is required")
	}
	return st.GetBackupStorage(ctx, namespace, name)
}
func backupPrefix(storage domain.BackupStorage, c *domain.Cluster, s *state, name string) string {
	return path.Join(strings.Trim(storage.Prefix, "/"), "neo4j", c.Ref().Namespace, c.Name+"-"+s.UID, name)
}
func s3Location(storage domain.BackupStorage, prefix string) (neo4jspec.S3, error) {
	u, err := s3endpoint.Parse(storage.EndpointURL)
	if err != nil || storage.Type != "s3" {
		return neo4jspec.S3{}, engine.Invalid("invalid S3 backup storage")
	}
	return neo4jspec.S3{Endpoint: storage.EndpointURL, Region: storage.Region, Bucket: storage.Bucket, Prefix: prefix,
		PathStyle: s3endpoint.PathStyle(u, storage.Bucket, storage.ForcePathStyle), VerifyTLS: storage.VerifyTLS,
		AccessKey: storage.Credentials.AccessKeyID, SecretKey: storage.Credentials.SecretAccessKey,
		EndpointAllowlist: os.Getenv("KARAKORAM_BACKUP_ENDPOINT_ALLOWLIST")}, nil
}
func backupEvent(ctx context.Context, rc *engine.Context, op *domain.Operation, reason, message string) error {
	if rc.Events == nil {
		return nil
	}
	return rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Operation: op.ID, Type: "Normal", Reason: reason, Message: message, CreatedAt: now(rc)})
}
func (p *Provider) runBackup(ctx context.Context, rc *engine.Context, op *domain.Operation, cp operationCheckpoint) (engine.OpResult, error) {
	name, err := operationResource(op, "backup")
	if err != nil {
		return checkpoint(cp, false), err
	}
	if rc.Store == nil {
		return checkpoint(cp, false), engine.ErrUnavailable
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return checkpoint(cp, false), err
	}
	if b.Type != neo4jspec.BackupTypeDump {
		return checkpoint(cp, false), engine.Invalid("Neo4j supports only dump backups")
	}
	if b.Provider != "" && !b.ProviderMatches("neo4j") {
		return checkpoint(cp, false), engine.Invalid("backup provider mismatch")
	}
	storage, err := p.backupStorage(ctx, rc, b.Storage, rc.Cluster.Ref().Namespace)
	if err != nil {
		return checkpoint(cp, false), err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return checkpoint(cp, false), err
	}
	if s.UID == "" {
		return checkpoint(cp, false), engine.Invalid("cluster identity is missing")
	}
	prefix := backupPrefix(*storage, rc.Cluster, s, b.Name)
	if b.PBMPrefix != "" {
		prefix = b.PBMPrefix
	}
	// A replay may only use this incarnation's immutable destination.
	if _, err = storedPrefix(domain.Backup{Namespace: b.Namespace, Cluster: b.Cluster, Name: b.Name, Provider: "neo4j", Type: b.Type, PBMPrefix: prefix}); err != nil || path.Base(path.Dir(prefix)) != rc.Cluster.Name+"-"+s.UID {
		return checkpoint(cp, false), engine.Invalid("backup destination does not belong to this cluster incarnation")
	}
	target, err := s3Location(*storage, prefix)
	if err != nil {
		return checkpoint(cp, false), err
	}
	if b.Phase == domain.PhaseSucceeded {
		cp.Step = "done"
		return checkpoint(cp, true), nil
	}
	if b.Phase == domain.PhaseFailed {
		return checkpoint(cp, false), errors.New("Neo4j offline backup failed")
	}
	if cp.Step == "" {
		stamp := now(rc)
		b.StartedAt = &stamp
		b.Phase = domain.PhaseRunning
		b.Provider = "neo4j"
		b.PBMPrefix = prefix
		b.Location = "s3://" + storage.Bucket + "/" + prefix
		b.SourceTopology = rc.Cluster.Spec.Topology
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return checkpoint(cp, false), err
		}
		if err = backupEvent(ctx, rc, op, "BackupOutageStarted", "Neo4j offline dump started; database unavailable until the member restarts"); err != nil {
			return checkpoint(cp, false), err
		}
		cp.Step = "backup"
		return checkpoint(cp, false), nil
	}
	var job neo4jspec.JobResponse
	if err = action(ctx, rc, neo4jspec.ActionBackup, neo4jspec.BackupRequest{Receipt: op.ID, Target: target}, &job); err != nil {
		return checkpoint(cp, false), errors.New("Neo4j backup receipt unavailable")
	}
	switch job.State {
	case neo4jspec.JobRunning:
		return checkpoint(cp, false), nil
	case neo4jspec.JobDone:
		if job.Manifest == nil {
			return checkpoint(cp, false), engine.Invalid("completed backup has no manifest")
		}
		if err = validateManifest(*job.Manifest, prefix, rc.Cluster.Spec.Version); err != nil {
			return checkpoint(cp, false), err
		}
		b.Size = 0
		for _, obj := range job.Manifest.Databases {
			b.Size += obj.Size
		}
		b.SourceVersion = job.Manifest.Neo4jVersion
		b.Provider = "neo4j"
		b.Phase = domain.PhaseSucceeded
		stamp := now(rc)
		b.CompletedAt = &stamp
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return checkpoint(cp, false), err
		}
		if err = backupEvent(ctx, rc, op, "BackupOutageEnded", "Neo4j offline dump completed and database restarted"); err != nil {
			return checkpoint(cp, false), err
		}
		cp.Step = "done"
		return checkpoint(cp, true), nil
	case neo4jspec.JobFailed:
		// The agent has released its stop hold before returning a failed receipt.
		cleanupCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		_ = deleteBackupObjects(cleanupCtx, *storage, *b)
		cancel()
		b.Phase = domain.PhaseFailed
		b.Error = "Neo4j offline backup failed"
		stamp := now(rc)
		b.CompletedAt = &stamp
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return checkpoint(cp, false), err
		}
		if err = backupEvent(ctx, rc, op, "BackupOutageEnded", "Neo4j offline dump failed; member restart completed"); err != nil {
			return checkpoint(cp, false), err
		}
		return checkpoint(cp, false), errors.New(b.Error)
	default:
		return checkpoint(cp, false), engine.Invalid("invalid Neo4j backup receipt state")
	}
}
