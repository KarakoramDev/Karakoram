// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package neo4j

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/neo4j/neo4jspec"
	"github.com/minio/minio-go/v7"
)

func versionParts(v string) ([3]int, error) {
	var result [3]int
	parts := strings.Split(v, ".")
	if len(parts) != 3 {
		return result, engine.Invalid("invalid Neo4j version")
	}
	for i, p := range parts {
		n, err := strconv.Atoi(p)
		if err != nil || n < 0 {
			return result, engine.Invalid("invalid Neo4j version")
		}
		result[i] = n
	}
	return result, nil
}
func validateManifest(m neo4jspec.Manifest, prefix, targetVersion string) error {
	if m.Format != neo4jspec.ManifestFormat || m.Edition != "community" || len(m.Databases) != 2 {
		return engine.Invalid("invalid Neo4j dump manifest")
	}
	source, err := versionParts(m.Neo4jVersion)
	if err != nil {
		return err
	}
	target, err := versionParts(targetVersion)
	if err != nil {
		return err
	}
	for i := range source {
		if source[i] > target[i] {
			return engine.Invalid("backup Neo4j version is newer than target")
		}
		if source[i] < target[i] {
			break
		}
	}
	if _, err = time.Parse(time.RFC3339, m.CreatedAt); err != nil {
		return engine.Invalid("invalid manifest creation time")
	}
	seen := map[string]bool{}
	var size int64
	for _, obj := range m.Databases {
		if (obj.Database != "system" && obj.Database != "neo4j") || seen[obj.Database] || obj.Key != path.Join(prefix, obj.Database+".dump") || obj.Size <= 0 {
			return engine.Invalid("invalid manifest dump object")
		}
		digest, err := hex.DecodeString(obj.SHA256)
		if err != nil || len(digest) != 32 {
			return engine.Invalid("invalid manifest dump checksum")
		}
		if obj.Size > int64(^uint64(0)>>1)-size {
			return engine.Invalid("manifest size overflow")
		}
		size += obj.Size
		seen[obj.Database] = true
	}
	return nil
}
func readManifest(ctx context.Context, storage domain.BackupStorage, prefix, version string) (neo4jspec.Manifest, error) {
	var manifest neo4jspec.Manifest
	ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	client, closeClient, err := backupstorage.NewClient(storage, storage.Credentials)
	if err != nil {
		return manifest, errors.New("cannot initialize backup storage client")
	}
	defer closeClient()
	obj, err := client.GetObject(ctx, storage.Bucket, path.Join(prefix, neo4jspec.ManifestObject), minio.GetObjectOptions{})
	if err != nil {
		return manifest, engine.Invalid("backup manifest unavailable")
	}
	defer obj.Close()
	const maxManifest = 64 * 1024
	raw, err := io.ReadAll(io.LimitReader(obj, maxManifest+1))
	if err != nil {
		return manifest, engine.Invalid("backup manifest unavailable")
	}
	if len(raw) > maxManifest {
		return manifest, engine.Invalid("backup manifest exceeds size limit")
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	if dec.Decode(&manifest) != nil {
		return manifest, engine.Invalid("invalid backup manifest JSON")
	}
	var trailing any
	if dec.Decode(&trailing) != io.EOF {
		return manifest, engine.Invalid("invalid backup manifest JSON")
	}
	return manifest, validateManifest(manifest, prefix, version)
}
func restoreRef(r *domain.Restore, c *domain.Cluster) domain.ClusterRef {
	namespace := r.SourceNamespace
	if namespace == "" {
		namespace = c.Ref().Namespace
	}
	cluster := r.Source.SourceCluster
	if cluster == "" {
		cluster = c.Name
	}
	return domain.ClusterRef{Namespace: namespace, Name: cluster}
}
func persistRestore(c *domain.Cluster, s *state, op *domain.Operation, cp operationCheckpoint) error {
	copyOp := *op
	copyOp.Checkpoint, _ = json.Marshal(cp)
	s.RestoreRecovery = &copyOp
	s.Maintenance = op.ID
	return save(c, s)
}
func (p *Provider) runRestore(ctx context.Context, rc *engine.Context, op *domain.Operation, cp operationCheckpoint) (engine.OpResult, error) {
	name, err := operationResource(op, "restore")
	if err != nil {
		return checkpoint(cp, false), err
	}
	if rc.Store == nil {
		return checkpoint(cp, false), engine.ErrUnavailable
	}
	r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return checkpoint(cp, false), err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return checkpoint(cp, false), err
	}
	if s.RestoreRecovery != nil && s.RestoreRecovery.ID == op.ID {
		if err = json.Unmarshal(s.RestoreRecovery.Checkpoint, &cp); err != nil {
			return checkpoint(cp, false), err
		}
	}
	if s.RestoreRecovery == nil {
		if r.Phase == domain.PhaseSucceeded {
			cp.Step = "done"
			return checkpoint(cp, true), nil
		}
		if r.Phase == domain.PhaseFailed {
			return checkpoint(cp, true), engine.Invalid("Neo4j offline restore failed; inspect the failure and submit a new restore")
		}
	}
	sourceRef := restoreRef(r, rc.Cluster)
	b, err := rc.Store.GetBackup(ctx, sourceRef, r.Source.Backup)
	if err != nil {
		return checkpoint(cp, false), err
	}
	if !b.ProviderMatches("neo4j") {
		return checkpoint(cp, false), engine.Invalid("backup provider mismatch")
	}
	if b.Phase != domain.PhaseSucceeded || b.Type != "dump" || r.Source.PITRTime != nil {
		return checkpoint(cp, false), engine.Invalid("restore requires a successful Neo4j dump")
	}
	prefix, err := storedPrefix(*b)
	if err != nil {
		return checkpoint(cp, false), err
	}
	storage, err := p.backupStorage(ctx, rc, b.Storage, sourceRef.Namespace)
	if err != nil {
		return checkpoint(cp, false), err
	}
	source, err := s3Location(*storage, prefix)
	if err != nil {
		return checkpoint(cp, false), err
	}
	if cp.Step == "" {
		// Read the commit marker from S3 before entering maintenance. A receipt never
		// substitutes for verifying the stored manifest, including on clone.
		manifest, e := readManifest(ctx, *storage, prefix, rc.Cluster.Spec.Version)
		if e != nil {
			return checkpoint(cp, false), e
		}
		if b.SourceVersion != "" && manifest.Neo4jVersion != b.SourceVersion {
			return checkpoint(cp, false), engine.Invalid("manifest version differs from backup provenance")
		}
		cp.Manifest = &manifest
		cp.Step = "restore"
		if err = persistRestore(rc.Cluster, s, op, cp); err != nil {
			return checkpoint(cp, false), err
		}
		stamp := now(rc)
		r.Phase = domain.PhaseRunning
		r.StartedAt = &stamp
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return checkpoint(cp, false), err
		}
		if err = backupEvent(ctx, rc, op, "RestoreOutageStarted", "Neo4j offline restore started; database unavailable until recovery completes"); err != nil {
			return checkpoint(cp, false), err
		}
		return checkpoint(cp, false), nil
	}
	if cp.Manifest == nil {
		return checkpoint(cp, false), engine.Invalid("restore checkpoint has no verified manifest")
	}
	if err = validateManifest(*cp.Manifest, prefix, rc.Cluster.Spec.Version); err != nil {
		return checkpoint(cp, false), err
	}
	if cp.Step == "restore" {
		password, e := secret(ctx, rc, "users/neo4j")
		if e != nil {
			return checkpoint(cp, false), e
		}
		var job neo4jspec.JobResponse
		if err = action(ctx, rc, neo4jspec.ActionRestore, neo4jspec.RestoreRequest{Receipt: op.ID, Source: source, Manifest: *cp.Manifest, AdminPassword: string(password)}, &job); err != nil {
			return checkpoint(cp, false), errors.New("Neo4j restore receipt unavailable")
		}
		switch job.State {
		case neo4jspec.JobRunning:
			return checkpoint(cp, false), nil
		case neo4jspec.JobDone:
			cp.Step = "await-ready"
		case neo4jspec.JobFailed:
			cp.Step = "failed-await-ready"
		default:
			return checkpoint(cp, false), engine.Invalid("invalid Neo4j restore receipt state")
		}
		if err = persistRestore(rc.Cluster, s, op, cp); err != nil {
			return checkpoint(cp, false), err
		}
		return checkpoint(cp, false), nil
	}
	if cp.Step != "await-ready" && cp.Step != "failed-await-ready" {
		return checkpoint(cp, false), engine.Invalid("invalid restore checkpoint")
	}
	ready, err := readiness(ctx, rc)
	if err != nil {
		return checkpoint(cp, false), errors.New("Neo4j restore readiness unavailable")
	}
	if !ready.Ready || ready.CredentialMismatch || ready.ConfigurationError != "" || ready.Version != rc.Cluster.Spec.Version {
		return checkpoint(cp, false), nil
	}
	stamp := now(rc)
	r.CompletedAt = &stamp
	r.Phase = domain.PhaseSucceeded
	failed := cp.Step == "failed-await-ready"
	if failed {
		r.Phase = domain.PhaseFailed
		r.Error = "Neo4j offline restore failed"
	}
	if err = rc.Store.UpdateRestore(ctx, r); err != nil {
		return checkpoint(cp, false), err
	}
	if err = backupEvent(ctx, rc, op, "RestoreOutageEnded", "Neo4j offline restore ended and database readiness verified"); err != nil {
		return checkpoint(cp, false), err
	}
	if !failed && name == "initial-restore" && rc.Cluster.Spec.DataSource != nil {
		s.CloneComplete = true
	}
	s.Maintenance = ""
	s.RestoreRecovery = nil
	if err = save(rc.Cluster, s); err != nil {
		return checkpoint(cp, false), err
	}
	if failed {
		return checkpoint(cp, true), engine.Invalid("Neo4j offline restore failed; inspect the failure and submit a new restore")
	}
	cp.Step = "done"
	return checkpoint(cp, true), nil
}
func (p *Provider) queueClone(ctx context.Context, rc *engine.Context, s *state) error {
	d := rc.Cluster.Spec.DataSource
	if d == nil || s.CloneQueued || s.CloneComplete {
		return nil
	}
	if rc.Store == nil {
		return engine.ErrUnavailable
	}
	namespace := d.Namespace
	if namespace == "" {
		namespace = rc.Cluster.Ref().Namespace
	}
	r := &domain.Restore{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: "initial-restore", SourceNamespace: namespace, Source: domain.RestoreSource{SourceCluster: d.Cluster, Backup: d.Backup, PITRTime: d.PITRTime}, Phase: domain.PhasePending}
	payload, _ := json.Marshal(map[string]string{"restore": r.Name})
	op := &domain.Operation{ID: rc.Cluster.Ref().Key() + "-" + s.UID + "-initial-restore", Namespace: r.Namespace, Cluster: r.Cluster, Kind: domain.OperationRestore, Payload: payload}
	if _, err := rc.Store.GetOperation(ctx, op.ID); missing(err) {
		if err = rc.Store.EnqueueRestore(ctx, r, op); err != nil {
			return err
		}
	} else if err != nil {
		return err
	}
	s.CloneQueued = true
	return nil
}
