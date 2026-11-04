// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package weaviate

import (
	"context"
	"errors"
	"fmt"
	"math"
	"net/url"
	"path"
	"regexp"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/weaviate/wvapi"
)

var backupIDPattern = regexp.MustCompile(`^[a-z0-9_-]{1,128}$`)

// Hash the entire identity, rather than truncating/sanitizing it into collisions.
func nativeBackupID(name string) string { return "kk-" + hash(name) }
func backupLocation(m *domain.WeaviateBackup) string {
	return (&url.URL{Scheme: "s3", Host: m.Bucket, Path: "/" + m.Path, Fragment: m.ID}).String()
}
func recordedBackup(storage *domain.BackupStorage, b *domain.Backup) (*domain.WeaviateBackup, error) {
	m := b.Weaviate
	if m == nil || b.Provider != "weaviate" || b.Storage != storage.Name || domain.NormalizedNamespace(b.Namespace) != domain.NormalizedNamespace(storage.Namespace) || m.Endpoint != storage.EndpointURL || m.Bucket != storage.Bucket || m.ClusterUID == "" || !backupIDPattern.MatchString(m.ID) || b.Location != backupLocation(m) {
		return nil, invalid("backup lacks a matching immutable Weaviate S3 identity")
	}
	for _, part := range strings.Split(m.Path, "/") {
		if part == "" || part == "." || part == ".." || strings.Contains(part, "\\") {
			return nil, invalid("unsafe recorded backup path")
		}
	}
	suffix := path.Join(domain.NormalizedNamespace(b.Namespace), b.Cluster, m.ClusterUID)
	if m.Path != suffix && !strings.HasSuffix(m.Path, "/"+suffix) {
		return nil, invalid("backup lacks its complete source cluster namespace")
	}
	return m, nil
}
func nativeLocation(m *domain.WeaviateBackup) wvapi.BackupLocation {
	return wvapi.BackupLocation{Bucket: m.Bucket, Path: m.Path}
}

// Native errors may contain credentials, signed URLs or user data. Persist only
// the enumerated native phase, never an untrusted response body.
func backupFailure(kind string, phase wvapi.BackupState) string {
	return "Weaviate " + kind + " " + string(phase)
}
func terminalBackup(status wvapi.BackupState) bool {
	return status == wvapi.BackupSuccess || status == wvapi.BackupFailed || status == wvapi.BackupCanceled
}

func (p *Provider) runBackup(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp operationState, name string) (engine.OpResult, error) {
	result := func(done bool, err error) (engine.OpResult, error) {
		r, e := operationResult(cp, done)
		return r, errors.Join(err, e)
	}
	if name == "" {
		return result(true, invalid("backup name is required"))
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return result(false, err)
	}
	if b.Type != "full" {
		return result(true, invalid("Weaviate backups require type full"))
	}
	if b.Storage == "" || b.Storage != rc.Cluster.Spec.Backup.Storage {
		return result(true, invalid("backup storage must equal the cluster's bound spec.backup.storage"))
	}
	storage, err := rc.Store.GetBackupStorage(ctx, rc.Cluster.Ref().Namespace, b.Storage)
	if err != nil {
		return result(false, err)
	}
	if cp.Stage == "" {
		if rc.Cluster.Status.Phase != domain.PhaseReady {
			return result(false, fmt.Errorf("%w: backup requires a Ready cluster", engine.ErrConflict))
		}
		ready, e := p.clusterReady(ctx, rc, s)
		if e != nil {
			return result(false, e)
		}
		if !ready {
			return result(false, fmt.Errorf("%w: backup requires all members healthy", engine.ErrConflict))
		}
		if b.Location == "" {
			names := make([]string, 0, len(s.Members))
			for _, m := range ordered(s) {
				names = append(names, m.Name)
			}
			b.Provider = "weaviate"
			b.SourceTopology = rc.Cluster.Spec.Topology
			b.SourceVersion = rc.Cluster.Spec.Version
			b.Weaviate = &domain.WeaviateBackup{Endpoint: storage.EndpointURL, Bucket: storage.Bucket, Path: path.Join(storage.Prefix, rc.Cluster.Ref().Namespace, rc.Cluster.Name, s.UID), ID: nativeBackupID(b.Name), ClusterUID: s.UID, Members: names, MemberCount: len(names)}
			b.Location = backupLocation(b.Weaviate)
			if err = rc.Store.UpdateBackup(ctx, b); err != nil {
				return result(false, err)
			}
		}
		cp.Stage = "submit"
		return result(false, nil)
	}
	m, err := recordedBackup(storage, b)
	if err != nil {
		return result(false, err)
	}
	client, err := p.native(ctx, rc, s)
	if err != nil {
		return result(false, err)
	}
	defer client.Close()
	switch cp.Stage {
	case "submit":
		_, err = client.BackupStatus(ctx, m.ID, nativeLocation(m))
		if wvapi.IsNotFound(err) {
			_, err = client.CreateBackup(ctx, wvapi.BackupRequest{ID: m.ID, Config: wvapi.BackupConfig{Bucket: m.Bucket, Path: m.Path}})
		}
		if err != nil {
			return result(false, err)
		}
		stamp := now(rc)
		b.Phase = domain.PhaseRunning
		if b.StartedAt == nil {
			b.StartedAt = &stamp
		}
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return result(false, err)
		}
		cp.Stage = "poll"
	case "poll":
		info, e := client.BackupStatus(ctx, m.ID, nativeLocation(m))
		if e != nil {
			return result(false, e)
		}
		if !terminalBackup(info.Status) {
			return result(false, nil)
		}
		stamp := now(rc)
		if parsed, e := time.Parse(time.RFC3339Nano, info.CompletedAt); e == nil {
			stamp = parsed
		}
		b.CompletedAt = &stamp
		b.Phase = domain.PhaseSucceeded
		b.Error = ""
		sizeBytes := info.Size * (1024 * 1024 * 1024)
		if !math.IsNaN(sizeBytes) && !math.IsInf(sizeBytes, 0) && sizeBytes > 0 && sizeBytes < float64(math.MaxInt64) {
			b.Size = int64(sizeBytes)
		}
		if info.Status != wvapi.BackupSuccess {
			b.Phase = domain.PhaseFailed
			b.Error = backupFailure("backup", info.Status)
		}
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return result(false, err)
		}
		cp.Stage = "done"
		if b.Error != "" {
			return result(true, errors.New(b.Error))
		}
		return result(true, nil)
	case "done":
		return result(true, nil)
	default:
		return result(false, invalid("unknown backup checkpoint"))
	}
	return result(false, nil)
}
