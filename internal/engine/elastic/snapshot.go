// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/elastic/esapi"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func snapshotName(name string) string {
	lower := strings.ToLower(name)
	var out strings.Builder
	for _, r := range lower {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			out.WriteRune(r)
		} else {
			out.WriteByte('-')
		}
	}
	value := strings.Trim(out.String(), "-_")
	if value == "" {
		value = "snapshot"
	}
	if value != name {
		value += "-" + secrets.Hash(name)[:8]
	}
	return value
}

// snapshotLocation is an immutable S3 repository reference, not a blob key:
// s3://<bucket>/<base_path>?repository=<repository>#<snapshot>. Elasticsearch
// snapshots share blobs, so neither the fragment nor its UUID is a delete prefix.
func snapshotLocation(storage *domain.BackupStorage, base, repo, name string) string {
	return (&url.URL{Scheme: "s3", Host: storage.Bucket, Path: "/" + base, RawQuery: url.Values{"repository": {repo}}.Encode(), Fragment: name}).String()
}
func recordedSnapshot(storage *domain.BackupStorage, b *domain.Backup) (base, repo, name string, err error) {
	u, e := url.Parse(b.Location)
	if e != nil || u.Scheme != "s3" || u.Host != storage.Bucket || u.User != nil || b.Provider != "elasticsearch" || b.Storage != storage.Name {
		return "", "", "", operationInvalid("backup lacks a recorded Elasticsearch repository identity")
	}
	base = strings.TrimPrefix(u.Path, "/")
	repo = u.Query().Get("repository")
	name = u.Fragment
	if base == "" || repo == "" || name == "" || strings.ContainsAny(name, "/\\ ,\"*?#:") || strings.ToLower(name) != name {
		return "", "", "", operationInvalid("unsafe snapshot repository identity")
	}
	for _, part := range strings.Split(base, "/") {
		if part == "" || part == "." || part == ".." || strings.Contains(part, "\\") {
			return "", "", "", operationInvalid("unsafe snapshot base path")
		}
	}
	prefix := strings.Trim(storage.Prefix, "/")
	if prefix != "" && !strings.HasPrefix(base, prefix+"/") {
		return "", "", "", operationInvalid("snapshot base path is outside the storage prefix")
	}
	return base, repo, name, nil
}

func (p *Provider) runBackup(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp operationState, name string) (engine.OpResult, error) {
	if name == "" {
		return operationResult(cp, true), operationInvalid("backup name is required")
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return operationResult(cp, false), err
	}
	if b.Type != "snapshot" {
		return operationResult(cp, true), operationInvalid("Elasticsearch backups require type snapshot")
	}
	storage, err := rc.Store.GetBackupStorage(ctx, rc.Cluster.Ref().Namespace, b.Storage)
	if err != nil {
		return operationResult(cp, false), err
	}
	repo, err := p.operationRepository(ctx, rc, s, storage, false, "")
	if errors.Is(err, errWaiting) {
		return operationResult(cp, false), nil
	}
	if err != nil {
		return operationResult(cp, false), err
	}
	snapshot := snapshotName(b.Name)
	if b.Location != "" {
		_, _, snapshot, err = recordedSnapshot(storage, b)
		if err != nil {
			return operationResult(cp, false), err
		}
	}
	client, err := p.es(ctx, rc, s)
	if err != nil {
		return operationResult(cp, false), err
	}
	defer client.Close()
	switch cp.Stage {
	case "":
		b.Provider = "elasticsearch"
		b.Location = snapshotLocation(storage, clusterBasePath(storage, rc.Cluster, s), repo, snapshot)
		b.SourceTopology = rc.Cluster.Spec.Topology
		b.SourceVersion = rc.Cluster.Spec.Version
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "start"
	case "start":
		// The stable name is the receipt: a crash after PUT but before checkpoint
		// persistence is recovered by GET, without launching a second snapshot.
		_, err = client.GetSnapshot(ctx, repo, snapshot)
		if esapi.IsNotFound(err) {
			err = client.CreateSnapshot(ctx, repo, snapshot, esapi.SnapshotRequest{Indices: "*", IncludeGlobalState: true}, false)
			if err != nil {
				if _, observeErr := client.GetSnapshot(ctx, repo, snapshot); observeErr != nil {
					return operationResult(cp, false), err
				}
			}
		} else if err != nil {
			return operationResult(cp, false), err
		}
		b.Phase = domain.PhaseRunning
		stamp := operationNow(rc)
		if b.StartedAt == nil {
			b.StartedAt = &stamp
		}
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return operationResult(cp, false), err
		}
		if err = operationEvent(ctx, rc, op, "BackupStarted", "Elasticsearch snapshot started"); err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "poll"
	case "poll":
		info, e := client.GetSnapshot(ctx, repo, snapshot)
		if e != nil {
			return operationResult(cp, false), e
		}
		if !info.StartTime.IsZero() {
			b.StartedAt = &info.StartTime
		}
		if info.Version != "" {
			b.SourceVersion = info.Version
		}
		switch info.State {
		case "SUCCESS":
			b.Phase = domain.PhaseSucceeded
			b.Error = ""
			if size, sizeErr := client.SnapshotSize(ctx, repo, snapshot); sizeErr == nil {
				b.Size = size
			}
		case "PARTIAL", "FAILED", "INCOMPATIBLE":
			b.Phase = domain.PhaseFailed
			b.Error = "Elasticsearch snapshot " + info.State
			if info.Reason != "" {
				b.Error += ": " + info.Reason
			}
			for _, failure := range info.Failures {
				if failure.Reason != "" {
					b.Error += ": " + failure.Reason
					break
				}
			}
			cp.Failure = b.Error
		default:
			return operationResult(cp, false), nil
		}
		stamp := info.EndTime
		if stamp.IsZero() {
			stamp = operationNow(rc)
		}
		b.CompletedAt = &stamp
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return operationResult(cp, false), err
		}
		if err = operationEvent(ctx, rc, op, "BackupCompleted", "Elasticsearch snapshot: "+string(b.Phase)); err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "done"
		if cp.Failure != "" {
			return operationResult(cp, true), errors.New(cp.Failure)
		}
		return operationResult(cp, true), nil
	default:
		return operationResult(cp, false), operationInvalid("unknown snapshot checkpoint")
	}
	return operationResult(cp, false), nil
}

func (p *Provider) runDeleteBackup(ctx context.Context, rc *engine.Context, s *state, op *domain.Operation, cp operationState, name string) (engine.OpResult, error) {
	if name == "" {
		return operationResult(cp, true), operationInvalid("backup name is required")
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if operationMissing(err) {
		cp.Stage = "done"
		return operationResult(cp, true), nil
	}
	if err != nil {
		return operationResult(cp, false), err
	}
	if cp.Stage == "" {
		b.Phase = domain.PhaseDeleting
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return operationResult(cp, false), err
		}
		cp.Stage = "delete"
		return operationResult(cp, false), nil
	}
	storage, err := rc.Store.GetBackupStorage(ctx, rc.Cluster.Ref().Namespace, b.Storage)
	if err != nil {
		return operationResult(cp, false), err
	}
	base, _, snapshot, err := recordedSnapshot(storage, b)
	if err != nil {
		return operationResult(cp, false), err
	}
	repo, err := p.operationRepository(ctx, rc, s, storage, false, base)
	if errors.Is(err, errWaiting) {
		return operationResult(cp, false), nil
	}
	if err != nil {
		return operationResult(cp, false), err
	}
	client, err := p.es(ctx, rc, s)
	if err != nil {
		return operationResult(cp, false), err
	}
	defer client.Close()
	if cp.Stage == "delete" {
		if err = client.DeleteSnapshot(ctx, repo, snapshot); err != nil && !esapi.IsNotFound(err) {
			return operationResult(cp, false), err
		}
		cp.Stage = "observe"
		return operationResult(cp, false), nil
	}
	if cp.Stage != "observe" {
		return operationResult(cp, false), operationInvalid("unknown snapshot deletion checkpoint")
	}
	if _, err = client.GetSnapshot(ctx, repo, snapshot); !esapi.IsNotFound(err) {
		return operationResult(cp, false), err
	}
	if err = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), b.Name); err != nil && !operationMissing(err) {
		return operationResult(cp, false), err
	}
	if err = operationEvent(ctx, rc, op, "BackupDeleted", fmt.Sprintf("Elasticsearch snapshot %s deleted", b.Name)); err != nil {
		return operationResult(cp, false), err
	}
	cp.Stage = "done"
	return operationResult(cp, true), nil
}
