// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"errors"
	"fmt"
	"net/url"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// cleanupBackupArtifacts runs only after authoritative runtime retirement. Its
// timestamp and per-repository checkpoints survive controller restarts.
func (p *Provider) cleanupBackupArtifacts(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.BackupCleanupStarted == nil {
		started := now(rc)
		s.BackupCleanupStarted = &started
		return false, nil // Publish the deadline before external side effects.
	}
	backups, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return false, err
	}
	for _, key := range sortedKeys(s.Repositories) {
		repo := s.Repositories[key]
		if repo.ReadOnly || slices.Contains(s.RetainedRepositories, key) {
			continue
		}
		if !slices.Contains(s.CleanedRepositories, key) {
			namespace, name := storageRef(rc.Cluster.Ref().Namespace, key)
			storage, failure := rc.Store.GetBackupStorage(ctx, namespace, name)
			if failure == nil {
				for _, b := range backups {
					location, e := url.Parse(b.Location)
					if e != nil || location.Query().Get("repository") != repo.Name || strings.TrimPrefix(location.Path, "/") != repo.BasePath {
						continue
					}
					if _, _, _, e := recordedSnapshot(storage, &b); e != nil {
						failure = e
					}
				}
			}
			deadline := s.BackupCleanupStarted.Add(2 * time.Minute)
			if failure == nil {
				if !now(rc).Before(deadline) {
					failure = errors.New("artifact cleanup deadline exhausted")
				} else {
					pass, cancel := context.WithTimeout(ctx, deadline.Sub(now(rc)))
					failure = backupstorage.DeleteElasticsearchRepository(pass, *storage, rc.Cluster.Ref().Namespace, rc.Cluster.Name, s.UID, repo.BasePath)
					cancel()
				}
			}
			if failure != nil {
				if ctx.Err() != nil {
					return false, ctx.Err()
				}
				if now(rc).Before(deadline) {
					return false, nil
				} // Transient storage/binding failures requeue without exhausting operation retries.
				if rc.Events == nil {
					return false, errors.New("cannot record retained snapshot outcome")
				}
				message := fmt.Sprintf("Snapshots may remain in storage %q repository %q exact recorded prefix %q (cluster incarnation %q). Infrastructure has retired; backup inventory is retained. Restore the original storage binding, verify exact incarnation ownership, remove only this owned repository prefix with object-store tooling, then remove retained backup records. Never guess an ambiguous prefix or delete a read-only source repository. Cleanup exceeded its two-minute bound.", key, repo.Name, repo.BasePath+"/", s.UID)
				if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "SnapshotsRetained", Message: message, CreatedAt: now(rc)}); err != nil {
					return false, err
				}
				s.RetainedRepositories = append(s.RetainedRepositories, key)
				return false, nil
			}
			s.CleanedRepositories = append(s.CleanedRepositories, key)
			return false, nil // Persist successful object cleanup before inventory removal.
		}
		for _, b := range backups {
			location, err := url.Parse(b.Location)
			if err != nil || b.Provider != "elasticsearch" || location.Query().Get("repository") != repo.Name || strings.TrimPrefix(location.Path, "/") != repo.BasePath || location.Fragment == "" {
				continue
			}
			if err := rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), b.Name); err != nil && !missing(err) {
				return false, err
			}
		}
	}
	s.CleanupBackups = true
	return true, nil
}
