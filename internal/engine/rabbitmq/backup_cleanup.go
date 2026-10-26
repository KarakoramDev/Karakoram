// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rabbitmq

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// cleanupBackupArtifacts runs only after owned runtimes have gone and volumes
// have been deleted or retained. Its checkpointed window survives CP restarts.
func (p *Provider) cleanupBackupArtifacts(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	if s.BackupCleanupStarted == nil {
		started := now(rc)
		s.BackupCleanupStarted = &started
		return false, nil // Persist the deadline before touching object storage.
	}
	backups, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return false, err
	}
	deadline := s.BackupCleanupStarted.Add(2 * time.Minute)
	for _, b := range backups {
		if slices.Contains(s.RetainedBackups, b.Name) {
			continue
		}
		if b.Provider != "rabbitmq" {
			// Other-engine retained inventory is not owned by this incarnation.
			continue
		}
		if b.Location == "" {
			// Backup creation persists its location before uploading any object.
			return false, rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), b.Name)
		}
		parts := strings.Split(b.Location, "/")
		if b.Provider == "rabbitmq" && len(parts) >= 3 && parts[len(parts)-2] == "definitions" && safeObjectPart(parts[len(parts)-3]) && safeObjectPart(s.UID) && parts[len(parts)-3] != s.UID {
			// Retained backups from an earlier incarnation are not ours to remove.
			continue
		}
		storage, failure := definitionsStorage(ctx, rc, &b)
		if failure == nil {
			failure = definitionsLocation(*storage, b)
			if !safeObjectPart(s.UID) {
				failure = errors.New("missing cluster incarnation ownership")
			}
		}
		if failure == nil && now(rc).Before(deadline) {
			pass, cancel := context.WithTimeout(ctx, deadline.Sub(now(rc)))
			failure = p.DeleteRetainedBackup(pass, *storage, b)
			cancel()
			if failure == nil {
				return false, rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), b.Name)
			}
		}
		if ctx.Err() != nil {
			return false, ctx.Err()
		}
		if now(rc).Before(deadline) {
			// Transient errors retry normally, without exhausting operation retries.
			return false, nil
		}
		// Never persist underlying storage errors: they can contain credentials.
		scope := fmt.Sprintf("backup %q in storage %q, recorded object %q", b.Name, b.Storage, b.Location)
		if storage != nil {
			scope += fmt.Sprintf(", bucket %q", storage.Bucket)
		}
		message := fmt.Sprintf("RabbitMQ definitions artifact may remain: %s (cluster incarnation %q). Runtime cleanup is complete; backup inventory is retained after the two-minute artifact cleanup bound. Restore the original storage binding, verify the recorded object's exact ownership, then remove only that object with object-store tooling and delete its retained backup record. Never guess a missing or ambiguous object location.", scope, s.UID)
		if rc.Events == nil {
			return false, errors.New("cannot record retained RabbitMQ backup artifact outcome")
		}
		if err := warningEvent(ctx, rc, "BackupArtifactsRetained", message); err != nil {
			return false, err
		}
		s.RetainedBackups = append(s.RetainedBackups, b.Name)
		return false, nil
	}
	s.CleanupBackups = true
	return true, nil
}
