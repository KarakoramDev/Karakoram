// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package openbao

import (
	"context"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/orphanvolumes"
	"strings"
	"time"
)

const orphanVolumeFailures = 5
const orphanVolumeAge = 30 * time.Minute

func volumeDelete(action string) bool { return action == "delete-volume" || action == "cleanup-volume" }
func (p *Provider) orphanVolume(ctx context.Context, rc *engine.Context, s *internalState, v *pending, r *platformRetry) (bool, error) {
	if !strings.Contains(strings.ToLower(r.Message), "still mounted") || r.StillMountedFailures < orphanVolumeFailures || r.FirstFailedAt.IsZero() || now(rc).Before(r.FirstFailedAt.Add(orphanVolumeAge)) {
		return false, nil
	}
	m := s.find(v.Member)
	if m == nil {
		return false, engine.ErrNotFound
	}
	runtime, err := rc.Backend.GetService(ctx, m.Name)
	if err != nil {
		return false, err
	}
	if runtime != nil {
		return false, nil
	}
	if err = verifyMemberServices(ctx, rc, m); err != nil {
		return false, err
	}
	volume, err := rc.Backend.GetService(ctx, m.Volume)
	if err != nil {
		return false, err
	}
	if volume == nil {
		return false, nil
	}
	if m.VolumeID == "" || volume.ID != m.VolumeID {
		return false, engine.ErrConflict
	}
	record, err := rc.Store.GetOrphanedVolume(ctx, rc.Cluster.Ref().Namespace, m.Volume)
	if err != nil && !missing(err) {
		return false, err
	}
	if record != nil && record.ServiceID != m.VolumeID {
		return false, engine.ErrConflict
	}
	if record == nil {
		at := now(rc)
		record = &domain.OrphanedVolume{Namespace: rc.Cluster.Ref().Namespace, Hostname: m.Volume, ServiceID: m.VolumeID, Cluster: rc.Cluster.Name, Member: m.Name, FirstError: r.FirstError, LastError: r.Message, FirstFailedAt: r.FirstFailedAt, OrphanedAt: at, LastAttemptAt: r.LastAttemptAt, NextAttemptAt: at.Add(orphanvolumes.InitialBackoff), Attempts: r.Failures, LastProcessID: r.LastProcessID}
		if err = rc.Store.PutOrphanedVolume(ctx, record); err != nil {
			return false, err
		}
	}
	if rc.Events != nil {
		if err = rc.Events.Record(ctx, domain.Event{Namespace: record.Namespace, Cluster: record.Cluster, Type: "Warning", Reason: "VolumeOrphaned", Message: fmt.Sprintf("volume %s (%s) handed to cleanup after %d failures", record.Hostname, record.ServiceID, record.Attempts), CreatedAt: now(rc)}); err != nil {
			return false, err
		}
	}
	m.VolumeOrphaned = true
	s.resetPlatformRetry(v)
	s.Pending = nil
	if v.Action == "delete-volume" {
		if err = deleteMemberSecrets(ctx, rc, m.Name); err != nil {
			return true, err
		}
		removeRecord(s, m.Name)
		delete(rc.Cluster.Status.DeploymentFailures, m.Name)
	}
	return true, nil
}

// Retained and janitor-owned identities are never allocated to a fresh member.
func allocateNewMember(ctx context.Context, rc *engine.Context, s *internalState) (*member, error) {
	retained, err := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
	if err != nil {
		return nil, err
	}
	orphans, err := rc.Store.ListOrphanedVolumes(ctx, rc.Cluster.Ref().Namespace)
	if err != nil {
		return nil, err
	}
	held := map[string]bool{}
	for _, v := range retained {
		held[v.Hostname] = true
		held[v.Member] = true
	}
	for _, v := range orphans {
		held[v.Hostname] = true
		held[v.Member] = true
	}
	for {
		m := s.add(rc.Cluster.Name)
		if held[m.Name] || held[m.Volume] {
			removeRecord(s, m.Name)
			continue
		}
		return m, nil
	}
}
