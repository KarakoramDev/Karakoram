// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/orphanvolumes"
)

const (
	orphanVolumeFailures = 5
	orphanVolumeAge      = 30 * time.Minute
)

func volumeDelete(action string) bool { return action == "delete-volume" || action == "cleanup-volume" }

// orphanVolume transfers responsibility only after checking runtime absence and
// volume identity again. The member flag prevents all subsequent cleanup passes
// (including after a restart) from touching a janitor-owned volume.
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
	if err := verifyMemberServices(ctx, rc, m); err != nil {
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
	// Reuse an existing handoff after a crash, without resetting janitor state.
	record, err := rc.Store.GetOrphanedVolume(ctx, rc.Cluster.Ref().Namespace, m.Volume)
	created := missing(err)
	if err != nil && !created {
		return false, err
	}
	if !created && record.ServiceID != m.VolumeID {
		return false, engine.ErrConflict
	}
	if created {
		at := now(rc)
		lastAttempt := r.LastAttemptAt
		if lastAttempt.IsZero() {
			lastAttempt = r.FirstFailedAt
		}
		record = &domain.OrphanedVolume{Namespace: rc.Cluster.Ref().Namespace, Hostname: m.Volume, ServiceID: m.VolumeID, Cluster: rc.Cluster.Name, Member: m.Name, FirstError: r.FirstError, LastError: r.Message, FirstFailedAt: r.FirstFailedAt, OrphanedAt: at, LastAttemptAt: lastAttempt, NextAttemptAt: at.Add(orphanvolumes.InitialBackoff), Attempts: r.Failures, LastProcessID: r.LastProcessID}
		if err := rc.Store.PutOrphanedVolume(ctx, record); err != nil {
			return false, err
		}
	}
	if rc.Events != nil {
		if err := rc.Events.Record(ctx, domain.Event{Namespace: record.Namespace, Cluster: record.Cluster, Type: "Warning", Reason: "VolumeOrphaned", Message: fmt.Sprintf("volume %s (%s) orphaned after %d delete failures; cluster work continues and the janitor keeps retrying", record.Hostname, record.ServiceID, record.Attempts), CreatedAt: now(rc)}); err != nil {
			return false, err
		}
	}
	m.VolumeOrphaned = true
	s.resetPlatformRetry(v)
	s.Pending = nil
	if v.Action == "delete-volume" {
		if err := deleteMemberSecrets(ctx, rc, m.Name); err != nil {
			return true, err
		}
		removeRecord(s, m.Name)
	}
	return true, nil
}

// allocateNewPlans checks hostnames only for freshly allocated members. Persisted
// members remain subject to ownership checks; allocation never adopts an old
// cluster's volume merely because its hostname matches.
func allocateNewPlans(ctx context.Context, rc *engine.Context, s *internalState, members []*member) error {
	for _, m := range members {
		m.NeedsAllocation = true
	}
	return skipOccupiedPlans(ctx, rc, s, members)
}

func skipOccupiedPlans(ctx context.Context, rc *engine.Context, s *internalState, members []*member) error {
	orphans, err := rc.Store.ListOrphanedVolumes(ctx, rc.Cluster.Ref().Namespace)
	if err != nil {
		return err
	}
	held := make(map[string]bool, len(orphans)*2)
	for _, v := range orphans {
		held[v.Hostname], held[v.Member] = true, true
	}
	for _, m := range members {
		if !m.NeedsAllocation || m.Stage != "planned" || m.ServiceID != "" || m.VolumeID != "" {
			continue
		}
		for {
			occupied := held[m.Name] || held[m.Volume]
			if !occupied {
				for _, name := range []string{m.Name, m.Volume} {
					if name == "" {
						continue
					}
					service, err := rc.Backend.GetService(ctx, name)
					if err != nil {
						return err
					}
					if service != nil {
						occupied = true
						break
					}
				}
			}
			if !occupied {
				m.NeedsAllocation = false
				break
			}
			if m.Role == agentapi.RoleRouter {
				return fmt.Errorf("%w: router hostname %s already exists", engine.ErrConflict, m.Name)
			}
			m.Index = s.NextIndex[m.Set]
			s.NextIndex[m.Set] = m.Index + 1
			m.Name = memberName(rc.Cluster.Name, m.Set, m.Role, m.Shard, m.Index)
			m.Volume = m.Name + "v"
		}
	}
	return nil
}
