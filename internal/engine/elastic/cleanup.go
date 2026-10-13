// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// Cleanup removes owned snapshots while the cluster is alive, then components,
// node runtimes, volumes and secrets. Unreachable clusters retain snapshots and
// emit a warning rather than blocking infrastructure deletion indefinitely.
func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return false, err
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	s.Phase = "cleanup"
	if opts.KeepVolumes {
		if s.Rolling != nil {
			s.ResumeAllocationReset = true
			s.Rolling = nil
		}
		s.DrainBlocked = ""
		for _, m := range s.Members {
			if m.AllocationExcluded {
				s.ResumeExcludeNames = append(s.ResumeExcludeNames, m.Name)
				m.AllocationExcluded = false
			}
			if masterEligible(rc.Cluster.Spec, m) && (m.Stage == stageExcluded || m.Stage == stageDeleting || m.Stage == "delete-volume" || m.Stage == "removed") {
				s.ResumeVotingReset = true
			}
			m.Removing = false
			m.Replaces = ""
		}
	}
	if busy, e := p.observePending(ctx, rc, s); busy || e != nil {
		return false, e
	}
	if s.Retry != nil && !strings.HasPrefix(s.Retry.Action, "cleanup-") {
		s.Retry = nil
	}
	if opts.KeepVolumes && s.Retry != nil && s.Retry.Action == "cleanup-volume" {
		s.Retry = nil
	}
	if busy, _, e := p.retryPending(ctx, rc, s); busy || e != nil {
		return false, e
	}
	if !opts.KeepBackups && !s.CleanupBackups {
		owned := false
		for _, repo := range s.Repositories {
			owned = owned || !repo.ReadOnly
		}
		if !owned {
			s.CleanupBackups = true
			return false, nil
		}
		c, e := p.es(ctx, rc, s)
		reachable := e == nil
		if reachable {
			_, e = c.Info(ctx)
			reachable = e == nil
		}
		if !reachable {
			if c != nil {
				c.Close()
			}
			if rc.Events != nil {
				if e = rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Type: "Warning", Reason: "SnapshotsRetained", Message: "Elasticsearch is unreachable; snapshots remain in backup storage and infrastructure deletion continues", CreatedAt: now(rc)}); e != nil {
					return false, e
				}
			}
			s.CleanupBackups = true
			return false, nil
		}
		defer c.Close()
		for _, key := range sortedKeys(s.Repositories) {
			repo := s.Repositories[key]
			if repo.ReadOnly {
				continue
			}
			snapshots, e := c.ListSnapshots(ctx, repo.Name)
			if e != nil {
				return false, e
			}
			if len(snapshots) > 0 {
				if e = c.DeleteSnapshot(ctx, repo.Name, snapshots[0].Snapshot); e != nil {
					return false, e
				}
				return false, nil
			}
		}
		s.CleanupBackups = true
		return false, nil
	}
	ordered := append(s.componentMembers(), s.esNodes()...)
	for _, m := range ordered {
		if err = verifyMemberServices(ctx, rc, m); err != nil {
			return false, err
		}
		if m.PublicPort > 0 {
			if err = rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
				return false, err
			}
			m.PublicPort = 0
			m.ExposureHash = ""
			return false, nil
		}
		if m.Subdomain {
			return false, p.issueAction(ctx, rc, s, &pending{Action: "subdomain-off", Member: m.Name})
		}
		runtime, e := rc.Backend.GetService(ctx, m.Name)
		if e != nil {
			return false, e
		}
		if runtime != nil {
			return false, p.issueAction(ctx, rc, s, &pending{Action: "cleanup-runtime", Member: m.Name})
		}
		if m.Volume == "" || m.VolumeOrphaned {
			continue
		}
		volume, e := rc.Backend.GetService(ctx, m.Volume)
		if e != nil {
			return false, e
		}
		if volume == nil {
			continue
		}
		if opts.KeepVolumes {
			retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
			if e != nil {
				return false, e
			}
			exists := false
			for _, v := range retained {
				exists = exists || v.Member == m.Name && v.ServiceID == volume.ID
			}
			if !exists {
				if e = rc.Store.CreateRetainedVolume(ctx, &domain.RetainedVolume{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Hostname: m.Volume, ServiceID: volume.ID, Role: m.Kind, Index: m.Index, RetainedAt: now(rc)}); e != nil {
					return false, e
				}
				return false, nil
			}
		} else {
			return false, p.issueAction(ctx, rc, s, &pending{Action: "cleanup-volume", Member: m.Name})
		}
	}
	if opts.KeepVolumes {
		s.Suspended = true
		rc.Cluster.Status.Phase = domain.PhaseSuspended
		return true, nil
	}
	if !s.CleanupSecrets {
		for _, m := range s.Members {
			if err = deleteMemberSecrets(ctx, rc, m.Name); err != nil {
				return false, err
			}
			if rc.Store != nil {
				if err = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
					return false, err
				}
			}
		}
		for _, key := range clusterSecretKeys {
			if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
				return false, err
			}
		}
		s.CleanupSecrets = true
	}
	return true, nil
}
