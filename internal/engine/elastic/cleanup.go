// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package elastic

import (
	"context"
	"strings"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
)

// Cleanup retires all runtime writers and optional volumes before bounded,
// control-plane cleanup of exact owned repository prefixes.
func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return false, err
	}
	// Backend-owned deletion fences survive this whole-cluster takeover.
	for _, m := range s.Members {
		m.UnplacedRetirement = nil
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	seedExposure(rc, s)
	if err = save(rc.Cluster, s); err != nil {
		return false, err
	}
	if busy, e := drainRuntimeRecovery(ctx, rc, s); busy || e != nil {
		return false, e
	}
	s.Phase = "cleanup"
	rc.Cluster.Status.Message = "Deleting: removing Elasticsearch runtime services and owned resources"
	if s.Pending != nil && engine.CleanupSupersedesDeployment(rc.Backend, s.Pending.Op) {
		delete(s.PlatformRetries, retryKey(s.Pending))
		s.Pending = nil
	}
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
			m.DrainIdleObservations, m.DrainShardCount, m.DrainWarning = 0, 0, ""
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
	if !opts.KeepBackups && !s.CleanupBackups {
		if done, e := p.cleanupBackupArtifacts(ctx, rc, s); !done || e != nil {
			return false, e
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
