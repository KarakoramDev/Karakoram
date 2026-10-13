// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package psmdb

import (
	"context"
	"slices"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/pbm"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb/users"
)

// Cleanup deletes backup objects first, then public routes, runtimes, and optionally persistent data/secrets.
// Keeping volumes preserves the complete member plan and credentials for resume.
func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, e := load(rc.Cluster)
	if e != nil {
		return false, e
	}
	defer func() {
		if e := save(rc.Cluster, s); err == nil {
			err = e
		}
	}()
	s.Phase = "cleanup"
	// Preserve deletion evidence when a cluster is deleted during scale-down.
	// Keeping volumes discards queued retries, but an already-issued deletion
	// must reach a terminal result before any volume is recorded as retained.
	for _, v := range []*pending{s.Retry, s.Pending} {
		if v == nil {
			continue
		}
		if opts.KeepVolumes && volumeDelete(v.Action) {
			s.resetPlatformRetry(v)
			continue
		}
		if v.Action == "delete-volume" || v.Action == "delete-runtime" {
			oldKey := platformRetryKey(v)
			if v.Action == "delete-volume" {
				v.Action = "cleanup-volume"
			} else {
				v.Action = "cleanup-runtime"
			}
			if r := s.PlatformRetries[oldKey]; r != nil {
				s.PlatformRetries[platformRetryKey(v)] = r
				delete(s.PlatformRetries, oldKey)
			}
		}
	}
	// Capture verified legacy identities while runtimes still exist; deletion
	// must not destroy the ownership evidence needed for their attached volumes.
	for _, m := range s.Members {
		if e = verifyMemberServices(ctx, rc, m); e != nil {
			return false, e
		}
	}
	// Deletion supersedes provisioning retries, but retains deletion backoff.
	if s.Retry != nil && s.Retry.Action != "cleanup-runtime" && s.Retry.Action != "cleanup-volume" {
		s.resetPlatformRetry(s.Retry)
	}
	for _, m := range s.Members {
		oldKey := m.Name + "/delete-volume"
		if r := s.PlatformRetries[oldKey]; r != nil {
			if !opts.KeepVolumes && !m.VolumeOrphaned {
				key := m.Name + "/cleanup-volume"
				if s.PlatformRetries[key] == nil {
					s.PlatformRetries[key] = r
				}
				if s.Retry == nil && s.Pending == nil {
					s.Retry = &pending{Action: "cleanup-volume", Member: m.Name}
				}
			}
			delete(s.PlatformRetries, oldKey)
		}
	}
	if s.Pending != nil {
		if s.Pending.Op.ID != "" {
			st, e := rc.Backend.OpStatus(ctx, s.Pending.Op)
			if e != nil {
				return false, e
			}
			if st.Failed && (s.Pending.Action == "cleanup-runtime" || s.Pending.Action == "cleanup-volume") && !(opts.KeepVolumes && volumeDelete(s.Pending.Action)) {
				return false, s.failPending(rc, st.Message)
			}
			if !st.Done && !st.Failed {
				return false, nil
			}
		}
		s.resetPlatformRetry(s.Pending)
		s.Pending = nil
		return false, nil
	}
	if busy, e := p.retryPending(ctx, rc, s); busy || e != nil {
		return false, e
	}
	if !opts.KeepBackups && !s.CleanupBackups {
		storages := append([]string(nil), s.UsedStorages...)
		if s.PBMStorage != "" && !slices.Contains(storages, s.PBMStorage) {
			storages = append(storages, s.PBMStorage)
		}
		backups, e := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
		if e != nil {
			return false, e
		}
		for _, b := range backups {
			if b.Storage != "" && !slices.Contains(storages, b.Storage) {
				storages = append(storages, b.Storage)
			}
		}
		for _, storage := range storages {
			if slices.Contains(s.CleanedStorages, storage) {
				continue
			}
			env, e := p.pbmEnvForStorage(ctx, rc, s, rc.Cluster.Ref().Namespace, storage)
			if e != nil {
				return false, e
			}
			r, e := pbm.DeleteAll(ctx, env)
			if e != nil {
				return false, e
			}
			if !r.Done {
				return false, nil
			}
			s.CleanedStorages = append(s.CleanedStorages, storage)
			return false, nil
		}
		s.CleanupBackups = true
		return false, nil
	}
	retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
	if e != nil {
		return false, e
	}
	for _, m := range ordered(s) {
		if e = verifyMemberServices(ctx, rc, m); e != nil {
			return false, e
		}
		if e = rc.Backend.DeletePublicPorts(ctx, m.Name); e != nil {
			return false, e
		}
		runtime, e := rc.Backend.GetService(ctx, m.Name)
		if e != nil {
			return false, e
		}
		if runtime != nil {
			op, e := rc.Backend.DeleteService(ctx, m.Name)
			if e != nil {
				return false, e
			}
			s.Pending = &pending{Op: op, Action: "cleanup-runtime", Member: m.Name}
			return false, nil
		}
		if m.Volume == "" || m.VolumeOrphaned {
			continue
		}
		v, e := rc.Backend.GetService(ctx, m.Volume)
		if e != nil {
			return false, e
		}
		if opts.KeepVolumes {
			exists := false
			for _, r := range retained {
				exists = exists || r.Member == m.Name
			}
			if !exists && v != nil {
				r := &domain.RetainedVolume{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Hostname: m.Volume, ServiceID: v.ID, Set: m.Set, Role: m.Role, Index: m.Index, RetainedAt: now(rc)}
				if e = rc.Store.CreateRetainedVolume(ctx, r); e != nil {
					return false, e
				}
				return false, nil
			}
		} else if v != nil {
			op, e := rc.Backend.DeleteService(ctx, m.Volume)
			s.Pending = &pending{Op: op, Action: "cleanup-volume", Member: m.Name}
			if e != nil {
				return false, s.failPending(rc, e.Error())
			}
			return false, nil
		}
	}
	if opts.KeepVolumes {
		s.Suspended = true
		rc.Cluster.Status.Phase = domain.PhaseSuspended
		return true, nil
	}
	if !s.CleanupSecrets {
		for _, m := range s.Members {
			if err := deleteMemberSecrets(ctx, rc, m.Name); err != nil {
				return false, err
			}
		}
		keys := []string{"keyfile", "tls/ca.crt", "tls/ca.key", "tls/user.crt", "tls/user.key"}
		keys = append(keys, s.RotationSecrets...)
		for _, u := range users.SystemUserDefs() {
			keys = append(keys, "users/"+u.Name)
		}
		for _, key := range keys {
			if e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); e != nil && !missing(e) {
				return false, e
			}
		}
		for _, r := range retained {
			if e = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), r.Member); e != nil && !missing(e) {
				return false, e
			}
		}
		s.CleanupSecrets = true
	}
	return true, nil
}
func (p *Provider) pbmEnvForStorage(ctx context.Context, rc *engine.Context, s *internalState, namespace, name string) (pbm.Env, error) {
	copyRC := *rc
	cluster := *rc.Cluster
	cluster.Spec.Backup.Storage = name
	copyRC.Cluster = &cluster
	env, e := p.pbmEnvInNamespace(ctx, &copyRC, s, namespace)
	env.RC = rc
	env.Hooks = hooks{p, rc}
	return env, e
}

func deleteMemberSecrets(ctx context.Context, rc *engine.Context, name string) error {
	if err := rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), name); err != nil && !missing(err) {
		return err
	}
	keys := []string{"members/" + name + "/users/backup", "members/" + name + "/users/clusterMonitor"}
	for _, kind := range []string{"members", "agents"} {
		keys = append(keys, "tls/"+kind+"/"+name+".pem", "tls/"+kind+"/"+name+".pending.pem")
	}
	for _, key := range keys {
		if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); err != nil && !missing(err) {
			return err
		}
	}
	return nil
}
