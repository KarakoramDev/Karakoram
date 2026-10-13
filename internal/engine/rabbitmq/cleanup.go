// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package rabbitmq

import (
	"context"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// Cleanup preserves ownership evidence until all platform processes are terminal.
func (p *Provider) Cleanup(ctx context.Context, rc *engine.Context, opts domain.DeleteOptions) (done bool, err error) {
	s, e := loadState(rc.Cluster)
	if e != nil {
		return false, e
	}
	defer func() {
		if e := s.save(rc.Cluster); err == nil {
			err = e
		}
	}()
	s.Phase = "cleanup"
	for _, v := range []*pending{s.Pending, s.Retry} {
		if v == nil {
			continue
		}
		if v.Action == "delete-volume" || v.Action == "delete-runtime" {
			old := platformRetryKey(v)
			if v.Action == "delete-volume" {
				v.Action = "cleanup-volume"
			} else {
				v.Action = "cleanup-runtime"
			}
			if r := s.PlatformRetries[old]; r != nil {
				s.PlatformRetries[platformRetryKey(v)] = r
				delete(s.PlatformRetries, old)
			}
		}
	}
	if s.Pending != nil {
		busy, e := p.observePending(ctx, rc, s)
		if busy || e != nil {
			return false, e
		}
	}
	if s.Retry != nil {
		if s.Retry.Action != "cleanup-runtime" && s.Retry.Action != "cleanup-volume" || opts.KeepVolumes && volumeDelete(s.Retry.Action) {
			s.resetPlatformRetry(s.Retry)
		} else {
			_, e := p.retryPending(ctx, rc, s)
			return false, e
		}
	}
	for _, m := range ordered(s) {
		if e = verifyMemberServices(ctx, rc, m); e != nil {
			return false, e
		}
	}
	if !opts.KeepBackups && !s.CleanupBackups {
		backups, e := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
		if e != nil {
			return false, e
		}
		if len(backups) > 0 {
			b := backups[0]
			if b.Location != "" {
				storage, e := rc.Store.GetBackupStorage(ctx, rc.Cluster.Ref().Namespace, b.Storage)
				if e != nil {
					return false, e
				}
				if e = p.DeleteRetainedBackup(ctx, *storage, b); e != nil {
					return false, e
				}
			}
			return false, rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), b.Name)
		}
		s.CleanupBackups = true
		return false, nil
	}
	retained, e := rc.Store.ListRetainedVolumes(ctx, rc.Cluster.Ref())
	if e != nil {
		return false, e
	}
	for _, m := range ordered(s) {
		runtime, e := rc.Backend.GetService(ctx, m.Name)
		if e != nil {
			return false, e
		}
		if runtime != nil {
			if m.Stage == "deployed" || m.Stage == "ready" || m.Stage == "joined" {
				if m.RemoveStep != "cleanup-stopped" {
					st, e := p.agentStatus(ctx, rc, m.Name)
					if e != nil {
						return false, e
					}
					if running(st) {
						if m.RemoveStep != "cleanup-stop" {
							if e = p.action(ctx, rc, m.Name, "drain", nil); e != nil {
								return false, e
							}
							m.RemoveStep = "cleanup-stop"
							return false, nil
						}
						a, e := p.agent(ctx, rc, m.Name)
						if e != nil {
							return false, e
						}
						e = a.Maintenance(ctx, agentapi.Maintenance{Enabled: true})
						a.Close()
						if e != nil {
							return false, e
						}
						if e = p.action(ctx, rc, m.Name, "stop", nil); e != nil {
							return false, e
						}
					}
					m.RemoveStep = "cleanup-stopped"
					return false, nil
				}
			}
			if e = rc.Backend.DeletePublicPorts(ctx, m.Name); e != nil {
				return false, e
			}
			if sub, ok := rc.Backend.(infra.SubdomainAccess); ok && m.ManagementPublic {
				op, e := sub.SetSubdomainAccess(ctx, m.Name, false)
				if e != nil {
					return false, e
				}
				m.ManagementPublic = false
				s.Pending = &pending{Op: op, Action: "cleanup-management", Member: m.Name}
				return false, nil
			}
			return false, p.submit(ctx, rc, s, &pending{Action: "cleanup-runtime", Member: m.Name})
		}
		if m.VolumeOrphaned {
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
			exists := false
			for _, v := range retained {
				if v.Member == m.Name {
					exists = true
				}
			}
			if !exists {
				return false, rc.Store.CreateRetainedVolume(ctx, &domain.RetainedVolume{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Hostname: m.Volume, ServiceID: volume.ID, Set: "rabbitmq", Role: "rabbitmq", Index: m.Index, RetainedAt: now(rc)})
			}
		} else {
			return false, p.submit(ctx, rc, s, &pending{Action: "cleanup-volume", Member: m.Name})
		}
	}
	if opts.KeepVolumes {
		s.Suspended = true
		rc.Cluster.Status.Phase = domain.PhaseSuspended
		return true, nil
	}
	if !s.CleanupSecrets {
		for _, m := range s.Members {
			if e = deleteMemberSecrets(ctx, rc, m.Name); e != nil {
				return false, e
			}
		}
		for _, key := range []string{"erlang-cookie", "users/karakoram", "users/admin", "users/monitor", "tls/ca.crt", "tls/ca.key", "tls/user.crt", "tls/user.key"} {
			if e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); e != nil && !missing(e) {
				return false, e
			}
		}
		for _, key := range s.RotationSecrets {
			if e = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), key); e != nil && !missing(e) {
				return false, e
			}
		}
		for _, v := range retained {
			if e = rc.Store.DeleteRetainedVolume(ctx, rc.Cluster.Ref(), v.Member); e != nil && !missing(e) {
				return false, e
			}
		}
		s.CleanupSecrets = true
	}
	return true, nil
}
func deleteMemberSecrets(ctx context.Context, rc *engine.Context, name string) error {
	if err := rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), name); err != nil && !missing(err) {
		return err
	}
	for _, kind := range []string{"members", "agents"} {
		for _, suffix := range []string{".pem", ".pending.pem"} {
			if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/"+name+suffix); err != nil && !missing(err) {
				return err
			}
		}
	}
	return nil
}
