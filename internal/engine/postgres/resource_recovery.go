// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"reflect"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// reconcileRecoveryResources changes capacity only, never maintenance fences,
// process intent, membership or database configuration. Volume growth does not
// need SQL. A failed runtime cannot serve writes and may accept corrected bounds
// even while a durable maintenance journal owns its database lifecycle.
func (p *Core) reconcileRecoveryResources(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	for _, m := range s.Members {
		if m.Removing || m.RuntimeRecovery.VolumeLost || m.Retirement != "" || m.Stage != "joined" && m.Stage != "deployed" {
			continue
		}
		desired := desiredComponent(rc, m).Resources
		if m.Volume != "" && desired.MinDisk >= m.Disk.MinDisk && desired.MaxDisk >= m.Disk.MaxDisk && (desired.MinDisk != m.Disk.MinDisk || desired.MaxDisk != m.Disk.MaxDisk) {
			if err := engine.RequireDiskGrowth(rc.Backend); err != nil {
				return true, err
			}
			disk := domain.Resources{MinDisk: desired.MinDisk, MaxDisk: desired.MaxDisk}
			op, err := rc.Backend.SetResources(ctx, m.Volume, disk)
			if err != nil {
				return true, err
			}
			p.track(s, m, "resize-volume", op)
			s.Pending.Resources = disk
			return true, nil
		}
		service, err := rc.Backend.GetService(ctx, m.Name)
		if err != nil {
			return true, err
		}
		if service == nil || reflect.DeepEqual(service.Resources, desired) {
			continue
		}
		safe := service.Status == infra.StatusFailed
		if !safe && m.Role == "data" && (s.Rolling != nil || s.Upgrade != nil) {
			if m.Offline {
				// An acknowledged offline config has stopped PostgreSQL before
				// publication, even if etcd keeps the runtime service alive.
				if a, dialErr := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name); dialErr == nil {
					status, statusErr := engine.ObserveAgentStatus(ctx, a, "PostgreSQL", m.Name)
					a.Close()
					safe = statusErr == nil && status.ConfigVersion >= m.ConfigVersion
				}
			} else if status, liveErr := live(ctx, rc, m.Name); liveErr == nil && !status.Running {
				// A failed postmaster does not necessarily fail its agent runtime.
				// A fresh quorum-backed writer elsewhere proves this is not the
				// authoritative primary; keep journal and membership fences intact.
				safe, _ = survivingPrimary(ctx, rc, s, m.Name)
			}
		}
		if !safe {
			continue
		}
		op, err := rc.Backend.SetResources(ctx, m.Name, desired)
		if err != nil {
			return true, err
		}
		p.track(s, m, "resize", op)
		s.Pending.Resources = desired
		return true, nil
	}
	return false, nil
}
