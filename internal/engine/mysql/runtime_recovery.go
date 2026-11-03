// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// Runtime repair never imports storage. Intentional stops remain owned by their
// lifecycle operation; a lost member remains fenced throughout replacement.
func (p *Core) recoverRuntimes(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Suspended || rc.Cluster.Deleting || s.Pending != nil || s.Rolling != nil || s.Formation.State != "formed" || s.Formation.Reformation != nil && s.Formation.Reformation.State != "formed" {
		return false, nil
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return true, err
	}
	if bs.RestoreOperation != nil || s.GaleraTLS != nil || s.TLSRotation != nil {
		return false, nil
	}
	ctx = freshMySQLReads(ctx)
	// Retain the latest successful native cut, including an empty executed set.
	// An unavailable sample never overwrites previously persisted evidence.
	for _, m := range s.data() {
		if m.Offline || m.Removing || m.RuntimeRecovery.VolumeLost {
			continue
		}
		st, err := live(ctx, rc, m.Name)
		if err == nil && st.MemberState == "ONLINE" {
			if _, err := parseGTIDSet(st.GTIDExecuted); err == nil {
				m.LastGTIDExecuted = st.GTIDExecuted
			}
		}
	}
	for _, m := range s.Members {
		if m.Removing || m.Offline || m.Retirement != "" || m.Stage != "joined" && !m.RuntimeRecovery.Initialized || desiredComponent(rc, m).Replicas == 0 {
			continue
		}
		if s.Replacement != nil && s.Replacement.Old == m.Name && m.ConfirmedVolumeLost {
			continue
		}
		m.RuntimeRecovery.Initialized = m.Initialized || m.Stage == "joined"
		tok, err := rc.Secrets.GetAgentToken(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return true, err
		}
		if tok == nil {
			return true, engine.ErrNotFound
		}
		var out engine.RuntimeRecoveryResult
		if !m.ConfirmedVolumeLost {
			out, err = engine.ReconcileRuntime(ctx, rc, engine.RuntimeMember{MemberService: engine.MemberService{Cluster: rc.Cluster.Name, Member: m.Name, Token: tok.Token, Volume: m.Volume, Resources: m.Resources, Containers: max(1, m.Replicas)}, ServiceID: &m.ServiceID, VolumeID: m.VolumeID, Recovery: &m.RuntimeRecovery})
			if err != nil {
				return true, err
			}
		}
		if out.VolumeLost {
			m.ConfirmedVolumeLost = true
		}
		if m.ConfirmedVolumeLost {
			replicated := clustered(s.Topology) && m.Role == "data"
			if replicated {
				if err = p.volumeRecoverySafe(ctx, rc, s, m); err != nil {
					m.RuntimeRecoveryReason = err.Error()
					return true, err
				}
			}
			// Save donor selection and loss evidence before publishing the operation.
			if err = save(rc.Cluster, s); err != nil {
				return true, err
			}
			err = engine.RecoverLostVolume(ctx, rc, m.Name, m.Volume, m.VolumeID, replicated)
			m.RuntimeRecoveryReason = ""
			if err != nil {
				m.RuntimeRecoveryReason = err.Error()
			}
			return true, err
		}
		if out.Imported {
			m.Stage = "imported"
			m.DeployOp = nil
			m.Public = false
		}
		if out.Busy {
			return true, nil
		}
	}
	return false, nil
}

func (p *Core) volumeRecoverySafe(ctx context.Context, rc *engine.Context, s *state, lost *member) error {
	refuse := func(reason string) error {
		return fmt.Errorf("%w: member %s lost volume %s; automatic replacement refused: %s; preserve retained data or restore a backup", engine.ErrConflict, lost.Name, lost.Volume, reason)
	}
	ctx = freshMySQLReads(ctx)
	count, donor := 0, ""
	primary, group := false, ""
	pxc := rc.Cluster.Spec.Provider == "pxc"
	var histories map[string]myspec.GaleraStatusResponse
	seedQualified := false
	if pxc {
		histories = make(map[string]myspec.GaleraStatusResponse)
		if err := loadGTIDJournal(ctx, rc, s); err != nil {
			return refuse("PXC history-continuity journal is unknown")
		}
		if activeGTIDIncident(s) != nil {
			return refuse("PXC history-continuity divergence is latched")
		}
	}
	for _, m := range s.data() {
		if m.Name == lost.Name {
			continue
		}
		if m.Removing || m.Offline || m.RuntimeRecovery.VolumeLost || m.Stage != "joined" || pxcJoinRoutingFenced(s, m) {
			return refuse("survivor " + m.Name + " is not an admitted online data member")
		}
		var gtid string
		if pxc {
			st, err := galeraStatus(ctx, rc, m.Name)
			if err != nil || !st.Eligible() || !myspec.ValidUUID(st.StateUUID) {
				return refuse("survivor " + m.Name + " lacks fresh Primary/Synced/ready/connected PXC evidence")
			}
			if group != "" && group != st.StateUUID {
				return refuse("PXC survivors have different state UUIDs")
			}
			group, gtid = st.StateUUID, st.GTIDExecuted
			histories[m.Name] = st
		} else {
			st, err := live(ctx, rc, m.Name)
			if err != nil || st.MemberState != "ONLINE" || st.GroupName == "" || st.GroupName != s.Formation.GroupName {
				return refuse("survivor " + m.Name + " lacks fresh ONLINE same-group GR evidence (agent upgrade may be required)")
			}
			if group != "" && group != st.GroupName {
				return refuse("GR survivors belong to different groups")
			}
			group, gtid = st.GroupName, st.GTIDExecuted
			primary = primary || onlinePrimary(st)
		}
		count++
		if gtid == "" {
			return refuse("survivor " + m.Name + " has unknown executed GTIDs")
		}
		contains, err := myspec.GTIDContains(gtid, lost.LastGTIDExecuted)
		if err != nil {
			return refuse("survivor " + m.Name + " has invalid executed GTIDs")
		}
		if contains && m.Name == s.Formation.Seed {
			seedQualified = true
		}
		if contains && donor == "" {
			donor = m.Name
		}
	}
	if count <= rc.Cluster.Spec.Components.Engine.Replicas/2 {
		return refuse("surviving ONLINE data members do not form a majority of configured membership")
	}
	if !pxc && !primary {
		return refuse("GR survivors have no writable primary")
	}
	if donor == "" {
		return refuse("no qualified donor contains the lost member's last known GTID set")
	}
	if pxc {
		if err := myspec.CheckGTIDQuorum(histories); err != nil {
			return refuse("PXC survivor executed histories are incompatible or unknown")
		}
		// Existing join certification uses Formation.Seed to pin and fence SST;
		// changing the authoritative donor here does not grant certification.
		changed := !seedQualified
		if changed {
			s.Formation.Seed = donor
		}
		for _, m := range s.Members {
			if m.GaleraSSTDonor == lost.Name {
				m.GaleraSSTDonor = donor
				changed = true
			}
		}
		if changed {
			publishRevision(s)
		}
	}
	return nil
}

func applyMySQLRuntimeRecoveryStatus(status *domain.ClusterStatus, s *state, at time.Time) {
	for _, m := range s.Members {
		engine.ApplyRuntimeRecoveryStatus(status, m.Name, m.Volume, m.RuntimeRecovery, at)
		if m.RuntimeRecovery.VolumeLost && m.RuntimeRecoveryReason != "" {
			status.Message = m.RuntimeRecoveryReason
			status.Conditions = append(status.Conditions, domain.Condition{Type: "VolumeReplacementSafe", Status: "False", Reason: "NativeDataProofMissing", Message: m.RuntimeRecoveryReason, LastTransitionTime: at})
		}
	}
}
