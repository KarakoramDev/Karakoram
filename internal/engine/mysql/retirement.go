// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"fmt"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"strings"
	"time"
)

// A non-null full view is the agent's additive capability advertisement.
// Admission observes every existing survivor freshly, before allocating a joiner
// or starting retirement. PXC retains its independent Galera membership proof.
// Confirmed loss alone may retain the released count proof for old survivors:
// their executables cannot safely be rolled until replica recovery completes.
func preflightIdentityBarrier(ctx context.Context, rc *engine.Context, s *state, old string, confirmedLoss bool) (bool, error) {
	if s.Topology != "group" {
		return false, nil
	}
	ctx = freshMySQLReads(ctx)
	invalidateMySQLReadObservation(ctx, rc)
	defer invalidateMySQLReadObservation(ctx, rc)
	var unsupported, unknown []string
	for _, m := range s.data() {
		if m.Name == old || m.Removing || m.Retirement != "" {
			continue
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			unknown = append(unknown, m.Name)
		} else if st.GroupView == nil {
			unsupported = append(unsupported, m.Name)
		}
	}
	if len(unknown) != 0 {
		return false, engine.Invalid("GR structural work refused: full GroupView capability is unknown or unreachable for members %s; restore agent reachability, then run karactl -n %s clusters upgrade-agents %s --wait before retrying", strings.Join(unknown, ", "), rc.Cluster.Ref().Namespace, rc.Cluster.Name)
	}
	if len(unsupported) != 0 {
		if confirmedLoss {
			return false, nil
		}
		return false, engine.Invalid("GR structural work refused: members %s lack full GroupView capability; run karactl -n %s clusters upgrade-agents %s --wait before retrying", strings.Join(unsupported, ", "), rc.Cluster.Ref().Namespace, rc.Cluster.Name)
	}
	return true, nil
}

// switchAway chooses an ONLINE secondary covering the writer's observed GTIDs.
// Incomparable or unknown observations never become a zero-lag candidate.
func switchAway(ctx context.Context, rc *engine.Context, s *state, old string, chosen *string) error {
	if s.Topology == "cluster" {
		return engine.ErrNotSupported
	}
	st, err := live(ctx, rc, old)
	if err != nil {
		return err
	}
	if !onlinePrimary(st) {
		return nil
	}
	writer, err := parseGTIDSet(st.GTIDExecuted)
	if err != nil {
		return err
	}
	target := ""
	targetName := ""
	var best gtidSet
	for _, peer := range s.data() {
		if peer.Name == old || peer.Removing || peer.Retirement != "" || *chosen != "" && peer.Name != *chosen {
			continue
		}
		status, e := live(ctx, rc, peer.Name)
		if e != nil || status.MemberState != "ONLINE" || status.MemberRole != "SECONDARY" || !myspec.ValidUUID(status.ServerUUID) {
			continue
		}
		set, e := parseGTIDSet(status.GTIDExecuted)
		if e != nil || !set.contains(writer) {
			continue
		}
		if err := proxyAdmission(ctx, rc, s, peer.Name, myspec.ProxyReaderGroup); err != nil {
			continue
		}
		if target == "" || set.contains(best) {
			target, targetName, best = status.ServerUUID, peer.Name, set
		}
	}
	if target == "" {
		return engine.ErrUnavailable
	}
	s.ProxyAdmissionWait = ""
	if *chosen == "" {
		*chosen = targetName
		if hasServingProxy(s) {
			// Persist selection before issuing the native action on the next pass.
			return nil
		}
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), old)
	if err != nil {
		return err
	}
	return a.Engine(ctx, "mysql", myspec.ActionSwitchover, myspec.SwitchoverRequest{TargetUUID: target}, nil)
}

// A confirmed-lost predecessor is not a survivor when cleaning up only its
// cancelled replacement allocation. Keep its loss fence and member record:
// subsequent recovery still needs the usual majority and donor admission.
func lostRollbackPredecessor(s *state, target string, m *member) bool {
	cp := s.Replacement
	return s.Topology == "group" && cp != nil && cp.Recovery == "rollback" &&
		cp.New == target && cp.Old == m.Name && m.ConfirmedVolumeLost
}

func survivorsOnline(ctx context.Context, rc *engine.Context, s *state, old string) error {
	writer := false
	for _, m := range s.data() {
		if m.Name == old || lostRollbackPredecessor(s, old, m) || !pxcRetirementSurvivor(ctx, s, old, m.Name) {
			continue
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil || st.MemberState != "ONLINE" {
			return engine.ErrUnavailable
		}
		writer = writer || onlinePrimary(st) || s.Topology == "cluster"
	}
	if !writer {
		return engine.ErrUnavailable
	}
	return nil
}

// Older rollback owners may already have erased their import plan. Preserve the
// released absence policy, but inventory deterministic names before forgetting
// credentials: visible owned reservations must return to exact cleanup.
func reconcileLegacyRollbackReservations(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	cp := s.Replacement
	if cp == nil || cp.Recovery != "rollback" || cp.New != m.Name || cp.IdentityBarrier || m.IdentityBarrier ||
		m.Stage != "planned" || m.ImportBatch != nil || m.ServerUUID != "" || m.Initialized ||
		(m.Retirement != "credentials" && m.Retirement != "delete") {
		return false, nil
	}
	outcome, err := replacementImportOutcome(ctx, rc, m)
	if err != nil || outcome != "" {
		return false, err
	}
	m.ImportAmbiguity = ""
	visible := false
	var incomplete []string
	for _, entry := range [2]struct{ name, id string }{{m.Name, m.ServiceID}, {m.Volume, m.VolumeID}} {
		if entry.name == "" {
			continue
		}
		service, err := engine.ObserveRetiringService(ctx, rc.Backend, entry.name, entry.id, m.Volume, m.VolumeID, m.ConfirmedVolumeLost)
		if err != nil {
			m.ImportAmbiguity = "Waiting for legacy replacement reservation inventory: " + entry.name
			return false, fmt.Errorf("%s: %w", m.ImportAmbiguity, err)
		}
		if service != nil {
			visible = true
			if service.Importing {
				incomplete = append(incomplete, entry.name)
			}
		}
	}
	if visible {
		if err := verifyMember(ctx, rc, m); err != nil {
			return false, err
		}
	}
	if len(incomplete) != 0 {
		m.ImportAmbiguity = "Waiting for incomplete legacy replacement reservations " + strings.Join(incomplete, ", ") + "; allow their platform import to finish or remove these exact owned objects; cleanup retries automatically"
		return false, fmt.Errorf("%s: %w", m.ImportAmbiguity, engine.ErrUnavailable)
	}
	if visible && m.Retirement == "credentials" && (m.ServiceID != "" || m.VolumeID != "") {
		m.Removing, m.Offline, m.Retirement = true, true, "runtime-stop"
		return true, nil
	}
	return false, nil
}

func (p *Core) retireMember(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	ctx = freshMySQLReads(ctx)
	invalidateMySQLReadObservation(ctx, rc)
	defer invalidateMySQLReadObservation(ctx, rc)
	if m.RetirementOp != nil && m.RetirementFenceID == "" {
		m.RetirementFenceID = m.ServiceID
	}
	if busy, err := p.observePending(ctx, rc, s); busy || err != nil {
		return false, err
	}
	if adopted, err := reconcileLegacyRollbackReservations(ctx, rc, s, m); adopted || err != nil {
		return false, err
	}
	if m.Retirement != "credentials" {
		if err := verifyMember(ctx, rc, m); err != nil {
			return false, err
		}
	}
	if s.Topology == "cluster" && m.Removing {
		if err := moveRetiringPXCSeed(ctx, rc, s, m.Name); err != nil {
			return false, err
		}
	}
	switch m.Retirement {
	case "handoff":
		if s.Replacement == nil && rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		if !m.ConfirmedVolumeLost {
			st, err := live(ctx, rc, m.Name)
			if err != nil {
				return false, err
			}
			if s.Topology == "group" && onlinePrimary(st) {
				return false, switchAway(ctx, rc, s, m.Name, &m.HandoffTarget)
			}
		}
		if err := survivorsOnline(ctx, rc, s, m.Name); err != nil {
			return false, err
		}
		if err := proxyHandoffReady(ctx, rc, s, m.Name, &m.HandoffTarget); err != nil {
			return false, err
		}
		if s.Topology == "cluster" {
			if err := moveRetiringPXCSeed(ctx, rc, s, m.Name); err != nil {
				return false, err
			}
		}
		m.Removing = true
		m.Retirement = "drain"
		publishRevision(s)
	case "drain":
		for _, peer := range s.Members {
			if peer.Role == "proxy" {
				ok, err := configApplied(ctx, rc, peer)
				if err != nil || !ok {
					return false, err
				}
			}
		}
		if m.ConfirmedVolumeLost {
			// Never start or contact the old data process. Fence its exact
			// runtime, then require native membership removal before deletion.
			m.Retirement = "runtime-stop"
			return false, nil
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		if s.Topology == "group" && onlinePrimary(st) {
			return false, switchAway(ctx, rc, s, m.Name, &m.HandoffTarget)
		}
		if err := proxyHandoffReady(ctx, rc, s, m.Name, &m.HandoffTarget); err != nil {
			return false, err
		}
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return false, err
		}
		if s.Topology == "cluster" {
			if err = a.Process(ctx, agentapi.ProcessAction{Process: "mysqld", Action: "stop", RequestID: "retire/" + s.UID + "/" + m.Name}); err != nil {
				return false, err
			}
		} else if err = a.Engine(ctx, "mysql", myspec.ActionStopGroup, struct{}{}, nil); err != nil {
			return false, err
		}
		m.Offline = true
		m.Retirement = "leave"
		publishRevision(s)
	case "runtime-stop":
		// A cancelled join may never have a reachable MySQL agent. Its exact
		// runtime stop plus terminal backend evidence fences rescheduling.
		if err := survivorsOnline(ctx, rc, s, m.Name); err != nil {
			return false, err
		}
		for _, peer := range s.Members {
			if peer.Role == "proxy" {
				ok, err := configApplied(ctx, rc, peer)
				if err != nil || !ok {
					return false, err
				}
			}
		}
		runtime, err := engine.ObserveRetiringService(ctx, rc.Backend, m.Name, m.ServiceID, m.Volume, m.VolumeID, m.ConfirmedVolumeLost)
		if err != nil {
			return false, err
		}
		if runtime == nil {
			m.ServiceID = ""
			m.Retirement = "delete"
			return false, nil
		}
		op, err := rc.Backend.Stop(ctx, m.Name)
		if err != nil {
			return false, err
		}
		m.RetirementOp = &op
		m.RetirementFenceID, m.RetirementFenceComplete = runtime.ID, false
		m.Retirement = "stop"
	case "leave":
		ok, err := configApplied(ctx, rc, m)
		if err != nil || !ok {
			return false, err
		}
		if err = membershipRemoved(ctx, rc, s, m.Name); err != nil {
			return false, err
		}
		op, err := rc.Backend.Stop(ctx, m.Name)
		if err != nil {
			return false, err
		}
		m.RetirementOp = &op
		m.RetirementFenceID, m.RetirementFenceComplete = m.ServiceID, false
		m.Retirement = "stop"
	case "stop":
		if m.RetirementOp == nil {
			return false, engine.Invalid("missing retirement stop receipt")
		}
		if !m.RetirementFenceComplete {
			status, err := rc.Backend.OpStatus(ctx, *m.RetirementOp)
			if err != nil {
				return false, err
			}
			if status.Failed {
				m.Retirement = "runtime-stop"
				return false, engine.ErrUnavailable
			}
			if !status.Done {
				return false, nil
			}
		}
		runtime, err := engine.ObserveRetiringService(ctx, rc.Backend, m.Name, m.ServiceID, m.Volume, m.VolumeID, m.ConfirmedVolumeLost)
		if err != nil {
			return false, err
		}
		if runtime != nil && runtime.ID != m.RetirementFenceID {
			return false, fmt.Errorf("retirement runtime identity changed: %w", engine.ErrConflict)
		}
		if runtime != nil && runtime.Status != infra.StatusStopped {
			m.Retirement = "runtime-stop"
			return false, nil
		}
		m.RetirementFenceComplete = true
		if err = membershipRemoved(ctx, rc, s, m.Name); err != nil {
			return false, err
		}
		m.Retirement = "delete"
	case "delete":
		if err := membershipRemoved(ctx, rc, s, m.Name); err != nil {
			return false, err
		}
		if m.ServiceID != "" {
			runtime, err := engine.ObserveRetiringService(ctx, rc.Backend, m.Name, m.ServiceID, m.Volume, m.VolumeID, m.ConfirmedVolumeLost)
			if err != nil {
				return false, err
			}
			if runtime != nil {
				if runtime.Status != infra.StatusStopped {
					m.Retirement = "runtime-stop"
					return false, nil
				}
				if err = rc.Backend.DeletePublicPorts(ctx, m.Name); err != nil {
					return false, err
				}
				op, err := rc.Backend.DeleteServiceByID(ctx, m.Name, m.ServiceID)
				if err != nil {
					return false, err
				}
				p.track(s, m, "delete-runtime", op)
				return false, nil
			}
			m.ServiceID = ""
			return false, nil
		}
		if m.VolumeID != "" {
			volume, err := engine.ObserveRetiringService(ctx, rc.Backend, m.Volume, m.VolumeID, m.Volume, m.VolumeID, m.ConfirmedVolumeLost)
			if err != nil {
				return false, err
			}
			if volume != nil {
				op, err := rc.Backend.DeleteServiceByID(ctx, m.Volume, m.VolumeID)
				if err != nil {
					if !errors.Is(err, infra.ErrRejected) || rc.Store == nil {
						return false, err
					}
					detail := infra.SafeDetail(err)
					if err = rc.Store.PutOrphanedVolume(ctx, &domain.OrphanedVolume{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Member: m.Name, Hostname: m.Volume, ServiceID: m.VolumeID, FirstError: detail, LastError: detail, FirstFailedAt: now(rc), OrphanedAt: now(rc), NextAttemptAt: now(rc).Add(time.Minute)}); err != nil {
						return false, err
					}
					m.VolumeID = ""
					m.Volume = ""
					return false, nil
				}
				p.track(s, m, "delete-volume", op)
				return false, nil
			}
			m.VolumeID = ""
			return false, nil
		}
		m.Retirement = "credentials"
		return false, nil
	case "credentials":
		if s.Topology == "group" {
			if err := membershipRemoved(ctx, rc, s, m.Name); err != nil {
				return false, err
			}
		}
		if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "mysql/imports/"+m.Name); err != nil && !missing(err) {
			return false, err
		}
		if err := rc.Secrets.DeleteAgentToken(ctx, rc.Cluster.Ref(), m.Name); err != nil && !missing(err) {
			return false, err
		}
		for _, kind := range []string{"agents", "servers"} {
			for _, suffix := range []string{".crt", ".key"} {
				if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), "tls/"+kind+"/"+m.Name+suffix); err != nil && !missing(err) {
					return false, err
				}
			}
		}
		if s.Formation.Seed == m.Name {
			for _, peer := range s.data() {
				if peer.Name != m.Name && !peer.Removing && peer.Stage == "joined" {
					s.Formation.Seed = peer.Name
					break
				}
			}
		}
		for i, peer := range s.Members {
			if peer == m {
				s.Members = append(s.Members[:i], s.Members[i+1:]...)
				break
			}
		}
		publishRevision(s)
		return true, nil
	default:
		return false, engine.Invalid("unknown MySQL retirement checkpoint %s", m.Retirement)
	}
	return false, nil
}

func membershipRemoved(ctx context.Context, rc *engine.Context, s *state, old string) error {
	if s.Topology == "group" {
		m := s.find(old)
		if m == nil {
			return engine.ErrUnavailable
		}
		if m.IdentityBarrier || s.Replacement != nil && s.Replacement.IdentityBarrier {
			return groupMembershipRemoved(ctx, rc, s, old)
		}
		if err := countMembershipRemoved(ctx, rc, s, old); err != nil {
			return err
		}
		if !m.LegacyRetirementWarned && rc.Events != nil {
			if err := rc.Events.Record(ctx, domain.Event{Namespace: rc.Cluster.Namespace, Cluster: rc.Cluster.Name, Resource: old, Type: "Warning", Reason: "LegacyGRRetirement", Message: "Completing legacy in-flight GR structural work using the pre-upgrade ONLINE/count proof, not the full UUID identity barrier; upgrade surviving agents before starting new structural work", CreatedAt: now(rc)}); err != nil {
				return err
			}
			m.LegacyRetirementWarned = true
		}
		return nil
	}
	return countMembershipRemoved(ctx, rc, s, old)
}

func countMembershipRemoved(ctx context.Context, rc *engine.Context, s *state, old string) error {
	if err := survivorsOnline(ctx, rc, s, old); err != nil {
		return err
	}
	expected := 0
	for _, m := range s.Members {
		if m.Role == "data" && m.Name != old && !lostRollbackPredecessor(s, old, m) && pxcRetirementSurvivor(ctx, s, old, m.Name) {
			expected++
		}
	}
	for _, m := range s.data() {
		if m.Name == old || lostRollbackPredecessor(s, old, m) || !pxcRetirementSurvivor(ctx, s, old, m.Name) {
			continue
		}
		st, err := live(ctx, rc, m.Name)
		// Old PXC agents do not include wsrep_cluster_size in mysql/status.
		// Retirement is a write-relevant transition, so its fallback is fresh
		// and cannot consume a read-only quorum memo as removal proof.
		if err == nil && rc.Cluster.Spec.Provider == "pxc" && st.GroupMembers == 0 {
			native, observeErr := galeraStatus(freshMySQLReads(ctx), rc, m.Name)
			if observeErr != nil || !native.Eligible() {
				return engine.ErrUnavailable
			}
			st.GroupMembers = native.ClusterSize
		}
		if err != nil || st.GroupMembers != expected {
			return engine.ErrUnavailable
		}
	}
	return nil
}

// Never infer membership from ONLINE counts: an extra UNREACHABLE retiree or
// the wrong ONLINE identities can have exactly the expected count.
func groupMembershipRemoved(ctx context.Context, rc *engine.Context, s *state, old string) error {
	wait := func(reason string) error {
		return fmt.Errorf("waiting for GR retirement identity convergence: %s: %w", reason, engine.ErrUnavailable)
	}
	retiree := s.find(old)
	neverDeployed := retiree != nil && replacementNeverDeployed(s, retiree)
	// An unadmitted rollback allocation can have owned services but no recorded
	// native UUID. It is not "never deployed": require its exact runtime fence
	// and a completed Stop receipt, then the full survivor identity-set proof
	// below must exclude every extra native row, including RECOVERING members.
	fencedUnadmitted := false
	if cp := s.Replacement; retiree != nil && cp != nil && cp.Recovery == "rollback" && cp.New == old &&
		retiree.Stage != "joined" && !retiree.Initialized && !retiree.RuntimeRecovery.Initialized && !retiree.ConfirmedVolumeLost &&
		retiree.ServerUUID == "" && retiree.RetirementOp != nil {
		completed := retiree.RetirementFenceComplete
		if !completed {
			// Older owners may have deleted the exact runtime after successful
			// stop/native proof. Fresh absence is checked below; an expired
			// Stop receipt itself is never converted into success.
			completed = retiree.ServiceID == "" && (retiree.Retirement == "delete" || retiree.Retirement == "credentials")
			if !completed {
				status, err := rc.Backend.OpStatus(ctx, *retiree.RetirementOp)
				if err != nil {
					return err
				}
				completed = status.Done && !status.Failed
			}
		}
		if completed {
			runtime, err := engine.ObserveRetiringService(ctx, rc.Backend, retiree.Name, retiree.ServiceID, retiree.Volume, retiree.VolumeID, retiree.ConfirmedVolumeLost)
			if err != nil {
				return err
			}
			id := retiree.RetirementFenceID
			if id == "" {
				id = retiree.ServiceID
			}
			fencedUnadmitted = runtime == nil || id != "" && runtime.ID == id && runtime.Status == infra.StatusStopped
		}
	}
	if retiree == nil || !neverDeployed && !fencedUnadmitted && !myspec.ValidUUID(retiree.ServerUUID) {
		return wait("retiree UUID is unknown in persisted state")
	}
	expected := make(map[string]string)
	for _, m := range s.data() {
		if m.Name == old || m.Removing || m.Retirement != "" || lostRollbackPredecessor(s, old, m) {
			continue
		}
		id := strings.ToLower(m.ServerUUID)
		if !myspec.ValidUUID(id) || id == strings.ToLower(retiree.ServerUUID) {
			return wait("survivor UUID is unknown or aliases the retiree")
		}
		if _, duplicate := expected[id]; duplicate {
			return wait("duplicate persisted survivor UUID")
		}
		expected[id] = m.Name
	}
	if len(expected) == 0 {
		return wait("no survivors")
	}
	viewID, allViewIDs, viewIDsDiffer := "", true, false
	var reference map[string]myspec.GroupViewMember
	for id, name := range expected {
		st, err := live(ctx, rc, name)
		if err != nil {
			return wait("survivor observation unavailable")
		}
		if st.GroupView == nil {
			return wait("upgrade agents: full GR membership view capability is required")
		}
		if st.MemberState != "ONLINE" || strings.ToLower(st.ServerUUID) != id || st.GroupName != s.Formation.GroupName {
			return wait("survivor is not locally ONLINE with its persisted UUID")
		}
		rows := make(map[string]myspec.GroupViewMember, len(expected))
		primary := false
		for _, row := range st.GroupView.Members {
			rowID := strings.ToLower(row.ID)
			if rowID == strings.ToLower(retiree.ServerUUID) {
				return wait("retiree UUID remains in a survivor view")
			}
			_, duplicate := rows[rowID]
			if _, ok := expected[rowID]; !ok || duplicate || row.State != "ONLINE" {
				return wait("survivor view does not contain exactly the ONLINE survivor UUID set")
			}
			primary = primary || row.Role == "PRIMARY"
			if rowID == id && row.Role != st.MemberRole {
				return wait("local role differs from its membership view")
			}
			row.ID = rowID
			rows[rowID] = row
			if reference != nil && reference[rowID] != row {
				return wait("survivors report inconsistent member identities or roles")
			}
		}
		if len(rows) != len(expected) || !primary {
			return wait("survivor view is incomplete or has no PRIMARY")
		}
		if reference == nil {
			reference = rows
		}
		if st.GroupView.ViewID == "" {
			allViewIDs = false
		} else if viewID == "" {
			viewID = st.GroupView.ViewID
		} else if viewID != st.GroupView.ViewID {
			viewIDsDiffer = true
		}
	}
	if allViewIDs && viewIDsDiffer {
		return wait("survivor view identifiers differ")
	}
	return nil
}

// Move live donor references before excluding the retiree from other members'
// peer lists. An unadmitted joiner is never promoted to authoritative donor.
// The same repair applies when upgrading an already-stopped leave checkpoint.
func moveRetiringPXCSeed(ctx context.Context, rc *engine.Context, s *state, old string) error {
	needed := s.Formation.Seed == old
	for _, m := range s.Members {
		needed = needed || m.GaleraSSTDonor == old
	}
	if !needed {
		return nil
	}
	seed := s.find(s.Formation.Seed)
	if !pxcRetirementSeedOnline(ctx, rc, s, seed, old) {
		seed = nil
		for _, peer := range s.Members {
			if peer.Name == s.Formation.Seed {
				continue
			}
			if pxcRetirementSeedOnline(ctx, rc, s, peer, old) {
				seed = peer
				break
			}
		}
	}
	if seed == nil {
		return engine.ErrUnavailable
	}
	s.Formation.Seed = seed.Name
	for _, m := range s.Members {
		if m.GaleraSSTDonor == old {
			m.GaleraSSTDonor = seed.Name
		}
	}
	publishRevision(s)
	return nil
}

func pxcRetirementSeedOnline(ctx context.Context, rc *engine.Context, s *state, m *member, old string) bool {
	if m == nil || m.Role != "data" || m.Name == old || m.Removing || m.Offline || m.Stage != "joined" || pxcJoinRoutingFenced(s, m) {
		return false
	}
	status, err := live(ctx, rc, m.Name)
	return err == nil && status.MemberState == "ONLINE"
}
