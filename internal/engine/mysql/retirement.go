// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"time"
)

// switchAway chooses an ONLINE secondary covering the writer's observed GTIDs.
// Incomparable or unknown observations never become a zero-lag candidate.
func switchAway(ctx context.Context, rc *engine.Context, s *state, old string) error {
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
	var best gtidSet
	for _, peer := range s.data() {
		if peer.Name == old || peer.Removing || peer.Retirement != "" {
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
		if target == "" || set.contains(best) {
			target, best = status.ServerUUID, set
		}
	}
	if target == "" {
		return engine.ErrUnavailable
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), old)
	if err != nil {
		return err
	}
	return a.Engine(ctx, "mysql", myspec.ActionSwitchover, myspec.SwitchoverRequest{TargetUUID: target}, nil)
}

func survivorsOnline(ctx context.Context, rc *engine.Context, s *state, old string) error {
	writer := false
	for _, m := range s.data() {
		if m.Name == old {
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

func (p *Core) retireMember(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	if busy, err := p.observePending(ctx, rc, s); busy || err != nil {
		return false, err
	}
	if m.Retirement != "credentials" {
		if err := verifyMember(ctx, rc, m); err != nil {
			return false, err
		}
	}
	switch m.Retirement {
	case "handoff":
		if s.Replacement == nil && rc.WaitForWindow().RequeueAfter > 0 {
			return false, nil
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		if s.Topology == "group" && onlinePrimary(st) {
			return false, switchAway(ctx, rc, s, m.Name)
		}
		if err := survivorsOnline(ctx, rc, s, m.Name); err != nil {
			return false, err
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
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		if s.Topology == "group" && onlinePrimary(st) {
			return false, switchAway(ctx, rc, s, m.Name)
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
		runtime, err := engine.ObserveRetiringService(ctx, rc.Backend, m.Name, m.ServiceID, m.Volume, m.VolumeID, false)
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
		m.Retirement = "stop"
	case "stop":
		if m.RetirementOp == nil {
			return false, engine.Invalid("missing retirement stop receipt")
		}
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
		runtime, err := engine.ObserveRetiringService(ctx, rc.Backend, m.Name, m.ServiceID, m.Volume, m.VolumeID, false)
		if err != nil {
			return false, err
		}
		if runtime != nil && runtime.Status != infra.StatusStopped {
			m.Retirement = "runtime-stop"
			return false, nil
		}
		if err = membershipRemoved(ctx, rc, s, m.Name); err != nil {
			return false, err
		}
		m.Retirement = "delete"
	case "delete":
		if err := membershipRemoved(ctx, rc, s, m.Name); err != nil {
			return false, err
		}
		if m.ServiceID != "" {
			runtime, err := engine.ObserveRetiringService(ctx, rc.Backend, m.Name, m.ServiceID, m.Volume, m.VolumeID, false)
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
			volume, err := engine.ObserveRetiringService(ctx, rc.Backend, m.Volume, m.VolumeID, m.Volume, m.VolumeID, false)
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
	if err := survivorsOnline(ctx, rc, s, old); err != nil {
		return err
	}
	for _, m := range s.data() {
		if m.Name == old {
			continue
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil || st.GroupMembers != len(s.data())-1 {
			return engine.ErrUnavailable
		}
	}
	return nil
}
