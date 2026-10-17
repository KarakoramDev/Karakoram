// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"errors"
	"fmt"
	"net/url"

	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// fenceRetirement is shared by replacement and scale-in. Each transition lives
// in cluster state; no membership or infrastructure removal may precede stopped.
func fenceRetirement(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	switch m.Retirement {
	case "":
		return publishRetirement(ctx, rc, s, m, "switchover")
	case "switchover", "untag":
		// A primary must remain failover-eligible: Patroni suppresses member
		// replication slots on a nofailover primary. This also recovers an old
		// checkpoint which already tagged a member that later became leader.
		if err := reloadRetirement(ctx, rc, s, m); err != nil {
			return retirementRuntimeFallback(ctx, rc, s, m, err)
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			return retirementRuntimeFallback(ctx, rc, s, m, err)
		}
		if !st.RetirementTagsCleared {
			return false, nil
		}
		if retirementLeader(st, m.Name) {
			return false, switchAway(ctx, rc, s, m.Name)
		}
		if !retirementReplica(st, m.Name) {
			return false, nil
		}
		return publishRetirement(ctx, rc, s, m, "tags")
	case "tags", "stop":
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			return retirementRuntimeFallback(ctx, rc, s, m, err)
		}
		if st.RetirementStopped && !st.Running && !retirementLeader(st, m.Name) {
			m.Retirement = "stopped"
			return false, nil
		}
		// Recheck before publishing/reloading tags and before stopping. If
		// leadership changed, first remove tags durably, then switch away.
		if retirementLeader(st, m.Name) {
			return publishRetirement(ctx, rc, s, m, "untag")
		}
		if !retirementReplica(st, m.Name) {
			return false, nil
		}
		if m.Retirement == "tags" {
			if err := reloadRetirement(ctx, rc, s, m); err != nil {
				return retirementRuntimeFallback(ctx, rc, s, m, err)
			}
			st, err = live(ctx, rc, m.Name)
			if err != nil {
				return retirementRuntimeFallback(ctx, rc, s, m, err)
			}
			if retirementLeader(st, m.Name) {
				return publishRetirement(ctx, rc, s, m, "untag")
			}
			if st.FailoverFenced && retirementReplica(st, m.Name) {
				m.Retirement = "stop"
			}
			return false, nil
		}
		if !st.FailoverFenced {
			return publishRetirement(ctx, rc, s, m, "switchover")
		}
		if err := pgAction(ctx, rc, m.Name, pgspec.ActionRetireStop, struct{}{}, nil); err != nil {
			return false, err
		}
		st, err = live(ctx, rc, m.Name)
		if err != nil {
			return false, err
		}
		if st.RetirementStopped && !st.Running && !retirementLeader(st, m.Name) {
			m.Retirement = "stopped"
		}
		return false, nil
	case "stopped", "removed-etcd":
		if m.RetirementOp != nil {
			return retirementRuntimeStopped(ctx, rc, s, m)
		}
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			return retirementRuntimeFallback(ctx, rc, s, m, err)
		}
		if !st.RetirementStopped || st.Running || retirementLeader(st, m.Name) {
			return publishRetirement(ctx, rc, s, m, "untag")
		}
		return survivingPrimary(ctx, rc, s, m.Name)
	case "runtime-stop":
		if ok, err := survivingPrimary(ctx, rc, s, m.Name); err != nil || !ok {
			return false, err
		}
		// Stopping a runtime can remove its DNS registration. Drain proxies
		// before stopping, not merely before deleting the infrastructure.
		if drained, err := drainProxyBackend(ctx, rc, s, m); err != nil || !drained {
			return false, err
		}
		if err := verifyMember(ctx, rc, m); err != nil {
			return false, err
		}
		runtime, err := rc.Backend.GetService(ctx, m.Name)
		if err != nil {
			return false, err
		}
		if runtime == nil {
			// An already absent backend runtime cannot reschedule. Preserve
			// the infrastructure-fence path for later checkpoint rechecks.
			m.RetirementOp = &infra.Op{}
			m.Retirement = "runtime-stopped"
			return false, nil
		}
		op, err := rc.Backend.Stop(ctx, m.Name)
		if err != nil {
			return false, err
		}
		m.RetirementOp = &op
		m.Retirement = "runtime-wait"
		return false, nil
	case "runtime-wait":
		if m.RetirementOp == nil {
			return false, fmt.Errorf("missing retirement stop receipt")
		}
		st, err := rc.Backend.OpStatus(ctx, *m.RetirementOp)
		if err != nil {
			return false, err
		}
		if st.Failed {
			m.RetirementOp = nil
			m.Retirement = "runtime-stop"
			return false, fmt.Errorf("retirement runtime stop failed")
		}
		if !st.Done {
			return false, nil
		}
		if stopped, err := retirementRuntimeStopped(ctx, rc, s, m); err != nil || !stopped {
			return false, err
		}
		m.Retirement = "runtime-stopped"
		return false, nil
	case "runtime-stopped":
		return retirementRuntimeStopped(ctx, rc, s, m)
	default:
		return false, fmt.Errorf("unknown retirement checkpoint %q", m.Retirement)
	}
}

// Engine action failures and demotion are not proof that the agent is lost.
// Only failure to reach its generic status endpoint selects the backend fence.
// Stop then suppresses rescheduling until the old runtime is terminal.
func retirementRuntimeFallback(ctx context.Context, rc *engine.Context, s *state, m *member, cause error) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	if rc.Agents == nil {
		return false, errors.Join(cause, engine.ErrUnavailable)
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if err == nil {
		_, err = a.Status(ctx)
		if err == nil {
			return false, cause
		}
		var transport *url.Error
		if !errors.As(err, &transport) {
			return false, errors.Join(cause, err)
		}
	}
	if ok, err := survivingPrimary(ctx, rc, s, m.Name); err != nil || !ok {
		return false, err
	}
	m.Retirement = "runtime-stop"
	return false, nil
}

func retirementRuntimeStopped(ctx context.Context, rc *engine.Context, s *state, m *member) (bool, error) {
	runtime, err := rc.Backend.GetService(ctx, m.Name)
	if err != nil {
		return false, err
	}
	if runtime != nil {
		if runtime.ID != m.ServiceID {
			return false, infra.ErrConflict
		}
		if runtime.Status != infra.StatusStopped {
			m.Retirement = "runtime-stop"
			m.RetirementOp = nil
			return false, nil
		}
	}
	return survivingPrimary(ctx, rc, s, m.Name)
}

func survivingPrimary(ctx context.Context, rc *engine.Context, s *state, old string) (bool, error) {
	for _, m := range s.data() {
		if m.Name == old || m.Removing || m.Retirement != "" {
			continue
		}
		st, err := live(ctx, rc, m.Name)
		if err == nil && st.Running && st.Role == "primary" && st.Leader == m.Name && st.EtcdHealthy {
			return true, nil
		}
	}
	return false, engine.ErrUnavailable
}

func retirementLeader(st pgspec.StatusResponse, name string) bool {
	return st.Role == "primary" || st.Leader == name
}

func retirementReplica(st pgspec.StatusResponse, name string) bool {
	return st.Running && st.Role == "replica" && st.Streaming && st.Leader != "" && st.Leader != name
}

func retirementTagsDesired(m *member) bool {
	switch m.Retirement {
	case "tags", "stop":
		return true
	case "stopped", "removed-etcd":
		return m.RetirementOp == nil
	}
	return false
}

func publishRetirement(ctx context.Context, rc *engine.Context, s *state, m *member, stage string) (bool, error) {
	m.Retirement = stage
	s.ConfigVersion++
	m.ConfigVersion = s.ConfigVersion
	return false, save(rc.Cluster, s)
}

func reloadRetirement(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if err := reloadPublished(ctx, rc, s, m); err != nil {
		return err
	}
	return pgAction(ctx, rc, m.Name, pgspec.ActionRetireFence, struct{}{}, nil)
}
