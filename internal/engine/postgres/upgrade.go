// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"errors"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

// upgradeState journals the maintenance workflow independently of the tracked
// specification update. A control-plane restart resumes the exact offline step.
// No credentials or SQL are stored in this journal.
type upgradeState struct {
	From     string   `json:"from"`
	To       string   `json:"to"`
	Receipt  string   `json:"receipt"`
	Stage    string   `json:"stage"`
	Primary  string   `json:"primary"`
	Locale   string   `json:"locale"`
	Order    []string `json:"order"`
	Index    int      `json:"index"`
	Waiting  bool     `json:"waiting,omitempty"`
	Timeline int      `json:"timeline,omitempty"`
}

func (p *Core) reconcileUpgrade(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Upgrade == nil {
		from := ""
		for _, m := range s.data() {
			if m.Version != "" && major(m.Version) != major(rc.Cluster.Spec.Version) {
				from = m.Version
				break
			}
		}
		if from == "" {
			return false, nil
		}
		if !upgradeAllowed(from, rc.Cluster.Spec.Version) || major(rc.Cluster.Spec.Version) != major(from)+1 {
			return true, engine.Invalid("major upgrade must target the adjacent supported major")
		}
		if !s.Formed || s.Replacement != nil {
			return true, engine.ErrUnavailable
		}
		if err := requireUpgradeBackup(ctx, rc, from); err != nil {
			return true, err
		}
		order, primary, locale, err := upgradeOrder(ctx, rc, s)
		if err != nil {
			return true, err
		}
		backup, err := loadBackupState(rc.Cluster)
		if err != nil {
			return true, err
		}
		// Previous-major backups cannot seed the upgraded replicas. A completed
		// full backup of the new major re-enables pgBackRest replica creation.
		backup.Replicas = false
		if err := saveBackupState(rc.Cluster, backup); err != nil {
			return true, err
		}
		s.Upgrade = &upgradeState{
			From: from, To: rc.Cluster.Spec.Version,
			Receipt: "pg-upgrade-" + s.UID + "-" + rc.Cluster.Spec.Version,
			Stage:   "prepare", Primary: primary, Locale: locale, Order: order,
		}
		return true, nil
	}
	u := s.Upgrade
	if u.To != rc.Cluster.Spec.Version {
		return true, engine.Invalid("cannot change version during a major upgrade")
	}
	request := pgspec.UpgradeRequest{
		Receipt: u.Receipt, OldMajor: major(u.From), NewMajor: major(u.To), Locale: u.Locale,
	}
	switch u.Stage {
	case "prepare", "finalize":
		done, err := p.upgradeDeploy(ctx, rc, s)
		if err != nil || !done {
			return true, err
		}
		if u.Stage == "finalize" {
			s.Upgrade = nil
			return true, nil
		}
		u.Stage = "pause"
	case "pause":
		// The DCS pause survives control-plane restarts. Do not publish any
		// stopped intent until every member has acknowledged election fencing.
		if s.Topology == "ha" {
			if err := pgAction(ctx, rc, u.Primary, "upgrade-pause", struct{}{}, nil); err != nil {
				return true, err
			}
			for _, m := range s.data() {
				st, err := live(ctx, rc, m.Name)
				if err != nil {
					return true, err
				}
				if !st.Running || !st.Paused {
					return true, engine.ErrUnavailable
				}
			}
		}
		// Installation or the pause handshake can change leadership. The
		// writer selected only after that handshake is the authoritative seed.
		_, primary, locale, err := upgradeOrder(ctx, rc, s)
		if err != nil {
			return true, err
		}
		st, err := live(ctx, rc, primary)
		if err != nil {
			return true, err
		}
		if st.Timeline < 1 || (s.Topology == "ha" && !st.Paused) {
			return true, engine.ErrUnavailable
		}
		u.Primary, u.Locale, u.Timeline = primary, locale, st.Timeline
		u.Index, u.Waiting = 0, false
		u.Stage = "stop-replicas"
		s.ConfigVersion++
		for _, m := range s.data() {
			if m.Name != u.Primary {
				m.Offline = true
				m.ConfigVersion = s.ConfigVersion
			}
		}
	case "stop-replicas":
		for _, m := range s.data() {
			if m.Name == u.Primary {
				continue
			}
			stopped, err := awaitUpgradeOffline(ctx, rc, m)
			if err != nil || !stopped {
				return true, err
			}
		}
		// With every other member durably stopped, this must still be the
		// paused writer captured above. A changed writer/timeline fails closed.
		st, err := live(ctx, rc, u.Primary)
		if err != nil {
			return true, err
		}
		if !st.Running || (st.Role != "primary" && st.Role != "standalone") ||
			st.Timeline != u.Timeline || (s.Topology == "ha" && !st.Paused) {
			return true, engine.Invalid("major upgrade final writer changed while fencing; recovery is required")
		}
		m := s.find(u.Primary)
		s.ConfigVersion++
		m.Offline, m.ConfigVersion = true, s.ConfigVersion
		u.Stage = "stop"
	case "stop":
		stopped, err := upgradeAllStopped(ctx, rc, s)
		if err != nil || !stopped {
			return true, err
		}
		u.Stage = "check"
	case "check":
		if stopped, err := upgradeAllStopped(ctx, rc, s); err != nil || !stopped {
			return true, err
		}
		if err := upgradeAction(ctx, rc, u.Primary, "upgrade-check", request); err != nil {
			return true, err
		}
		u.Stage = "link"
	case "link":
		if stopped, err := upgradeAllStopped(ctx, rc, s); err != nil || !stopped {
			return true, err
		}
		if err := upgradeAction(ctx, rc, u.Primary, "upgrade-run", request); err != nil {
			return true, err
		}
		u.Stage = "dcs-reset"
	case "dcs-reset":
		if s.Topology == "ha" {
			a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), u.Primary)
			if err != nil {
				return true, err
			}
			if err := a.Engine(ctx, "postgres", "dcs-reset", struct{}{}, nil); err != nil {
				return true, err
			}
		}
		u.Stage = "primary"
		m := s.find(u.Primary)
		m.Version, m.Offline = u.To, false
		s.ConfigVersion++
		m.ConfigVersion = s.ConfigVersion
	case "primary":
		m := s.find(u.Primary)
		if err := reloadPublished(ctx, rc, s, m); err != nil {
			return true, err
		}
		ok, err := ready(ctx, rc, m)
		if err != nil || !ok {
			return true, err
		}
		m.Stage = "joined"
		u.Stage, u.Index, u.Waiting = "replicas", 0, false
	case "replicas":
		return true, p.rebuildUpgradeReplicas(ctx, rc, s, request)
	case "stanza":
		if err := p.EnsureStanza(ctx, rc, true); err != nil {
			return true, err
		}
		u.Stage = "backup"
	case "backup":
		if err := p.EnqueueFullBackup(ctx, rc, "post-upgrade-"+s.UID+"-"+u.To); err != nil {
			return true, err
		}
		order, _, _, err := upgradeOrder(ctx, rc, s)
		if err != nil {
			return true, err
		}
		u.Stage, u.Order, u.Index, u.Waiting = "finalize", order, 0, false
	default:
		return true, fmt.Errorf("unknown PostgreSQL upgrade checkpoint %q", u.Stage)
	}
	return true, nil
}

func requireUpgradeBackup(ctx context.Context, rc *engine.Context, from string) error {
	if rc.Store == nil || rc.Cluster.Spec.Backup.Storage == "" {
		return engine.Invalid("major upgrade requires a completed backup")
	}
	backups, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return fmt.Errorf("list upgrade backups: %w", err)
	}
	for _, backup := range backups {
		if backup.Phase == domain.PhaseSucceeded && backup.SourceVersion == from && backup.Postgres != nil && backup.Postgres.Label != "" {
			return nil
		}
	}
	return engine.Invalid("major upgrade requires a completed backup of the current PostgreSQL version")
}

func upgradeOrder(ctx context.Context, rc *engine.Context, s *state) ([]string, string, string, error) {
	var order []string
	primary, locale := "", ""
	for _, m := range s.data() {
		st, err := live(ctx, rc, m.Name)
		if err != nil || !st.Running {
			return nil, "", "", engine.ErrUnavailable
		}
		if st.Role == "primary" || st.Role == "standalone" {
			if primary != "" {
				return nil, "", "", engine.Invalid("major upgrade requires exactly one writable primary")
			}
			primary, locale = m.Name, st.Locale
		} else {
			if !st.Streaming {
				return nil, "", "", engine.ErrUnavailable
			}
			order = append(order, m.Name)
		}
	}
	if primary == "" || locale == "" {
		return nil, "", "", engine.ErrUnavailable
	}
	return append(order, primary), primary, locale, nil
}

func (p *Core) upgradeDeploy(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	u := s.Upgrade
	if u.Index >= len(u.Order) {
		return true, nil
	}
	m := s.find(u.Order[u.Index])
	if m == nil {
		return false, engine.ErrNotFound
	}
	if u.Waiting {
		if m.DeployOp != nil {
			status, err := rc.Backend.OpStatus(ctx, *m.DeployOp)
			if status.Status == "DeploymentRetriesExhausted" && err == nil {
				err = &infra.DeploymentExhaustedError{Service: m.Name, LastError: status.Message}
			}
			if errors.Is(err, infra.ErrDeploymentRetriesExhausted) {
				spec, specErr := p.runtimeSpec(rc, m)
				if specErr != nil {
					return false, specErr
				}
				m.DeployOp, u.Waiting = nil, false
				return false, engine.RecordDeploymentExhausted(ctx, rc, m.Name, spec, err)
			}
			if err != nil || (!status.Done && !status.Failed) {
				return false, err
			}
			if status.Failed {
				m.DeployOp = nil
				u.Waiting = false
				return false, fmt.Errorf("upgrade deployment failed for %s", m.Name)
			}
			m.DeployOp = nil
		}
		ok, err := ready(ctx, rc, m)
		if err != nil || !ok {
			return false, err
		}
		m.Stage = "joined"
		u.Index++
		u.Waiting = false
		return false, nil
	}
	st, err := live(ctx, rc, m.Name)
	if err != nil {
		return false, err
	}
	if st.Role == "primary" && s.Topology == "ha" {
		return false, switchAway(ctx, rc, s, m.Name)
	}
	if err := save(rc.Cluster, s); err != nil {
		return false, err
	}
	spec, err := p.runtimeSpec(rc, m)
	if err != nil {
		return false, err
	}
	if engine.DeploymentBlocked(rc, m.Name, spec) {
		return false, nil
	}
	op, err := engine.Deploy(ctx, rc, m.Name, spec)
	if err != nil {
		return false, err
	}
	m.DeployOp, m.Stage = &op, "deploying"
	u.Waiting = true
	return false, nil
}

// awaitUpgradeOffline also stops PostgreSQL left running by paused Patroni.
// The acknowledged desired-process intent, rather than a temporary stop,
// prevents configuration pulls and member restarts from undoing the shutdown.
func awaitUpgradeOffline(ctx context.Context, rc *engine.Context, m *member) (bool, error) {
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return false, err
	}
	status, err := engine.ObserveAgentStatus(ctx, a, "PostgreSQL", m.Name)
	if err != nil {
		return false, err
	}
	if !m.Offline {
		return false, engine.Invalid("major upgrade requires durable stopped intent on every member")
	}
	if status.ConfigVersion < m.ConfigVersion {
		return false, a.Reload(ctx)
	}
	if err := a.Engine(ctx, "postgres", "upgrade-stop", struct{}{}, nil); err != nil {
		return false, err
	}
	st, err := live(ctx, rc, m.Name)
	return !st.Running, err
}

func upgradeAllStopped(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if s.Upgrade == nil || s.Upgrade.Timeline < 1 {
		return false, engine.Invalid("major upgrade has no acknowledged final-writer fence; recovery is required")
	}
	for _, m := range s.data() {
		stopped, err := awaitUpgradeOffline(ctx, rc, m)
		if err != nil || !stopped {
			return false, err
		}
	}
	return true, nil
}

func reloadPublished(ctx context.Context, rc *engine.Context, s *state, m *member) error {
	if err := save(rc.Cluster, s); err != nil {
		return err
	}
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
	if err != nil {
		return err
	}
	return a.Reload(ctx)
}

func upgradeAction(ctx context.Context, rc *engine.Context, member, action string, request pgspec.UpgradeRequest) error {
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), member)
	if err != nil {
		return err
	}
	var response pgspec.UpgradeResponse
	if err := a.Engine(ctx, "postgres", action, request, &response); err != nil {
		return fmt.Errorf("%s on %s: %w", action, member, err)
	}
	if action == "upgrade-check" && !response.Checked || action == "upgrade-run" && !response.Done {
		return engine.ErrUnavailable
	}
	return nil
}
