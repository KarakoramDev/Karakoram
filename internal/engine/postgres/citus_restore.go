// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
)

type citusRestoreCheckpoint struct {
	Seeds []string `json:"seeds"`
	Clone bool     `json:"clone,omitempty"`
}

func (p *Core) configureCitusRestore(ctx context.Context, rc *engine.Context, sp *pgspec.Spec, b *domain.Backup, restore *pgspec.Restore) error {
	if sp.Citus == nil {
		return nil
	}
	for _, g := range b.Postgres.Groups {
		if g.Group == sp.Citus.Group {
			storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
			if err != nil {
				return err
			}
			repo, err := repository(storage, g.Backup.Repository)
			if err != nil {
				return err
			}
			restore.Source = repo
			restore.Set = g.Backup.Label
			restore.TargetName = b.Postgres.RestorePoint
			restore.TargetTime = ""
			return nil
		}
	}
	return engine.Invalid("backup omits replication group %d", sp.Citus.Group)
}

func (p *Core) runCitusRestore(ctx context.Context, rc *engine.Context, op *domain.Operation, cp backupCheckpoint, s *state, r *domain.Restore) (engine.OpResult, error) {
	fail := func(err error) (engine.OpResult, error) { return backupResult(cp, false), err }
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return fail(err)
	}
	if cp.Step == "" && bs.RestoreOperation == op.ID && len(bs.RestoreCheckpoint) > 0 {
		if err = json.Unmarshal(bs.RestoreCheckpoint, &cp); err != nil {
			return fail(err)
		}
	}
	source := sourceForRestore(r, rc.Cluster)
	if cp.Source != nil {
		source = *cp.Source
	}
	b, _, err := p.resolveRestore(ctx, rc, source)
	if err != nil {
		return fail(err)
	}
	switch cp.Step {
	case "", "citus-hold", "citus-stop", "citus-dcs", "citus-wipe", "citus-start-seeds", "citus-primaries", "citus-start-replicas", "citus-replicas":
		if err := p.preflightRestoreSet(ctx, rc, b); err != nil {
			return fail(err)
		}
	}
	if s.CitusOperation != "" && s.CitusOperation != op.ID {
		return fail(engine.ErrConflict)
	}
	if cp.CitusRestore == nil {
		c := &citusRestoreCheckpoint{Clone: r.Name == "initial-restore" && rc.Cluster.Spec.DataSource != nil}
		for _, g := range s.groups() {
			if len(g.data()) == 0 {
				return fail(engine.ErrUnavailable)
			}
			c.Seeds = append(c.Seeds, g.data()[0].Name)
		}
		cp.CitusRestore = c
		cp.Step = "citus-hold"
		cp.Source = &source
		cp.Source.Backup = b.Name
		cp.Generation = max(s.BootstrapGeneration, bs.Generation, s.RepositoryGeneration) + 1
		bs.ManagedRolesHeld = true
		bs.Replicas = false
		bs.RestoreOperation = op.ID
		stamp := now(rc)
		bs.LineageStart = &stamp
		r.StartedAt = &stamp
		r.Phase = domain.PhaseRunning
		s.CitusOperation = op.ID
		if err = save(rc.Cluster, s); err != nil {
			return fail(err)
		}
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return fail(err)
		}
	} else {
		c := cp.CitusRestore
		switch cp.Step {
		case "citus-hold":
			for _, m := range s.Members {
				if m.Role == "proxy" {
					a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
					if err != nil {
						return fail(err)
					}
					err = a.Reload(ctx)
					a.Close()
					if err != nil {
						return fail(err)
					}
				}
			}
			cp.Step = "citus-stop"
			cp.Index = 0
			if c.Clone {
				cp.Step = "citus-primaries"
			}
		case "citus-stop", "citus-wipe":
			if cp.Index < len(s.data()) {
				m := s.data()[cp.Index]
				if err = pgAction(ctx, rc, m.Name, "prepare-restore", pgspec.PrepareRestoreRequest{Receipt: op.ID + "/" + cp.Step, Wipe: cp.Step == "citus-wipe"}, nil); err != nil {
					return fail(err)
				}
				cp.Index++
			} else {
				cp.Index = 0
				if cp.Step == "citus-stop" {
					cp.Step = "citus-dcs"
				} else {
					bs.Restore = cp.Source
					bs.Generation = cp.Generation
					s.RetiredRepositoryGenerations = append(s.RetiredRepositoryGenerations, s.RepositoryGeneration)
					s.RepositoryGeneration = cp.Generation
					bs.Stanza = ""
					if err = save(rc.Cluster, s); err != nil {
						return fail(err)
					}
					cp.Step = "citus-start-seeds"
				}
			}
		case "citus-dcs":
			if cp.Index < len(c.Seeds) {
				if err = pgAction(ctx, rc, c.Seeds[cp.Index], "dcs-reset", struct{}{}, nil); err != nil {
					return fail(err)
				}
				cp.Index++
			} else {
				cp.Step = "citus-wipe"
				cp.Index = 0
			}
		case "citus-start-seeds", "citus-start-replicas":
			names := c.Seeds
			if cp.Step == "citus-start-replicas" {
				names = nil
				for _, m := range s.data() {
					seed := false
					for _, name := range c.Seeds {
						if m.Name == name {
							seed = true
						}
					}
					if !seed {
						names = append(names, m.Name)
					}
				}
			}
			if cp.Index < len(names) {
				a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), names[cp.Index])
				if err != nil {
					return fail(err)
				}
				if err = a.Reload(ctx); err == nil {
					err = a.Process(ctx, agentapi.ProcessAction{Process: "patroni", Action: "start", RequestID: op.ID + "/start/" + names[cp.Index]})
				}
				a.Close()
				if err != nil {
					return fail(err)
				}
				cp.Index++
			} else {
				cp.Index = 0
				if cp.Step == "citus-start-seeds" {
					cp.Step = "citus-primaries"
				} else {
					cp.Step = "citus-replicas"
				}
			}
		case "citus-primaries":
			for _, name := range c.Seeds {
				st, err := live(ctx, rc, name)
				if err != nil || !st.Running || st.Role != "primary" {
					return backupResult(cp, false), err
				}
				if err = pgAction(ctx, rc, name, "rekey", struct{}{}, nil); err != nil {
					return fail(err)
				}
			}
			if c.Clone {
				cp.Step = "citus-replicas"
			} else {
				cp.Step = "citus-start-replicas"
			}
			cp.Index = 0
		case "citus-replicas":
			for _, m := range s.data() {
				ok, err := ready(ctx, rc, m)
				if err != nil || !ok {
					return backupResult(cp, false), err
				}
			}
			cp.Step = "citus-metadata"
		case "citus-metadata":
			if err = p.rewriteCitusRestoreMetadata(ctx, rc, s); err != nil {
				return fail(err)
			}
			// Drop every source bootstrap/credential reference before enabling
			// archiving into the target's fresh, group-local repositories.
			bs.Restore = nil
			bs.CloneComplete = true
			if err = saveBackupState(rc.Cluster, bs); err != nil {
				return fail(err)
			}
			if err = p.reloadBackupMembers(ctx, rc, s); err != nil {
				return fail(err)
			}
			if rc.Cluster.Spec.Backup.Storage != "" {
				if err = p.EnsureStanza(ctx, rc, false); err != nil {
					return fail(err)
				}
			}
			cp.Step = "citus-archive"
			cp.Index = 0
		case "citus-archive":
			if cp.Index < len(s.data()) {
				if err = pgAction(ctx, rc, s.data()[cp.Index].Name, "finish-restore", struct{}{}, nil); err != nil {
					return fail(err)
				}
				cp.Index++
			} else {
				cp.Step = "citus-access"
			}
		case "citus-access":
			if err = p.reconcileRecoveredRoles(ctx, rc); err != nil {
				return fail(err)
			}
			bs.ManagedRolesHeld = false
			bs.Restore = nil
			bs.CloneComplete = true
			s.BootstrapGeneration = cp.Generation
			if err = save(rc.Cluster, s); err != nil {
				return fail(err)
			}
			cp.Step = "citus-release"
		case "citus-release":
			if err = p.reloadBackupMembers(ctx, rc, s); err != nil {
				return fail(err)
			}
			cp.Step = "citus-complete"
		case "citus-complete":
			if bs.ManagedRolesHeld {
				return fail(engine.Invalid("Citus connectivity requires released recovery clients"))
			}
			leaders := map[int]string{}
			for _, group := range s.groups() {
				leader, _, err := group.primary(ctx, rc)
				if err != nil {
					return fail(err)
				}
				leaders[group.id] = leader.Name
			}
			if err = citusConnectivity(ctx, rc, leaders); err != nil {
				return fail(err)
			}
			if rc.Cluster.Spec.Backup.Storage != "" {
				if err = p.EnsureStanza(ctx, rc, false); err != nil {
					return fail(err)
				}
				if err = p.EnqueueFullBackup(ctx, rc, op.ID+"/fresh-base"); err != nil {
					return fail(err)
				}
			}
			stamp := now(rc)
			r.Phase = domain.PhaseSucceeded
			r.CompletedAt = &stamp
			if err = rc.Store.UpdateRestore(ctx, r); err != nil {
				return fail(err)
			}
			s.CitusOperation = ""
			bs.RestoreOperation = ""
			if c.Clone {
				bs.InitialApplied = true
			}
			bs.RestoreCheckpoint = nil
			if err = save(rc.Cluster, s); err != nil {
				return fail(err)
			}
			if err = saveBackupState(rc.Cluster, bs); err != nil {
				return fail(err)
			}
			cp.Step = "done"
			return backupResult(cp, true), nil
		default:
			return fail(engine.Invalid("unknown distributed restore stage %q", cp.Step))
		}
	}
	bs.RestoreCheckpoint, err = json.Marshal(cp)
	if err != nil {
		return fail(err)
	}
	if err = saveBackupState(rc.Cluster, bs); err != nil {
		return fail(err)
	}
	return backupResult(cp, false), nil
}

func (p *Core) rewriteCitusRestoreMetadata(ctx context.Context, rc *engine.Context, s *state) error {
	var req pgspec.CitusRestoreMetadataRequest
	for _, group := range s.groups() {
		leader, _, err := group.primary(ctx, rc)
		if err != nil {
			return err
		}
		for _, m := range group.data() {
			if m.Removing {
				continue
			}
			role := "secondary"
			if m == leader {
				role = "primary"
			}
			req.Nodes = append(req.Nodes, pgspec.CitusNode{GroupID: group.id, Host: m.Name, Port: pgspec.PortPostgres, Role: role})
		}
	}
	// Finish every node-local rewrite before observing any catalog. No remote
	// metadata-sync UDF is safe while recovery client fences are installed.
	for _, node := range req.Nodes {
		if node.Role != "primary" {
			continue
		}
		if err := pgAction(ctx, rc, node.Host, pgspec.ActionCitusRestoreMetadata, req, nil); err != nil {
			return err
		}
	}
	for _, node := range req.Nodes {
		if node.Role != "primary" {
			continue
		}
		var status pgspec.CitusStatusResponse
		if err := pgAction(ctx, rc, node.Host, pgspec.ActionCitusStatus, struct{}{}, &status); err != nil {
			return err
		}
		if err := verifyCitusRestoreMetadata(s, status); err != nil {
			return err
		}
		for _, actual := range status.Nodes {
			matches := false
			for _, target := range req.Nodes {
				if actual.GroupID == target.GroupID && actual.Host == target.Host && actual.Port == target.Port && actual.Role == target.Role {
					matches = true
					break
				}
			}
			if !matches {
				return engine.Invalid("restored Citus catalog does not match target member %s", actual.Host)
			}
		}
	}
	return nil
}

func verifyCitusRestoreMetadata(s *state, status pgspec.CitusStatusResponse) error {
	expected := map[int]bool{}
	for _, g := range s.groups() {
		expected[g.id] = false
	}
	seen := map[string]bool{}
	for _, node := range status.Nodes {
		if !node.Active {
			return engine.Invalid("restored Citus node %d is inactive", node.NodeID)
		}
		m := s.find(node.Host)
		if m == nil || m.Role != "data" || m.Group != node.GroupID || node.Port != 5432 {
			return engine.Invalid("restored pg_dist_node does not match formation: %s", node.Host)
		}
		if seen[node.Host] {
			return engine.Invalid("restored Citus catalog repeats member %s", node.Host)
		}
		seen[node.Host] = true
		switch node.Role {
		case "primary":
			if expected[node.GroupID] {
				return engine.Invalid("restored Citus catalog repeats primary for group %d", node.GroupID)
			}
			expected[node.GroupID] = true
		case "secondary":
		default:
			return engine.Invalid("restored Citus node %s has invalid role %q", node.Host, node.Role)
		}
	}
	for group, found := range expected {
		if !found {
			return fmt.Errorf("restored pg_dist_node omits group %d", group)
		}
	}
	return nil
}
