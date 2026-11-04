// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

type citusBackupCheckpoint struct {
	Groups    []domain.PostgresBackupGroup `json:"groups"`
	Members   []string                     `json:"members"`
	Timelines []int                        `json:"timelines"`
	Index     int                          `json:"index"`
	Point     string                       `json:"point"`
	Abort     string                       `json:"abort,omitempty"`
}

func groupRepoPrefix(storage *domain.BackupStorage, c *domain.Cluster, s *state, group int) string {
	name := "g" + strconv.Itoa(group)
	if incarnation := s.GroupRepositoryIncarnations[group]; incarnation > 0 {
		name += "-i" + strconv.Itoa(incarnation)
	}
	return path.Join(repoPrefix(storage, c, s), name)
}

func (p *Core) runCitusBackup(ctx context.Context, rc *engine.Context, op *domain.Operation, cp backupCheckpoint, s *state, b *domain.Backup) (engine.OpResult, error) {
	fail := func(err error) (engine.OpResult, error) { return backupResult(cp, false), err }
	if s.CitusOperation == "" {
		s.CitusOperation = op.ID
		if err := save(rc.Cluster, s); err != nil {
			return fail(err)
		}
		return backupResult(cp, false), nil
	}
	if s.CitusOperation != op.ID {
		return fail(engine.ErrConflict)
	}
	if cp.Step == "citus-abort" {
		return p.abortCitusBackup(ctx, rc, op, cp, s, b)
	}
	if cp.Citus == nil {
		storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
		if err != nil {
			return fail(err)
		}
		profiles, err := p.selectedProfileRequirements(rc.Cluster.Spec.EngineOptions)
		if err != nil {
			return fail(err)
		}
		c := &citusBackupCheckpoint{Point: "kk_" + secrets.Hash(op.ID)[:32]}
		for _, g := range s.groups() {
			m, st, err := g.primary(ctx, rc)
			if err != nil {
				return fail(err)
			}
			if st.Version == "" {
				return fail(engine.ErrUnavailable)
			}
			c.Members = append(c.Members, m.Name)
			c.Timelines = append(c.Timelines, st.Timeline)
			c.Groups = append(c.Groups, domain.PostgresBackupGroup{Group: g.id, Primary: m.Name, Timeline: st.Timeline, Backup: domain.PostgresBackup{Repository: groupRepoPrefix(storage, rc.Cluster, s, g.id), SystemID: st.SystemID, Version: st.Version, Profiles: profiles}})
			if g.id == 0 {
				b.SourceVersion = st.Version
			}
		}
		cp.Citus = c
		cp.Step = "citus-backups"
		if b.Trigger == "pitr" {
			if !matchingCitusBases(storage, rc.Cluster, s, b.Postgres) {
				return fail(engine.Invalid("PITR point has no matching group bases"))
			}
			c.Groups = append([]domain.PostgresBackupGroup(nil), b.Postgres.Groups...)
			for i := range c.Groups {
				c.Groups[i].LSN = ""
			}
			cp.Step = "citus-point"
		}
		b.Provider = rc.Cluster.Spec.Provider
		b.SourceTopology = rc.Cluster.Spec.Topology
		b.Storage = storage.Name
		b.RecoveryIdentity = "backup-" + secrets.Hash(b.Namespace + "/" + b.Cluster + "/" + b.Name)[:32]
		for _, user := range systemNames {
			pw, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "users/"+user)
			if err != nil {
				return fail(err)
			}
			if err = rc.Secrets.SetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/"+user, pw); err != nil {
				return fail(err)
			}
		}
		if err := p.captureManagedRoles(ctx, rc, b); err != nil {
			return fail(err)
		}
		for _, g := range c.Groups {
			if g.Group == 0 {
				metadata := g.Backup
				metadata.Groups = c.Groups
				b.Postgres = &metadata
			}
		}
		stamp := now(rc)
		b.StartedAt = &stamp
		b.Phase = domain.PhaseRunning
		if err := rc.Store.UpdateBackup(ctx, b); err != nil {
			return fail(err)
		}
		bs, err := loadBackupState(rc.Cluster)
		if err != nil {
			return fail(err)
		}
		if b.Storage != rc.Cluster.Spec.Backup.Storage {
			present := false
			for _, name := range bs.RepositoryStorages {
				if name == b.Storage {
					present = true
				}
			}
			if !present {
				bs.RepositoryStorages = append(bs.RepositoryStorages, b.Storage)
			}
		}
		if err = saveBackupState(rc.Cluster, bs); err != nil {
			return fail(err)
		}
		cp.Step = "citus-publish"
		return backupResult(cp, false), nil
	}
	c := cp.Citus
	switch cp.Step {
	case "citus-publish":
		if err := p.reloadBackupMembers(ctx, rc, s); err != nil {
			return fail(err)
		}
		cp.Step = "citus-backups"
		if b.Trigger == "pitr" {
			cp.Step = "citus-point"
		}
	case "citus-backups":
		if c.Index == len(c.Groups) {
			c.Index = 0
			cp.Step = "citus-point"
			return backupResult(cp, false), nil
		}
		g := &c.Groups[c.Index]
		leader, st, err := s.group(g.Group).primary(ctx, rc)
		if err != nil {
			return fail(err)
		}
		if leader.Name != c.Members[c.Index] || st.Timeline != c.Timelines[c.Index] {
			return fail(engine.Invalid("group primary changed during coordinated backup"))
		}
		storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
		if err != nil {
			return fail(err)
		}
		repo, err := repository(storage, g.Backup.Repository)
		if err != nil {
			return fail(err)
		}
		if err = pgAction(ctx, rc, leader.Name, "stanza-create", pgspec.RepositoryRequest{Repository: &repo}, nil); err != nil {
			return fail(err)
		}
		var response pgspec.BackupResponse
		if err = pgAction(ctx, rc, leader.Name, "backup", pgspec.BackupRequest{Receipt: op.ID + "/g" + strconv.Itoa(g.Group), Type: b.Type, Repository: &repo}, &response); err != nil {
			return fail(err)
		}
		if response.State == "failed" {
			return fail(fmt.Errorf("group %d backup failed: %s", g.Group, response.Message))
		}
		if response.State != "done" {
			return backupResult(cp, false), nil
		}
		var inv pgspec.BackupInfoResponse
		if err = pgAction(ctx, rc, leader.Name, "backup-info", pgspec.RepositoryRequest{Repository: &repo}, &inv); err != nil {
			return fail(err)
		}
		found := false
		for _, item := range inv.Backups {
			if item.Label == response.Label {
				g.Backup = domain.PostgresBackup{Label: item.Label, Repository: g.Backup.Repository, Prior: item.Prior, References: item.References, WALStart: item.WALStart, WALStop: item.WALStop, SystemID: item.SystemID, Version: item.Version, RepoBytes: item.RepoBytes, Profiles: g.Backup.Profiles}
				found = true
			}
		}
		if !found {
			return fail(fmt.Errorf("group %d completed backup missing from inventory", g.Group))
		}
		c.Index++
	case "citus-point":
		leader, _, err := s.group(0).primary(ctx, rc)
		if err != nil {
			return fail(err)
		}
		var point pgspec.CitusRestorePointResponse
		if err = pgAction(ctx, rc, leader.Name, "citus-restore-point", pgspec.CitusRestorePointRequest{Name: c.Point}, &point); err != nil {
			return fail(err)
		}
		// NodeID is not the replication group ID. Resolve the returned host against
		// formation members, including replicas whose address metadata names them.
		for i := range c.Groups {
			g := &c.Groups[i]
			for _, node := range point.Nodes {
				for _, m := range s.group(g.Group).data() {
					if node.Host == m.Name {
						g.LSN = node.LSN
					}
				}
			}
			if g.LSN == "" {
				return fail(engine.Invalid("restore point omits group %d", g.Group))
			}
		}
		cp.Step = "citus-switch"
		c.Index = 0
	case "citus-switch", "citus-archive":
		if c.Index == len(c.Groups) {
			c.Index = 0
			if cp.Step == "citus-switch" {
				cp.Step = "citus-archive"
			} else {
				cp.Step = "citus-record"
			}
			return backupResult(cp, false), nil
		}
		g := &c.Groups[c.Index]
		leader, st, err := s.group(g.Group).primary(ctx, rc)
		if err != nil {
			return fail(err)
		}
		if leader.Name != c.Members[c.Index] || st.Timeline != c.Timelines[c.Index] {
			return fail(engine.Invalid("group primary changed before the coordinated archive barrier"))
		}
		if cp.Step == "citus-switch" {
			if err = pgAction(ctx, rc, leader.Name, "wal-switch", struct{}{}, nil); err != nil {
				return fail(err)
			}
		} else {
			var coverage pgspec.ArchiveCoversResponse
			if err = pgAction(ctx, rc, leader.Name, "archive-covers", pgspec.ArchiveCoversRequest{LSN: g.LSN}, &coverage); err != nil {
				return fail(err)
			}
			if !coverage.Covers {
				return backupResult(cp, false), nil
			}
		}
		c.Index++
	case "citus-record":
		b.Size = 0
		for _, g := range c.Groups {
			b.Size += g.Backup.RepoBytes
			if g.Group == 0 {
				base := g.Backup
				b.Postgres = &base
			}
		}
		if b.Postgres == nil {
			return fail(engine.Invalid("coordinator backup missing"))
		}
		b.Postgres.RestorePoint = c.Point
		b.Postgres.Groups = c.Groups
		stamp := now(rc)
		b.CompletedAt = &stamp
		b.LastWriteTime = &stamp
		b.Phase = domain.PhaseSucceeded
		if err := rc.Store.UpdateBackup(ctx, b); err != nil {
			return fail(err)
		}
		bs, err := loadBackupState(rc.Cluster)
		if err != nil {
			return fail(err)
		}
		if b.Type == "full" {
			bs.Replicas = true
			if err = saveBackupState(rc.Cluster, bs); err != nil {
				return fail(err)
			}
		}
		if err = p.reloadBackupMembers(ctx, rc, s); err != nil {
			return fail(err)
		}
		s.CitusOperation = ""
		if err = save(rc.Cluster, s); err != nil {
			return fail(err)
		}
		cp.Step = "done"
		return backupResult(cp, true), nil
	default:
		return fail(engine.Invalid("unknown coordinated backup stage %q", cp.Step))
	}
	return backupResult(cp, false), nil
}

// Failed coordinated attempts never replay a permanently invalid primary or
// timeline checkpoint. Recovery observes all original receipts before unfreezing.
func handoffCitusBackupRecovery(rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return engine.OpResult{}, err
	}
	if s.Topology != "distributed" || s.CitusOperation != op.ID {
		return engine.OpResult{Done: true}, nil
	}
	var cp backupCheckpoint
	if err = json.Unmarshal(op.Checkpoint, &cp); err != nil {
		return engine.OpResult{}, err
	}
	if cp.Citus == nil {
		cp.Citus = &citusBackupCheckpoint{}
	}
	cp.Citus.Abort = op.Message
	if cp.Citus.Abort == "" {
		cp.Citus.Abort = "coordinated backup aborted before completion"
	}
	cp.Step = "citus-abort"
	result := backupResult(cp, false)
	copy := *op
	copy.Checkpoint = result.Checkpoint
	copy.Step = result.Step
	s.RestoreRecovery = &copy
	if err = save(rc.Cluster, s); err != nil {
		return engine.OpResult{}, err
	}
	return engine.OpResult{Done: true, Checkpoint: copy.Checkpoint, Step: "backup-recovery"}, nil
}

func (p *Core) abortCitusBackup(ctx context.Context, rc *engine.Context, op *domain.Operation, cp backupCheckpoint, s *state, b *domain.Backup) (engine.OpResult, error) {
	fail := func(err error) (engine.OpResult, error) { return backupResult(cp, false), err }
	if b.Trigger != "pitr" && len(cp.Citus.Groups) > 0 {
		storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
		if err != nil {
			return fail(err)
		}
		for i, g := range cp.Citus.Groups {
			repo, err := repository(storage, g.Backup.Repository)
			if err != nil {
				return fail(err)
			}
			var response pgspec.BackupResponse
			// Poll the original member, not its replacement primary. An absent
			// receipt is terminal and must never launch a new native backup.
			if err = pgAction(ctx, rc, cp.Citus.Members[i], "backup", pgspec.BackupRequest{Receipt: op.ID + "/g" + strconv.Itoa(g.Group), Type: b.Type, Repository: &repo, ObserveOnly: true}, &response); err != nil {
				return fail(err)
			}
			switch response.State {
			case "done", "failed", "absent":
			case "running":
				return backupResult(cp, false), nil
			default:
				return fail(engine.Invalid("unknown backup receipt state %q", response.State))
			}
		}
	}
	b.Phase = domain.PhaseFailed
	b.Error = cp.Citus.Abort
	stamp := now(rc)
	b.CompletedAt = &stamp
	if err := rc.Store.UpdateBackup(ctx, b); err != nil {
		return fail(err)
	}
	s.CitusOperation = ""
	if err := save(rc.Cluster, s); err != nil {
		return fail(err)
	}
	cp.Step = "done"
	return backupResult(cp, true), nil
}
