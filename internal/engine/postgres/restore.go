// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func sourceForRestore(r *domain.Restore, target *domain.Cluster) restoreSource {
	ns := r.SourceNamespace
	if ns == "" {
		ns = target.Ref().Namespace
	}
	cluster := r.Source.SourceCluster
	if cluster == "" {
		cluster = target.Name
	}
	source := restoreSource{Namespace: ns, Cluster: cluster, Backup: r.Source.Backup, RestorePoint: r.Source.RestorePoint}
	if r.Source.PITRTime != nil {
		source.Time = r.Source.PITRTime.UTC().Format(time.RFC3339Nano)
	}
	return source
}
func (p *Core) resolveRestore(ctx context.Context, rc *engine.Context, source restoreSource) (*domain.Backup, pgspec.Restore, error) {
	ref := domain.ClusterRef{Namespace: source.Namespace, Name: source.Cluster}
	var b *domain.Backup
	if source.Time != "" && rc.Cluster.Spec.Topology.Type == "distributed" {
		return nil, pgspec.Restore{}, engine.Invalid("distributed PostgreSQL requires a named restore point, not timestamp PITR")
	}
	if source.RestorePoint != "" && source.Backup == "" {
		items, e := rc.Store.ListBackups(ctx, ref)
		if e != nil {
			return nil, pgspec.Restore{}, e
		}
		for _, item := range items {
			if item.Phase == domain.PhaseSucceeded && item.Postgres != nil && item.Postgres.RestorePoint == source.RestorePoint {
				source.Backup = item.Name
				break
			}
		}
		if source.Backup == "" {
			return nil, pgspec.Restore{}, engine.Invalid("unknown coordinated restore point")
		}
	}
	var err error
	if source.Backup != "" {
		b, err = rc.Store.GetBackup(ctx, ref, source.Backup)
	} else {
		var target time.Time
		target, err = time.Parse(time.RFC3339Nano, source.Time)
		if err != nil {
			return nil, pgspec.Restore{}, engine.Invalid("invalid restore time")
		}
		items, e := rc.Store.ListBackups(ctx, ref)
		if e != nil {
			return nil, pgspec.Restore{}, e
		}
		sourceCluster, e := rc.Store.GetCluster(ctx, ref)
		if e != nil {
			return nil, pgspec.Restore{}, e
		}
		sourceContext := *rc
		sourceContext.Cluster = sourceCluster
		if _, e = p.ValidatePITRRestore(ctx, &sourceContext, target); e != nil {
			return nil, pgspec.Restore{}, e
		}
		b, err = p.selectPITRBackup(ctx, rc, sourceCluster, target, items)
		if err != nil {
			return nil, pgspec.Restore{}, err
		}
	}
	if err != nil {
		return nil, pgspec.Restore{}, err
	}
	if !b.ProviderMatches(rc.Cluster.Spec.Provider) {
		return nil, pgspec.Restore{}, engine.Invalid("restore requires exactly matching source and target providers")
	}
	if b.Phase != domain.PhaseSucceeded || b.Postgres == nil || b.Postgres.Label == "" || b.Postgres.Repository == "" {
		return nil, pgspec.Restore{}, engine.Invalid("source backup has no consistent pgBackRest set")
	}
	if major(b.SourceVersion) != major(rc.Cluster.Spec.Version) {
		return nil, pgspec.Restore{}, engine.Invalid("restore requires the source PostgreSQL major version")
	}
	if err := p.validateBackupProfiles(b, rc.Cluster); err != nil {
		return nil, pgspec.Restore{}, err
	}
	if rc.Cluster.Spec.Topology.Type == "distributed" {
		opts, e := ParseOptions(rc.Cluster.Spec.EngineOptions)
		if e != nil {
			return nil, pgspec.Restore{}, e
		}
		if b.SourceTopology.Type != "distributed" || len(b.Postgres.Groups) != 1+opts.citusOptions().WorkerGroups {
			return nil, pgspec.Restore{}, engine.Invalid("distributed restore requires exactly matching source and target group counts")
		}
		if b.Postgres.RestorePoint == "" {
			return nil, pgspec.Restore{}, engine.Invalid("backup has no coordinated restore point")
		}
	} else if len(b.Postgres.Groups) > 0 {
		return nil, pgspec.Restore{}, engine.Invalid("distributed backups require a distributed target")
	}
	storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
	if err != nil {
		return nil, pgspec.Restore{}, err
	}
	repo, err := repository(storage, b.Postgres.Repository)
	return b, pgspec.Restore{Source: repo, Set: b.Postgres.Label, TargetTime: source.Time, TargetName: b.Postgres.RestorePoint}, err
}

// selectPITRBackup shares immutable base-set selection between clone admission
// and execution. Requirements never come from the current source specification.
func (p *Core) selectPITRBackup(ctx context.Context, rc *engine.Context, source *domain.Cluster, target time.Time, items []domain.Backup) (*domain.Backup, error) {
	s, err := load(source)
	if err != nil {
		return nil, err
	}
	bs, err := loadBackupState(source)
	if err != nil {
		return nil, err
	}
	storage, err := p.backupStorage(ctx, rc, source.Spec.Backup.Storage, source.Ref().Namespace)
	if err != nil {
		return nil, err
	}
	repo := repoPrefix(storage, source, s)
	var selected *domain.Backup
	for i := range items {
		b := &items[i]
		if b.Phase != domain.PhaseSucceeded || b.Postgres == nil || b.CompletedAt == nil || b.CompletedAt.After(target) {
			continue
		}
		if b.Storage != storage.Name || b.Postgres.Repository != repo || bs.LineageStart != nil && b.CompletedAt.Before(*bs.LineageStart) {
			continue
		}
		if selected == nil || b.CompletedAt.After(*selected.CompletedAt) {
			selected = b
		}
	}
	if selected == nil {
		return nil, engine.Invalid("no consistent backup precedes the restore target")
	}
	return selected, nil
}
func (p *Core) configureBackup(ctx context.Context, rc *engine.Context, s *state, sp *pgspec.Spec) error {
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	sp.Postgres.RecoveryClientsHeld = bs.ManagedRolesHeld
	for _, storageName := range bs.RepositoryStorages {
		if storageName == rc.Cluster.Spec.Backup.Storage {
			continue
		}
		storage, err := p.backupStorage(ctx, rc, storageName, rc.Cluster.Ref().Namespace)
		if err != nil {
			return err
		}
		prefix := repoPrefix(storage, rc.Cluster, s)
		if s.Topology == "distributed" && sp.Citus != nil {
			prefix = groupRepoPrefix(storage, rc.Cluster, s, sp.Citus.Group)
		}
		repo, err := repository(storage, prefix)
		if err != nil {
			return err
		}
		if sp.Backup == nil {
			timeout := 60
			if rc.Cluster.Spec.Backup.PITR.Enabled && rc.Cluster.Spec.Backup.PITR.OplogSpanMinutes > 0 {
				timeout = rc.Cluster.Spec.Backup.PITR.OplogSpanMinutes * 60
			}
			sp.Backup = &pgspec.Backup{Repository: repo, ArchiveTimeoutSeconds: timeout}
		} else {
			sp.Backup.AdditionalRepositories = append(sp.Backup.AdditionalRepositories, repo)
		}
	}
	sp.Postgres.PgBackRestReplicas = bs.Replicas && sp.Backup != nil
	if sp.Bootstrap != nil && bs.Generation > sp.Bootstrap.Generation {
		sp.Bootstrap.Generation = bs.Generation
	}
	if bs.Restore != nil {
		b, restore, err := p.resolveRestore(ctx, rc, *bs.Restore)
		if err != nil {
			return err
		}
		if err := p.preflightRestoreSet(ctx, rc, b); err != nil {
			return err
		}
		if err = p.configureCitusRestore(ctx, rc, sp, b, &restore); err != nil {
			return err
		}
		recovery, err := recoverySuperuser(ctx, rc, b)
		if err != nil {
			return err
		}
		sp.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &restore, RecoverySuperuser: recovery, Generation: bs.Generation}
	} else if d := rc.Cluster.Spec.DataSource; d != nil && !bs.CloneComplete && !engine.InitialDataSourceUnproven(rc.Cluster) {
		source := restoreSource{Namespace: d.Namespace, Cluster: d.Cluster, Backup: d.Backup}
		if source.Namespace == "" {
			source.Namespace = rc.Cluster.Ref().Namespace
		}
		if d.PITRTime != nil {
			source.Time = d.PITRTime.UTC().Format(time.RFC3339Nano)
		}
		b, restore, err := p.resolveRestore(ctx, rc, source)
		if err != nil {
			return err
		}
		if err := p.preflightRestoreSet(ctx, rc, b); err != nil {
			return err
		}
		if err = p.configureCitusRestore(ctx, rc, sp, b, &restore); err != nil {
			return err
		}
		recovery, err := recoverySuperuser(ctx, rc, b)
		if err != nil {
			return err
		}
		// Bootstrap performs native recovery before the later imperative
		// bookkeeping operation exists. Close amendment admission before any
		// agent can receive this restore configuration.
		if err = rc.Store.ClaimInitialDataSource(store.WithClusterSnapshot(ctx, rc.Cluster), rc.Cluster.Ref()); err != nil {
			return err
		}
		sp.Bootstrap = &pgspec.Bootstrap{Method: "restore", Restore: &restore, RecoverySuperuser: recovery, Generation: s.BootstrapGeneration}
	}
	return nil
}
func recoverySuperuser(ctx context.Context, rc *engine.Context, b *domain.Backup) (*pgspec.Credentials, error) {
	// Patroni's custom-bootstrap lifecycle rekeys its configured target
	// superuser before invoking post_bootstrap; it needs no source password.
	if patroniTopology(rc.Cluster.Spec.Topology.Type) {
		return nil, nil
	}
	if b.RecoveryIdentity == "" {
		return nil, engine.Invalid("source backup has no sealed recovery credentials")
	}
	value, err := rc.Secrets.GetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/postgres")
	if err != nil {
		return nil, fmt.Errorf("read snapshot recovery credential: %w", err)
	}
	return &pgspec.Credentials{Username: "postgres", Password: string(value)}, nil
}

func (p *Core) syncBackups(ctx context.Context, rc *engine.Context, s *state) (bool, error) {
	if rc.Store == nil {
		return false, nil
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return false, err
	}
	if rc.Cluster.Spec.DataSource != nil && bs.InitialQueued && !bs.InitialApplied && !engine.InitialDataSourceUnproven(rc.Cluster) {
		if err = p.MigrateInitialDataSource(ctx, rc); err != nil {
			return false, err
		}
		bs, err = loadBackupState(rc.Cluster)
		if err != nil {
			return false, err
		}
		r, readErr := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), "initial-restore")
		if readErr != nil {
			return false, readErr
		}
		if r.Phase == domain.PhaseFailed || r.Phase == domain.PhaseCancelled {
			return false, engine.Invalid("Initial data source restore operation %s ended %s: %s", r.OperationID, r.Phase, r.Error)
		}
		if r.Phase == domain.PhaseSucceeded && !bs.InitialApplied {
			return false, engine.Invalid("Initial data source restore operation %s has no validated completed recovery checkpoint", r.OperationID)
		}
	}
	if d := rc.Cluster.Spec.DataSource; d != nil && !bs.InitialQueued && !engine.InitialDataSourceUnproven(rc.Cluster) {
		source := restoreSource{Namespace: d.Namespace, Cluster: d.Cluster, Backup: d.Backup}
		if source.Namespace == "" {
			source.Namespace = rc.Cluster.Ref().Namespace
		}
		if d.PITRTime != nil {
			source.Time = d.PITRTime.UTC().Format(time.RFC3339Nano)
		}
		b, _, err := p.resolveRestore(ctx, rc, source)
		if err != nil {
			return false, err
		}
		if err := p.preflightRestoreSet(ctx, rc, b); err != nil {
			return false, err
		}
		if b.RecoveryIdentity == "" {
			return false, engine.Invalid("source backup has no sealed recovery credentials")
		}
		if !bs.CloneCredentials {
			bs.ManagedRolesHeld = true
			if err = saveBackupState(rc.Cluster, bs); err != nil {
				return false, err
			}
			if err = p.restoreManagedRoles(ctx, rc, b); err != nil {
				return false, err
			}
			bs.CloneCredentials = true
			if err = saveBackupState(rc.Cluster, bs); err != nil {
				return false, err
			}
		}
		// Only DATA formation is required for recovery. An unplaceable proxy
		// must not prevent completion and indefinitely fence its own correction.
		for _, member := range s.data() {
			if member.Stage != "joined" {
				return false, nil
			}
		}
		if _, _, err := primary(ctx, rc, s); err != nil {
			return false, nil
		}
		r := &domain.Restore{
			Namespace: rc.Cluster.Ref().Namespace, SourceNamespace: source.Namespace,
			Cluster: rc.Cluster.Name, Name: "initial-restore",
			Source: domain.RestoreSource{
				SourceCluster: source.Cluster, Backup: d.Backup, PITRTime: d.PITRTime,
			},
			Phase: domain.PhasePending,
		}
		payload, _ := json.Marshal(map[string]string{"restore": r.Name})
		op := &domain.Operation{
			ID:        rc.Cluster.Ref().Key() + "-" + s.UID + "-initial-restore",
			Namespace: r.Namespace, Cluster: r.Cluster,
			Kind: domain.OperationRestore, Payload: payload,
		}
		if _, err = rc.Store.GetOperation(ctx, op.ID); missing(err) {
			err = rc.Store.EnqueueRestore(ctx, r, op)
		}
		if err != nil {
			return false, err
		}
		bs.InitialQueued = true
		return false, saveBackupState(rc.Cluster, bs)
	}
	if rc.Cluster.Spec.Backup.Storage == "" || (rc.Cluster.Spec.DataSource != nil && !bs.CloneComplete && !engine.InitialDataSourceUnproven(rc.Cluster)) {
		return false, nil
	}
	storage, err := p.backupStorage(ctx, rc, "", rc.Cluster.Ref().Namespace)
	if err != nil {
		return false, err
	}
	identity := storage.Name + ":" + repoPrefix(storage, rc.Cluster, s) + ":" + strconvMajor(rc.Cluster.Spec.Version)
	if s.Topology == "distributed" {
		// A worker addition or retired-group reincarnation creates a new
		// repository even when storage, cluster lineage and major are unchanged.
		// Sort paths so member allocation order cannot invalidate the checkpoint.
		groups := s.groups()
		paths := make([]string, 0, len(groups))
		for _, g := range groups {
			paths = append(paths, groupRepoPrefix(storage, rc.Cluster, s, g.id))
		}
		slices.Sort(paths)
		identity += ":" + strings.Join(paths, ",")
	}
	if bs.Stanza == identity {
		if s.Topology == "distributed" {
			return p.scheduleCitusPoint(ctx, rc, s)
		}
		return false, nil
	}
	for _, member := range s.data() {
		if member.Stage != "joined" {
			return false, nil
		}
	}
	if _, _, err = primary(ctx, rc, s); err != nil {
		return false, nil
	}
	if err = p.EnsureStanza(ctx, rc, false); err != nil {
		return true, err
	}
	bs.Stanza = identity
	return true, saveBackupState(rc.Cluster, bs)
}
func strconvMajor(version string) string { return fmt.Sprint(major(version)) }
func (p *Core) runRestore(ctx context.Context, rc *engine.Context, op *domain.Operation, cp backupCheckpoint) (engine.OpResult, error) {
	name, err := operationResource(op, "restore")
	if err != nil {
		return backupResult(cp, false), err
	}
	r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return backupResult(cp, false), err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return backupResult(cp, false), err
	}
	if s.Topology == "distributed" {
		return p.runCitusRestore(ctx, rc, op, cp, s, r)
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return backupResult(cp, false), err
	}
	if cp.Step == "" && bs.RestoreOperation == op.ID && len(bs.RestoreCheckpoint) > 0 {
		if err = json.Unmarshal(bs.RestoreCheckpoint, &cp); err != nil {
			return backupResult(cp, false), err
		}
	}
	clone := r.Name == "initial-restore" && rc.Cluster.Spec.DataSource != nil
	source := sourceForRestore(r, rc.Cluster)
	if cp.Source != nil {
		source = *cp.Source
	}
	b, restore, err := p.resolveRestore(ctx, rc, source)
	if err != nil {
		return backupResult(cp, false), err
	}
	// Check while the source set is still needed, including a clone waiting
	// for bootstrap. Later cutover/cleanup must not depend on retained storage.
	switch cp.Step {
	case "", "hold-access", "stop", "credentials", "restore-configure", "dcs-reset", "wipe", "restore-single", "publish-seed", "start-seed", "primary", "clone-primary":
		if err := p.preflightRestoreSet(ctx, rc, b); err != nil {
			return backupResult(cp, false), err
		}
	}
	switch cp.Step {
	case "":
		// Revalidate a queued request before changing lineage state or fencing
		// any member, including requests that name both a set and a PITR time.
		if source.Time != "" {
			target, err := time.Parse(time.RFC3339Nano, source.Time)
			if err != nil {
				return backupResult(cp, false), engine.Invalid("invalid restore time")
			}
			sourceCluster, err := rc.Store.GetCluster(ctx, domain.ClusterRef{Namespace: source.Namespace, Name: source.Cluster})
			if err != nil {
				return backupResult(cp, false), err
			}
			sourceContext := *rc
			sourceContext.Cluster = sourceCluster
			if _, err = p.ValidatePITRRestore(ctx, &sourceContext, target); err != nil {
				return backupResult(cp, false), err
			}
		}
		// Freeze the selected set; newly scheduled backups cannot change a resumed restore.
		source.Backup = b.Name
		cp.Source = &source
		cp.Label = b.Postgres.Label
		cp.Member = s.data()[0].Name
		cp.Generation = max(s.BootstrapGeneration, bs.Generation, s.RepositoryGeneration) + 1
		if !clone && rc.Cluster.Spec.Backup.Storage != "" {
			storage, err := p.backupStorage(ctx, rc, "", rc.Cluster.Ref().Namespace)
			if err != nil {
				return backupResult(cp, false), err
			}
			target, err := repository(storage, repoPrefix(storage, rc.Cluster, s))
			if err != nil {
				return backupResult(cp, false), err
			}
			if restore.Source.Path != target.Path || restore.Source.S3.Endpoint != target.S3.Endpoint || restore.Source.S3.Bucket != target.S3.Bucket {
				cp.RepositoryGeneration = cp.Generation
			}
		}
		cp.Step = "hold-access"
		if clone {
			cp.Step = "clone-primary"
		}
		stamp := now(rc)
		// Replicas must basebackup from the restored primary. A repository's
		// latest old-lineage backup may lie after the requested PITR target.
		bs.Replicas = false
		bs.ManagedRolesHeld = true
		bs.LineageStart = &stamp
		bs.RestoreOperation = op.ID
		bs.RestoreCheckpoint, err = json.Marshal(cp)
		if err != nil {
			return backupResult(cp, false), err
		}
		if err = saveBackupState(rc.Cluster, bs); err != nil {
			return backupResult(cp, false), err
		}
		r.StartedAt = &stamp
		r.Phase = domain.PhaseRunning
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return backupResult(cp, false), err
		}
		if err = backupEvent(ctx, rc, op, "RestoreStarted", "PostgreSQL restore started"); err != nil {
			return backupResult(cp, false), err
		}
	case "hold-access":
		// Revoke cached proxy logins before recovery credentials are delivered
		// and before the restored catalog becomes writable.
		for _, member := range s.Members {
			if member.Role != "proxy" {
				continue
			}
			a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), member.Name)
			if err != nil {
				return backupResult(cp, false), err
			}
			err = a.Reload(ctx)
			a.Close()
			if err != nil {
				return backupResult(cp, false), err
			}
		}
		cp.Step = "stop"
	case "stop":
		if cp.Index < len(s.data()) {
			m := s.data()[cp.Index]
			// Stop holds are durable agent controls; config pulls cannot restart an old lineage.
			err = pgAction(ctx, rc, m.Name, "prepare-restore", pgspec.PrepareRestoreRequest{Receipt: op.ID + "/stop", Wipe: false}, nil)
			if err != nil {
				return backupResult(cp, false), err
			}
			cp.Index++
			return backupResult(cp, false), nil
		}
		cp.Index = 0
		cp.Step = "credentials"
	case "credentials":
		if cp.RepositoryGeneration > 0 && s.RepositoryGeneration != cp.RepositoryGeneration {
			// Recovery can reuse source timeline numbers or introduce another
			// system identifier. Fence first, then isolate all target writes in
			// a fresh repository; previous backup records keep their provenance.
			s.RetiredRepositoryGenerations = append(s.RetiredRepositoryGenerations, s.RepositoryGeneration)
			s.RepositoryGeneration = cp.RepositoryGeneration
			bs.Stanza = ""
			if err = save(rc.Cluster, s); err != nil {
				return backupResult(cp, false), err
			}
			if err = saveBackupState(rc.Cluster, bs); err != nil {
				return backupResult(cp, false), err
			}
		}
		if s.Topology == "single" {
			bs.Restore = &source
			bs.Generation = cp.Generation
			if err = saveBackupState(rc.Cluster, bs); err != nil {
				return backupResult(cp, false), err
			}
		}
		cp.Step = "restore-configure"
	case "restore-configure":
		// The direct restore action must also see the new target repository,
		// especially when the source uses the same path on another S3 endpoint.
		if err = p.reloadBackupMembers(ctx, rc, s); err != nil {
			return backupResult(cp, false), err
		}
		if s.Topology == "ha" {
			cp.Step = "dcs-reset"
		} else {
			cp.Step = "restore-single"
		}
	case "dcs-reset":
		if err = pgAction(ctx, rc, cp.Member, "dcs-reset", struct{}{}, nil); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "wipe"
	case "wipe":
		if cp.Index < len(s.data()) {
			m := s.data()[cp.Index]
			// A fresh custom bootstrap must see an empty directory on the seed too.
			// pgBackRest's delta is retained for the dedicated single-member path.
			if err = pgAction(ctx, rc, m.Name, "prepare-restore", pgspec.PrepareRestoreRequest{Receipt: op.ID + "/wipe", Wipe: true}, nil); err != nil {
				return backupResult(cp, false), err
			}
			cp.Index++
			return backupResult(cp, false), nil
		}
		bs.Restore = &source
		bs.Generation = cp.Generation
		if err = saveBackupState(rc.Cluster, bs); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "publish-seed"
	case "restore-single":
		if err = pgAction(ctx, rc, cp.Member, "restore", pgspec.RestoreRequest{Receipt: op.ID, Restore: restore, Generation: cp.Generation}, nil); err != nil {
			return backupResult(cp, false), err
		}
		bs.Generation = cp.Generation
		if err = saveBackupState(rc.Cluster, bs); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "publish-seed"
	case "publish-seed":
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), cp.Member)
		if err != nil {
			return backupResult(cp, false), err
		}
		if err = a.Reload(ctx); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "start-seed"
	case "start-seed":
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), cp.Member)
		if err != nil {
			return backupResult(cp, false), err
		}
		process := "postgres"
		if s.Topology == "ha" {
			process = "patroni"
		}
		if err = a.Process(ctx, agentapi.ProcessAction{Process: process, Action: "start"}); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "primary"
	case "primary", "clone-primary":
		member, observation, err := primary(ctx, rc, s)
		if err != nil {
			return backupResult(cp, false), nil
		}
		if clone && (s.BootstrapGeneration <= 0 || observation.BootstrapGeneration != s.BootstrapGeneration) {
			// An old/empty running primary is not evidence of restored data.
			// Agents publish this receipt only after native pgBackRest bootstrap.
			return backupResult(cp, false), nil
		}
		if !clone && member != cp.Member {
			return backupResult(cp, false), engine.Invalid("unexpected primary during restore")
		}
		cp.Member = member
		if err = pgAction(ctx, rc, member, "rekey", struct{}{}, nil); err != nil {
			return backupResult(cp, false), err
		}
		cp.Index = 0
		if clone {
			cp.Step = "clone-cutover"
		} else {
			cp.Step = "replicas"
		}
	case "access":
		if err = p.reconcileRecoveredRoles(ctx, rc); err != nil {
			return backupResult(cp, false), err
		}
		bs.ManagedRolesHeld = false
		if err = saveBackupState(rc.Cluster, bs); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "publish-access"
	case "publish-access":
		for _, member := range s.Members {
			if member.Role != "proxy" && member.Role != "data" {
				continue
			}
			// A never-joined proxy has never published target access. It will
			// fetch the durable, recovered credentials on first formation.
			if clone && member.Role == "proxy" && member.Stage != "joined" {
				continue
			}
			a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), member.Name)
			if err != nil {
				return backupResult(cp, false), err
			}
			if err = a.Reload(ctx); err != nil {
				return backupResult(cp, false), err
			}
		}
		cp.Step = "fresh-base"
	case "clone-cutover":
		// Own repo configuration replaces the read-only source bootstrap before WAL
		// archiving is enabled. The source repository is never used by archive-push.
		bs.CloneComplete = true
		if err = saveBackupState(rc.Cluster, bs); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "clone-publish"
	case "clone-publish", "restore-publish":
		if err = p.reloadBackupMembers(ctx, rc, s); err != nil {
			return backupResult(cp, false), err
		}
		if rc.Cluster.Spec.Backup.Storage != "" {
			// A fresh stanza must exist before archive_mode is re-enabled.
			if err = p.EnsureStanza(ctx, rc, false); err != nil {
				return backupResult(cp, false), err
			}
		}
		if cp.Step == "restore-publish" {
			cp.Step = "restore-archive"
		} else {
			cp.Step = "clone-archive"
		}
		cp.Index = 0
	case "clone-archive", "restore-archive":
		if cp.Index < len(s.data()) {
			member := s.data()[cp.Index]
			if err := pgAction(ctx, rc, member.Name, "finish-restore", struct{}{}, nil); err != nil {
				return backupResult(cp, false), err
			}
			cp.Index++
			return backupResult(cp, false), nil
		}
		if cp.Step == "restore-archive" {
			cp.Step = "access"
		} else {
			cp.Step = "ready"
		}
	case "replicas":
		if cp.Index < len(s.data()) {
			m := s.data()[cp.Index]
			cp.Index++
			if m.Name == cp.Member {
				return backupResult(cp, false), nil
			}
			a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if err != nil {
				cp.Index--
				return backupResult(cp, false), err
			}
			if err = a.Reload(ctx); err != nil {
				cp.Index--
				return backupResult(cp, false), err
			}
			if err = a.Process(ctx, agentapi.ProcessAction{Process: "patroni", Action: "start"}); err != nil {
				cp.Index--
				return backupResult(cp, false), err
			}
			return backupResult(cp, false), nil
		}
		cp.Step = "ready"
	case "ready":
		for _, m := range s.data() {
			st, err := live(ctx, rc, m.Name)
			if err != nil || !st.Running || (m.Name != cp.Member && s.Topology == "ha" && !st.Streaming) {
				return backupResult(cp, false), nil
			}
		}
		bs.Restore = nil
		if err = saveBackupState(rc.Cluster, bs); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "restore-cleanup"
		if !clone && rc.Cluster.Spec.Backup.Storage != "" {
			cp.Step = "restore-publish"
		}
	case "restore-cleanup":
		// Refresh cached specs and remove source credential files on every
		// member before recovered proxy logins are reconciled or completion.
		if err = p.reloadBackupMembers(ctx, rc, s); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "stanza"
	case "stanza":
		if rc.Cluster.Spec.Backup.Storage != "" {
			if err = p.EnsureStanza(ctx, rc, false); err != nil {
				return backupResult(cp, false), err
			}
		}
		cp.Step = "access"
	case "fresh-base":
		if (clone || cp.RepositoryGeneration > 0 || rc.Cluster.Spec.Backup.PITR.Enabled) && rc.Cluster.Spec.Backup.Storage != "" {
			if err = p.EnqueueFullBackup(ctx, rc, op.ID+"/fresh-base"); err != nil {
				return backupResult(cp, false), err
			}
		}
		stamp := now(rc)
		r.CompletedAt = &stamp
		r.Phase, r.Error = domain.PhaseSucceeded, ""
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return backupResult(cp, false), err
		}
		if err = backupEvent(ctx, rc, op, "RestoreCompleted", "PostgreSQL restore completed"); err != nil {
			return backupResult(cp, false), err
		}
		bs.RestoreOperation = ""
		if clone {
			bs.InitialApplied = true
		}
		bs.RestoreCheckpoint = nil
		if err = saveBackupState(rc.Cluster, bs); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "done"
		return backupResult(cp, true), nil
	default:
		return backupResult(cp, false), engine.Invalid("unknown restore checkpoint %q", cp.Step)
	}
	return backupResult(cp, false), nil
}

// PITRWindow reports a deliberately conservative observable interval, not WAL
// filename timestamps or a promise that an external actor has not removed WAL.
func (p *Core) PITRWindow(ctx context.Context, rc *engine.Context) ([]domain.TimeWindow, error) {
	if rc.Cluster.Spec.Topology.Type == "distributed" {
		return nil, nil
	}
	if !rc.Cluster.Spec.Backup.PITR.Enabled {
		return nil, nil
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return nil, err
	}
	_, st, err := primary(ctx, rc, s)
	if err != nil {
		return nil, err
	}
	if st.ArchiveFailing || st.RecoverableUntil == "" {
		return nil, nil
	}
	end, err := time.Parse(time.RFC3339Nano, st.RecoverableUntil)
	if err != nil {
		return nil, fmt.Errorf("decode primary recovery watermark: %w", err)
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return nil, err
	}
	storage, err := p.backupStorage(ctx, rc, rc.Cluster.Spec.Backup.Storage, rc.Cluster.Ref().Namespace)
	if err != nil {
		return nil, err
	}
	prefix := repoPrefix(storage, rc.Cluster, s)
	backupState, err := loadBackupState(rc.Cluster)
	if err != nil {
		return nil, err
	}
	active := items[:0]
	for _, backup := range items {
		if backupState.LineageStart != nil && (backup.CompletedAt == nil || backup.CompletedAt.Before(*backupState.LineageStart)) {
			continue
		}
		if backup.Storage == storage.Name && backup.Postgres != nil && backup.Postgres.Repository == prefix {
			active = append(active, backup)
		}
	}
	return consistentWindow(active, rc.Cluster.Spec.Provider, st.SystemID, end), nil
}
func consistentWindow(items []domain.Backup, provider, systemID string, end time.Time) []domain.TimeWindow {
	var start time.Time
	for _, b := range items {
		if b.Phase != domain.PhaseSucceeded || !b.ProviderMatches(provider) || b.Postgres == nil || b.CompletedAt == nil || (systemID != "" && b.Postgres.SystemID != systemID) {
			continue
		}
		if start.IsZero() || b.CompletedAt.Before(start) {
			start = *b.CompletedAt
		}
	}
	if start.IsZero() || !end.After(start) {
		return nil
	}
	return []domain.TimeWindow{{Start: start, End: end}}
}
func (p *Core) ValidatePITRRestore(ctx context.Context, rc *engine.Context, target time.Time) ([]domain.TimeWindow, error) {
	if rc.Cluster.Spec.Topology.Type == "distributed" {
		return nil, engine.InvalidField("source.pitrTime", domain.ReasonUnsupported, "distributed PostgreSQL requires a coordinated named restore point")
	}
	if !rc.Cluster.Spec.Backup.PITR.Enabled {
		return nil, engine.Invalid("PITR is not enabled for the source cluster")
	}
	windows, err := p.PITRWindow(ctx, rc)
	if err != nil {
		return nil, err
	}
	for _, window := range windows {
		if !target.Before(window.Start) && !target.After(window.End) {
			return windows, nil
		}
	}
	return windows, engine.Invalid("restore target is outside the conservative archived WAL window")
}

var _ engine.PITRRestoreValidator = (*Core)(nil)
