// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"fmt"
	"slices"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/backupstorage"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
)

// InitialDataSourceApplied reports the durable initial restore completion receipt.
// Native health and the observed spec generation do not establish data import.
func (*Core) InitialDataSourceApplied(c *domain.Cluster) (bool, error) {
	s, err := loadBackupState(c)
	return s.CloneComplete, err
}

type restoreSource struct {
	Namespace string `json:"namespace"`
	Cluster   string `json:"cluster"`
	Backup    string `json:"backup,omitempty"`
	Time      string `json:"time,omitempty"`
}

func (p *Core) resolveRestore(ctx context.Context, rc *engine.Context, source restoreSource) (*domain.Backup, myspec.Restore, error) {
	ref := domain.ClusterRef{Namespace: source.Namespace, Name: source.Cluster}
	var b *domain.Backup
	var err error
	if source.Backup != "" {
		b, err = rc.Store.GetBackup(ctx, ref, source.Backup)
	} else {
		target, e := time.Parse(time.RFC3339Nano, source.Time)
		if e != nil {
			return nil, myspec.Restore{}, engine.Invalid("invalid restore time")
		}
		sourceCluster, e := rc.Store.GetCluster(ctx, ref)
		if e != nil {
			return nil, myspec.Restore{}, e
		}
		sourceContext := *rc
		sourceContext.Cluster = sourceCluster
		_, byFull, e := p.validatePITRRestore(ctx, &sourceContext, target)
		if e != nil {
			return nil, myspec.Restore{}, e
		}
		sourceState, e := load(sourceCluster)
		if e != nil {
			return nil, myspec.Restore{}, e
		}
		sourceBS, e := loadBackupState(sourceCluster)
		if e != nil {
			return nil, myspec.Restore{}, e
		}
		storage, e := p.backupStorage(ctx, &sourceContext, "", ref.Namespace)
		if e != nil {
			return nil, myspec.Restore{}, e
		}
		prefix := repoPrefix(storage, sourceCluster, sourceState, sourceBS.Generation)
		items, e := rc.Store.ListBackups(ctx, ref)
		if e != nil {
			return nil, myspec.Restore{}, e
		}
		for _, item := range items {
			m := item.MySQL
			if item.Phase == domain.PhaseSucceeded && m != nil && item.Storage == storage.Name && m.Repository == prefix && !m.EndTime.After(target) && backupGTIDContinuitySafe(sourceState, items, item, target) && (b == nil || m.EndTime.After(b.MySQL.EndTime)) {
				if byFull != nil {
					chain, chainErr := restoreChain(items, item)
					if chainErr != nil {
						continue
					}
					covered := false
					for _, root := range items {
						rm := root.MySQL
						if rm == nil || rm.Type != "full" || root.Phase != domain.PhaseSucceeded || root.Provider != item.Provider || root.Storage != item.Storage || rm.Label != m.ChainRootLabel || rm.Repository != m.Repository || rm.ClusterUID != m.ClusterUID || rm.Generation != m.Generation || rm.PhysicalIdentity != m.PhysicalIdentity || len(rm.ObjectPaths) != 1 || rm.ObjectPaths[0] != chain[0] {
							continue
						}
						for _, w := range byFull[root.Name] {
							if !target.Before(w.Start) && !target.After(w.End) {
								covered = true
								break
							}
						}
						if covered {
							break
						}
					}
					if !covered {
						continue
					}
				}
				copy := item
				b = &copy
			}
		}
		if b == nil {
			if byFull != nil {
				return nil, myspec.Restore{}, pitrTargetOutOfRange()
			}
			return nil, myspec.Restore{}, engine.Invalid("no consistent MySQL backup precedes the restore target")
		}
	}
	if err != nil {
		return nil, myspec.Restore{}, err
	}
	if !b.ProviderMatches(p.Name()) || b.Phase != domain.PhaseSucceeded || b.MySQL == nil || major(b.SourceVersion) != major(rc.Cluster.Spec.Version) {
		return nil, myspec.Restore{}, engine.Invalid("restore requires a succeeded MySQL backup of the same server major")
	}
	if b.RecoveryIdentity == "" {
		return nil, myspec.Restore{}, engine.Invalid("MySQL backup has no sealed managed-account snapshot")
	}
	snapshot, err := rc.Secrets.GetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "mysql/access/snapshot")
	if err != nil {
		return nil, myspec.Restore{}, err
	}
	var values map[string][]byte
	if err = json.Unmarshal(snapshot, &values); err != nil {
		return nil, myspec.Restore{}, err
	}
	for _, key := range backupAccessKeys {
		if _, ok := values[key]; !ok {
			return nil, myspec.Restore{}, engine.Invalid("MySQL account snapshot is incomplete")
		}
	}
	items, err := rc.Store.ListBackups(ctx, ref)
	if err != nil {
		return nil, myspec.Restore{}, err
	}
	chain, err := restoreChain(items, *b)
	if err != nil {
		return nil, myspec.Restore{}, err
	}
	storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
	if err != nil {
		return nil, myspec.Restore{}, err
	}
	if err = backupstorage.ValidateMySQLBackup(*storage, *b); err != nil {
		return nil, myspec.Restore{}, err
	}
	repo, err := repository(storage, b.MySQL.Repository)
	if err != nil {
		return nil, myspec.Restore{}, err
	}
	request := myspec.Restore{Source: repo, BackupID: b.Name, Label: b.MySQL.Label, ObjectPaths: chain, TargetTime: source.Time}
	request.ObjectManifests, err = backupManifestRefs(ctx, rc, items, chain)
	if err != nil {
		return nil, myspec.Restore{}, err
	}
	request.RecoveryUsers, err = backupRecoveryUsers(ctx, rc, b)
	if err != nil {
		return nil, myspec.Restore{}, err
	}
	if err = request.Validate(); err != nil {
		return nil, myspec.Restore{}, engine.Invalid("invalid MySQL restore provenance: %v", err)
	}
	return b, request, nil
}
func restoreChain(items []domain.Backup, b domain.Backup) ([]string, error) {
	if b.MySQL == nil {
		return nil, engine.Invalid("backup has no MySQL chain metadata")
	}
	m := b.MySQL
	chain := []string{}
	seen := map[string]bool{}
	for {
		if seen[m.Label] || len(m.ObjectPaths) != 1 || m.ObjectPaths[0] != m.Repository+"/base/"+m.Label {
			return nil, engine.Invalid("invalid MySQL backup chain")
		}
		seen[m.Label] = true
		chain = append(chain, m.ObjectPaths[0])
		if m.Type == "full" {
			if m.ParentLabel != "" || m.Label != b.MySQL.ChainRootLabel {
				return nil, engine.Invalid("MySQL chain root mismatch")
			}
			break
		}
		var parent *domain.MySQLBackup
		for _, item := range items {
			p := item.MySQL
			if p != nil && m.PhysicalIdentity != "" && p.PhysicalIdentity == m.PhysicalIdentity && item.Provider == b.Provider && item.Phase == domain.PhaseSucceeded && item.Storage == b.Storage && p.Repository == m.Repository && p.ClusterUID == m.ClusterUID && p.Generation == m.Generation && p.Label == m.ParentLabel && p.ToLSN == m.FromLSN && major(p.ServerVersion) == major(b.SourceVersion) {
				parent = p
				break
			}
		}
		if parent == nil {
			return nil, engine.Invalid("MySQL incremental parent is missing or inconsistent")
		}
		m = parent
	}
	for i, j := 0, len(chain)-1; i < j; i, j = i+1, j-1 {
		chain[i], chain[j] = chain[j], chain[i]
	}
	return chain, nil
}
func sourceForRestore(r *domain.Restore, c *domain.Cluster) restoreSource {
	source := restoreSource{Namespace: r.SourceNamespace, Cluster: r.Source.SourceCluster, Backup: r.Source.Backup}
	if source.Namespace == "" {
		source.Namespace = c.Ref().Namespace
	}
	if source.Cluster == "" {
		source.Cluster = c.Name
	}
	if r.Source.PITRTime != nil {
		source.Time = r.Source.PITRTime.UTC().Format(time.RFC3339Nano)
	}
	return source
}
func persistRestore(rc *engine.Context, op *domain.Operation, cp backupCheckpoint) error {
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	if bs.RestoreOperation != nil && bs.RestoreOperation.ID != op.ID {
		return engine.ErrConflict
	}
	copy := *op
	copy.Checkpoint = backupResult(cp, false).Checkpoint
	copy.Step = cp.Step
	bs.RestoreOperation = &copy
	return saveBackupState(rc.Cluster, bs)
}
func (p *Core) runRestore(ctx context.Context, rc *engine.Context, op *domain.Operation, cp backupCheckpoint) (engine.OpResult, error) {
	s, err := load(rc.Cluster)
	if err != nil {
		return backupResult(cp, false), err
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return backupResult(cp, false), err
	}
	if bs.RestoreOperation != nil {
		if bs.RestoreOperation.ID != op.ID {
			return backupResult(cp, false), engine.ErrConflict
		}
		if len(bs.RestoreOperation.Checkpoint) > 0 {
			if err = json.Unmarshal(bs.RestoreOperation.Checkpoint, &cp); err != nil {
				return backupResult(cp, false), err
			}
		}
	}
	if cp.Step == "" {
		if len(s.data()) == 0 {
			return backupResult(cp, false), engine.ErrUnavailable
		}
		if op.Kind == "initial-restore" {
			d := normalizedDataSource(rc.Cluster.Spec.DataSource, rc.Cluster.Namespace)
			if d == nil {
				return backupResult(cp, false), engine.Invalid("initial restore has no dataSource")
			}
			cp.Source = &restoreSource{Namespace: d.Namespace, Cluster: d.Cluster, Backup: d.Backup}
			if d.PITRTime != nil {
				cp.Source.Time = d.PITRTime.UTC().Format(time.RFC3339Nano)
			}
		} else {
			name, e := operationResource(op, "restore")
			if e != nil {
				return backupResult(cp, false), e
			}
			r, e := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), name)
			if e != nil {
				return backupResult(cp, false), e
			}
			source := sourceForRestore(r, rc.Cluster)
			cp.Source = &source
		}
		_, request, e := p.resolveRestore(ctx, rc, *cp.Source)
		if e != nil {
			return backupResult(cp, false), e
		}
		if clustered(s.Topology) {
			request.NewGroupName, e = p.newReplicationIdentity()
			if e != nil {
				return backupResult(cp, false), e
			}
		}
		// Checkpoints contain immutable object provenance, never S3 credentials.
		request.Source.S3 = myspec.S3{}
		request.RecoveryUsers = nil
		cp.Request = &request
		cp.Member = s.Formation.Seed
		if cp.Member == "" {
			cp.Member = s.data()[0].Name
		}
		cp.Generation = bs.Generation + 1
		cp.Step = "preflight"
		if err = persistRestore(rc, op, cp); err != nil {
			return backupResult(cp, false), err
		}
		return backupResult(cp, false), nil
	}
	if cp.Request == nil {
		return backupResult(cp, false), engine.Invalid("restore checkpoint lacks frozen source")
	}
	switch cp.Step {
	case "preflight":
		request, e := p.hydrateRestore(ctx, rc, cp)
		if e != nil {
			return backupResult(cp, false), e
		}
		var out myspec.RestoreStatus
		if e = mysqlAction(ctx, rc, cp.Member, myspec.ActionRestorePreflight, myspec.RestoreRequest{Receipt: op.ID, Restore: request, PlanDigest: cp.PlanDigest}, &out); e != nil {
			return backupResult(cp, false), e
		}
		if out.State == "failed" {
			return backupResult(cp, false), engine.Invalid("restore preflight failed at %s: %s; existing data remains untouched", out.Stage, out.Error)
		}
		if out.State == "prepared" && out.Stage == "ready" {
			if !myspec.ValidDigest(out.PlanDigest) || cp.PlanDigest != "" && cp.PlanDigest != out.PlanDigest {
				return backupResult(cp, false), engine.Invalid("restore preflight returned a missing or changed frozen plan digest")
			}
			cp.PlanDigest = out.PlanDigest
			cp.Preflighted, cp.Step = true, "fence"
		} else {
			cp.Step = "preflight-poll"
		}
	case "preflight-poll":
		var out myspec.RestoreStatus
		if e := mysqlAction(ctx, rc, cp.Member, myspec.ActionRestoreStatus, map[string]string{"receipt": op.ID}, &out); e != nil {
			return backupResult(cp, false), e
		}
		if out.State == "failed" {
			return backupResult(cp, false), engine.Invalid("restore preflight failed at %s: %s; existing data remains untouched", out.Stage, out.Error)
		}
		if out.State == "prepared" && out.Stage == "ready" {
			if !myspec.ValidDigest(out.PlanDigest) || cp.PlanDigest != "" && cp.PlanDigest != out.PlanDigest {
				return backupResult(cp, false), engine.Invalid("restore preflight returned a missing or changed frozen plan digest")
			}
			cp.PlanDigest = out.PlanDigest
			cp.Preflighted, cp.Step = true, "fence"
		} else {
			return backupResult(cp, false), nil
		}
	case "fence":
		if !cp.Preflighted || !myspec.ValidDigest(cp.PlanDigest) {
			cp.Step = "preflight"
			if err = persistRestore(rc, op, cp); err != nil {
				return backupResult(cp, false), err
			}
			return backupResult(cp, false), nil
		}
		// Revalidate a frozen PITR source before the first destructive step.
		if cp.Source != nil && cp.Source.Time != "" {
			if cp.Source.Namespace == rc.Cluster.Ref().Namespace && cp.Source.Cluster == rc.Cluster.Name {
				stopped := false
				for _, m := range s.data() {
					stopped = stopped || m.Offline
				}
				if !stopped {
					if e := checkPXCQuorumGTIDs(ctx, rc, s); e != nil && len(s.GTIDIncidents) == 0 {
						return backupResult(cp, false), e
					}
				}
			}
			if _, e := p.hydrateRestore(ctx, rc, cp); e != nil {
				return backupResult(cp, false), e
			}
		}
		// Persist a fresh attempt before requesting proof; an earlier successful
		// verification can never authorize STOP for this fence attempt.
		var token [16]byte
		if _, e := rand.Read(token[:]); e != nil {
			return backupResult(cp, false), e
		}
		cp.RecheckID, cp.Step = fmt.Sprintf("%x", token), "fence-recheck"
	case "fence-recheck":
		if cp.RecheckID == "" {
			cp.Step = "fence"
			break
		}
		request, e := p.hydrateRestore(ctx, rc, cp)
		if e != nil {
			return backupResult(cp, false), e
		}
		var proof myspec.RestoreStatus
		if e = mysqlAction(ctx, rc, cp.Member, myspec.ActionRestorePreflight, myspec.RestoreRequest{Receipt: op.ID, Restore: request, PlanDigest: cp.PlanDigest, RecheckID: cp.RecheckID}, &proof); e != nil {
			return backupResult(cp, false), e
		}
		if proof.State == "failed" {
			return backupResult(cp, false), engine.Invalid("restore preflight no longer verifies: %s; no further members were stopped", proof.Error)
		}
		if proof.State == "prepared" && proof.PlanDigest != cp.PlanDigest {
			return backupResult(cp, false), engine.Invalid("restore preflight plan changed before STOP; existing data remains untouched")
		}
		cp.Step = "fence-recheck-poll"
	case "fence-recheck-poll":
		var proof myspec.RestoreStatus
		if e := mysqlAction(ctx, rc, cp.Member, myspec.ActionRestoreStatus, map[string]string{"receipt": op.ID}, &proof); e != nil {
			return backupResult(cp, false), e
		}
		if proof.State == "failed" {
			return backupResult(cp, false), engine.Invalid("restore preflight no longer verifies: %s; no further members were stopped", proof.Error)
		}
		if proof.State == "prepared" && proof.PlanDigest != cp.PlanDigest {
			return backupResult(cp, false), engine.Invalid("restore preflight plan changed before STOP; existing data remains untouched")
		}
		if cp.RecheckID == "" || proof.RecheckID != cp.RecheckID {
			cp.Step = "fence-recheck"
			break
		}
		if proof.State != "prepared" || proof.Stage != "ready" || proof.VerifiedRecheckID != cp.RecheckID {
			return backupResult(cp, false), nil
		}
		for _, m := range s.data() {
			m.Offline = true
		}
		publishRevision(s)
		if err = save(rc.Cluster, s); err != nil {
			return backupResult(cp, false), err
		}
		for _, m := range s.data() {
			a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if e != nil {
				return backupResult(cp, false), e
			}
			if e = a.Reload(ctx); e != nil {
				return backupResult(cp, false), e
			}
			if e = a.Process(ctx, agentapi.ProcessAction{Process: "mysqld", Action: "stop", RequestID: op.ID + "/stop/" + m.Name}); e != nil {
				return backupResult(cp, false), e
			}
		}
		cp.Step = "stopped"
	case "stopped":
		for _, m := range s.data() {
			a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if e != nil {
				return backupResult(cp, false), e
			}
			applied, e := a.Status(ctx)
			if e != nil {
				return backupResult(cp, false), e
			}
			if applied.ConfigVersion < m.ConfigVersion || applied.Processes["mysqld"].PID != 0 {
				return backupResult(cp, false), nil
			}
			if s.Topology != "cluster" {
				var offline myspec.OfflineGTIDResponse
				if e = a.Engine(ctx, "mysql", myspec.ActionOfflineGTID, struct{}{}, &offline); e != nil {
					return backupResult(cp, false), e
				}
				if !offline.Stopped {
					return backupResult(cp, false), nil
				}
			}
		}
		cp.Step = "dispatch"
	case "dispatch":
		var out myspec.RestoreStatus
		request, e := p.hydrateRestore(ctx, rc, cp)
		if e != nil {
			return backupResult(cp, false), e
		}
		if err = mysqlAction(ctx, rc, cp.Member, myspec.ActionRestore, myspec.RestoreRequest{Receipt: op.ID, Restore: request, PlanDigest: cp.PlanDigest}, &out); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "poll"
	case "poll":
		var out myspec.RestoreStatus
		if err = mysqlAction(ctx, rc, cp.Member, myspec.ActionRestoreStatus, map[string]string{"receipt": op.ID}, &out); err != nil {
			return backupResult(cp, false), err
		}
		if out.State == "failed" {
			return backupResult(cp, false), fmt.Errorf("MySQL restore stopped at %s: %s", out.Stage, out.Error)
		}
		if out.State != "done" {
			return backupResult(cp, false), nil
		}
		if !myspec.ValidUUID(out.ServerUUID) {
			return backupResult(cp, false), engine.Invalid("restored seed has no new server UUID")
		}
		request, e := p.hydrateRestore(ctx, rc, cp)
		if e != nil {
			return backupResult(cp, false), e
		}
		if len(request.RecoveryUsers) > 0 && out.RecoveredUsers == nil {
			return backupResult(cp, false), engine.Invalid("restore agent did not report recovered managed accounts")
		}
		expected := make(map[string]bool, len(request.RecoveryUsers))
		for _, user := range request.RecoveryUsers {
			expected[user.Name] = true
		}
		for _, name := range out.RecoveredUsers {
			if !expected[name] {
				return backupResult(cp, false), engine.Invalid("restore agent reported an unknown recovered account")
			}
		}
		cp.RecoveredUsers = out.RecoveredUsers
		seed := s.find(cp.Member)
		seed.ServerUUID = out.ServerUUID
		seed.Initialized = true
		seed.Stage = "deployed"
		seed.Offline = false
		s.Formation.Seed = cp.Member
		s.Formation.Blocked = ""
		s.Formation.FormationEpoch++
		if clustered(s.Topology) {
			s.Formation.GroupName = cp.Request.NewGroupName
			s.Formation.State = "bootstrapping"
			s.Formation.Reformation = &reformation{State: "bootstrapping"}
			for _, m := range s.data() {
				if m.Name == cp.Member {
					continue
				}
				m.Initialized = false
				m.CloneDone = false
				m.CloneReplacesData = s.Topology == "group"
				m.ServerUUID = ""
				m.Stage = "deployed"
				m.Offline = true
				if s.Topology == "group" {
					m.Clone = &myspec.CloneRequest{Receipt: op.ID + "/clone/" + m.Name, Donor: myspec.Peer{Name: cp.Member, Host: cp.Member, Port: 3306}, Version: rc.Cluster.Spec.Version}
				} else {
					m.Clone = nil
				}
			}
		} else {
			s.Formation.State = "formed"
		}
		publishRevision(s)
		if err = save(rc.Cluster, s); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "reform"
		if err = restoreBackupAccess(ctx, rc, cp); err != nil {
			return backupResult(cp, false), err
		}
	case "reform":
		for _, m := range s.data() {
			a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if e != nil {
				return backupResult(cp, false), e
			}
			if e = a.Reload(ctx); e != nil {
				return backupResult(cp, false), e
			}
		}
		if s.Topology == "cluster" && s.Formation.State == "bootstrapping" {
			for _, m := range s.data() {
				if m.Name == s.Formation.Seed {
					continue
				}
				ok, e := configApplied(ctx, rc, m)
				if e != nil || !ok {
					return backupResult(cp, false), e
				}
				if e = mysqlAction(ctx, rc, m.Name, myspec.ActionGaleraPrepareJoin, myspec.GaleraBootstrapRequest{ClusterName: s.Formation.GroupName, FormationEpoch: s.Formation.FormationEpoch}, nil); e != nil {
					return backupResult(cp, false), e
				}
			}
		}
		// Fencing creates durable process-stop holds. Reloading desired-running
		// configuration cannot release them. Start only members admitted by the
		// restored formation: the seed first, then followers after it forms.
		for _, m := range s.data() {
			if m.Offline {
				continue
			}
			ok, e := configApplied(ctx, rc, m)
			if e != nil || !ok {
				return backupResult(cp, false), e
			}
			a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if e != nil {
				return backupResult(cp, false), e
			}
			status, e := a.Status(ctx)
			if e != nil {
				return backupResult(cp, false), e
			}
			if status.Processes["mysqld"].PID == 0 {
				if e = a.Process(ctx, agentapi.ProcessAction{Process: "mysqld", Action: "start", RequestID: op.ID + "/start/" + m.Name}); e != nil {
					return backupResult(cp, false), e
				}
			}
		}
		if clustered(s.Topology) {
			_, e := p.reconcileFormation(ctx, rc, s)
			if err = save(rc.Cluster, s); err != nil {
				return backupResult(cp, false), err
			}
			if e != nil {
				return backupResult(cp, false), e
			}
		}
		for _, m := range s.data() {
			ok, e := ready(ctx, rc, m)
			if e != nil {
				return backupResult(cp, false), e
			}
			if !ok {
				return backupResult(cp, false), nil
			}
			m.Stage = "joined"
			m.Initialized = true
		}
		if err = save(rc.Cluster, s); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "activate"
	case "activate":
		bs.Generation = cp.Generation
		bs.CloneComplete = true
		// The independent target repository becomes active only after all
		// members have re-formed; publishing it cannot archive into the source.
		if err = saveBackupState(rc.Cluster, bs); err != nil {
			return backupResult(cp, false), err
		}
		publishRevision(s)
		if err = save(rc.Cluster, s); err != nil {
			return backupResult(cp, false), err
		}
		for _, m := range s.data() {
			a, e := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
			if e != nil {
				return backupResult(cp, false), e
			}
			if e = a.Reload(ctx); e != nil {
				return backupResult(cp, false), e
			}
		}
		if rc.Cluster.Spec.Backup.Storage != "" {
			if err = p.EnqueueFullBackup(ctx, rc, op.ID); err != nil {
				return backupResult(cp, false), err
			}
		}
		cp.Step = "done"
	case "done":
		return backupResult(cp, true), nil
	default:
		return backupResult(cp, false), engine.Invalid("unknown restore checkpoint")
	}
	if cp.Step == "done" {
		bs, err = loadBackupState(rc.Cluster)
		if err != nil {
			return backupResult(cp, false), err
		}
		bs.RestoreOperation = nil
		if err = saveBackupState(rc.Cluster, bs); err != nil {
			return backupResult(cp, false), err
		}
		if op.Kind == domain.OperationRestore {
			name, e := operationResource(op, "restore")
			if e != nil {
				return backupResult(cp, false), e
			}
			r, e := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), name)
			if e != nil {
				return backupResult(cp, false), e
			}
			stamp := now(rc)
			r.Phase = domain.PhaseSucceeded
			r.CompletedAt = &stamp
			if e = rc.Store.UpdateRestore(ctx, r); e != nil {
				return backupResult(cp, false), e
			}
		}
		return backupResult(cp, true), nil
	}
	if err = persistRestore(rc, op, cp); err != nil {
		return backupResult(cp, false), err
	}
	return backupResult(cp, false), nil
}
func (p *Core) EnqueueFullBackup(ctx context.Context, rc *engine.Context, key string) error {
	name := "full-" + secretsHash(key)
	if _, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name); err == nil {
		return nil
	} else if !missing(err) {
		return err
	}
	b := &domain.Backup{Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name, Name: name, Provider: "mysql", Storage: rc.Cluster.Spec.Backup.Storage, Type: "full", Trigger: "restore", Phase: domain.PhasePending, CreatedAt: now(rc)}
	payload, _ := json.Marshal(map[string]string{"backup": name})
	return rc.Store.EnqueueBackup(ctx, b, &domain.Operation{Namespace: b.Namespace, Cluster: b.Cluster, Kind: domain.OperationBackup, Payload: payload})
}

func (p *Core) hydrateRestore(ctx context.Context, rc *engine.Context, cp backupCheckpoint) (myspec.Restore, error) {
	if cp.Request == nil || cp.Source == nil {
		return myspec.Restore{}, engine.Invalid("restore has no frozen source")
	}
	b, err := rc.Store.GetBackup(ctx, domain.ClusterRef{Namespace: cp.Source.Namespace, Name: cp.Source.Cluster}, cp.Request.BackupID)
	if err != nil {
		return myspec.Restore{}, err
	}
	if b.MySQL == nil || b.MySQL.Repository != cp.Request.Source.Path {
		return myspec.Restore{}, engine.Invalid("restore repository provenance changed")
	}
	if cp.Source.Time != "" {
		target, e := time.Parse(time.RFC3339Nano, cp.Source.Time)
		if e != nil {
			return myspec.Restore{}, engine.Invalid("invalid frozen PITR target")
		}
		ref := domain.ClusterRef{Namespace: cp.Source.Namespace, Name: cp.Source.Cluster}
		source := rc.Cluster
		if ref != rc.Cluster.Ref() {
			source, e = rc.Store.GetCluster(ctx, ref)
			if e != nil {
				return myspec.Restore{}, e
			}
			sourceContext := *rc
			sourceContext.Cluster = source
			if _, e = p.ValidatePITRRestore(ctx, &sourceContext, target); e != nil {
				return myspec.Restore{}, e
			}
		}
		sourceState, e := load(source)
		if e != nil {
			return myspec.Restore{}, e
		}
		if source.Spec.Provider == "pxc" {
			sourceContext := *rc
			sourceContext.Cluster = source
			if e := loadGTIDJournal(ctx, &sourceContext, sourceState); e != nil {
				return myspec.Restore{}, e
			}
		}
		items, e := rc.Store.ListBackups(ctx, ref)
		if e != nil {
			return myspec.Restore{}, e
		}
		if !backupGTIDContinuitySafe(sourceState, items, *b, target) {
			return myspec.Restore{}, engine.Invalid("frozen PITR chain crosses a PXC GTID discontinuity; choose a verified safe target or a fresh full baseline")
		}
	}
	storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
	if err != nil {
		return myspec.Restore{}, err
	}
	request := *cp.Request
	items, err := rc.Store.ListBackups(ctx, domain.ClusterRef{Namespace: cp.Source.Namespace, Name: cp.Source.Cluster})
	if err != nil {
		return request, err
	}
	refs, err := backupManifestRefs(ctx, rc, items, request.ObjectPaths)
	if err != nil {
		return request, err
	}
	if !slices.Equal(refs, request.ObjectManifests) {
		return request, engine.Invalid("frozen physical backup manifest differs from sealed provenance")
	}
	request.Source, err = repository(storage, cp.Request.Source.Path)
	if err != nil {
		return request, err
	}
	request.RecoveryUsers, err = backupRecoveryUsers(ctx, rc, b)
	return request, err
}
