// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/mysql/myspec"
	"github.com/KarakoramDev/Karakoram/internal/s3endpoint"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

type backupState struct {
	Generation       int                 `json:"generation,omitempty"`
	Storages         []string            `json:"storages,omitempty"`
	Repositories     map[string][]string `json:"repositories,omitempty"`
	RestoreOperation *domain.Operation   `json:"restoreOperation,omitempty"`
	CloneComplete    bool                `json:"cloneComplete,omitempty"`
}

func loadBackupState(c *domain.Cluster) (backupState, error) {
	var root map[string]json.RawMessage
	var s backupState
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return s, err
		}
		if len(root["mysqlBackup"]) > 0 {
			return s, json.Unmarshal(root["mysqlBackup"], &s)
		}
	}
	return s, nil
}
func saveBackupState(c *domain.Cluster, s backupState) error {
	root := map[string]json.RawMessage{}
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return err
		}
	}
	b, err := json.Marshal(s)
	if err != nil {
		return err
	}
	root["mysqlBackup"] = b
	c.Internal, err = json.Marshal(root)
	return err
}
func repoPrefix(storage *domain.BackupStorage, c *domain.Cluster, s *state, generation int) string {
	identity := c.Name + "-" + s.UID
	if generation > 0 {
		identity += "-g" + strconv.Itoa(generation)
	}
	return path.Join(strings.Trim(storage.Prefix, "/"), c.Spec.Provider, c.Ref().Namespace, identity)
}
func repository(storage *domain.BackupStorage, prefix string) (myspec.Repository, error) {
	u, err := s3endpoint.Parse(storage.EndpointURL)
	if err != nil {
		return myspec.Repository{}, err
	}
	port := 0
	if u.Port() != "" {
		port, err = strconv.Atoi(u.Port())
		if err != nil {
			return myspec.Repository{}, err
		}
	}
	return myspec.Repository{Path: prefix, S3: myspec.S3{Endpoint: storage.EndpointURL, Port: port, Bucket: storage.Bucket, Region: storage.Region, PathStyle: s3endpoint.PathStyle(u, storage.Bucket, storage.ForcePathStyle), VerifyTLS: storage.VerifyTLS, AccessKey: storage.Credentials.AccessKeyID, SecretKey: storage.Credentials.SecretAccessKey}}, nil
}
func (p *Core) backupStorage(ctx context.Context, rc *engine.Context, name, namespace string) (*domain.BackupStorage, error) {
	store := rc.Store
	if store == nil {
		store = p.options.Store
	}
	if store == nil {
		return nil, engine.ErrUnavailable
	}
	if name == "" {
		name = rc.Cluster.Spec.Backup.Storage
	}
	if name == "" {
		return nil, engine.Invalid("backup storage is required")
	}
	return store.GetBackupStorage(ctx, namespace, name)
}
func mysqlAction(ctx context.Context, rc *engine.Context, member, action string, request, response any) error {
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), member)
	if err != nil {
		return err
	}
	return a.Engine(ctx, "mysql", action, request, response)
}

type backupCheckpoint struct {
	Step           string          `json:"step"`
	Member         string          `json:"member,omitempty"`
	Source         *restoreSource  `json:"source,omitempty"`
	Request        *myspec.Restore `json:"request,omitempty"`
	Generation     int             `json:"generation,omitempty"`
	RecoveredUsers []string        `json:"recoveredUsers,omitempty"`
	Preflighted    bool            `json:"preflighted,omitempty"`
	PlanDigest     string          `json:"planDigest,omitempty"`
	RecheckID      string          `json:"recheckID,omitempty"`
}

func backupResult(cp backupCheckpoint, done bool) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{Done: done, Step: cp.Step, Checkpoint: raw, RequeueAfter: 2 * time.Second}
}
func operationResource(op *domain.Operation, key string) (string, error) {
	var p map[string]string
	if err := json.Unmarshal(op.Payload, &p); err != nil {
		return "", err
	}
	if p[key] == "" {
		return "", engine.Invalid("operation has no %s", key)
	}
	return p[key], nil
}
func (p *Core) runBackupOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	var cp backupCheckpoint
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return engine.OpResult{}, err
		}
	}
	if cp.Step == "done" {
		return backupResult(cp, true), nil
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return backupResult(cp, false), err
	}
	if s.GaleraTLS != nil {
		return backupResult(cp, false), engine.ErrUnavailable
	}
	switch op.Kind {
	case domain.OperationBackup:
		return p.runBackup(ctx, rc, op, cp)
	case domain.OperationDeleteBackup:
		return p.runDeleteBackup(ctx, rc, op, cp)
	case domain.OperationRestore, "initial-restore":
		return p.runRestore(ctx, rc, op, cp)
	}
	return engine.OpResult{}, engine.ErrNotSupported
}
func backupMember(ctx context.Context, rc *engine.Context, s *state) (string, error) {
	return backupMemberIdentity(ctx, rc, s, "")
}

func backupMemberIdentity(ctx context.Context, rc *engine.Context, s *state, identity string) (string, error) {
	// A PITR baseline must anchor the stream that is actually archived.
	// Numeric GTID equality cannot authorize replay from another member.
	if rc.Cluster.Spec.Backup.PITR.Enabled {
		writer, err := archiveWriter(ctx, rc, s)
		if err != nil {
			return "", err
		}
		status, err := live(ctx, rc, writer)
		if err != nil || identity != "" && status.PhysicalIdentity != identity {
			return "", engine.Invalid("PITR backup source changed; take a fresh full from the current archive writer")
		}
		return writer, nil
	}
	primary := ""
	for _, m := range s.data() {
		st, err := live(ctx, rc, m.Name)
		if err != nil || !st.Online(false) || identity != "" && st.PhysicalIdentity != identity {
			continue
		}
		if st.MemberRole == "SECONDARY" {
			return m.Name, nil
		}
		if onlinePrimary(st) {
			primary = m.Name
		}
	}
	if primary == "" {
		return "", engine.ErrUnavailable
	}
	return primary, nil
}
func (p *Core) runBackup(ctx context.Context, rc *engine.Context, op *domain.Operation, cp backupCheckpoint) (engine.OpResult, error) {
	name, err := operationResource(op, "backup")
	if err != nil {
		return backupResult(cp, false), err
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return backupResult(cp, false), err
	}
	if b.Phase == domain.PhaseSucceeded {
		s, e := load(rc.Cluster)
		if e != nil {
			return backupResult(cp, false), e
		}
		if e = establishGTIDBaseline(ctx, rc, s, b); e != nil {
			return backupResult(cp, false), e
		}
		return backupResult(backupCheckpoint{Step: "done"}, true), nil
	}
	if b.Type != "full" && b.Type != "incremental" {
		return backupResult(cp, false), engine.Invalid("MySQL backup type must be full or incremental")
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return backupResult(cp, false), err
	}
	observationErr := checkPXCQuorumGTIDs(ctx, rc, s)
	if observationErr != nil {
		if _, observation := observationErr.(*myspec.GTIDQuorumError); !observation {
			return backupResult(cp, false), observationErr
		}
	}
	if b.Type == "incremental" {
		if incident := activeGTIDIncident(s); incident != nil {
			return backupResult(cp, false), engine.Invalid("%s", incident.message())
		}
		if observationErr != nil {
			return backupResult(cp, false), observationErr
		}
		if b.MySQL != nil {
			items, e := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
			if e != nil {
				return backupResult(cp, false), e
			}
			if !backupGTIDContinuitySafe(s, items, *b, now(rc)) {
				return backupResult(cp, false), engine.Invalid("incremental checkpoint crosses a PXC GTID discontinuity; take a new full backup")
			}
		}
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return backupResult(cp, false), err
	}
	storage, err := p.backupStorage(ctx, rc, b.Storage, rc.Cluster.Ref().Namespace)
	if err != nil {
		return backupResult(cp, false), err
	}
	if cp.Step == "" {
		cp.Member, err = backupMember(ctx, rc, s)
		if err != nil {
			return backupResult(cp, false), err
		}
		prefix := repoPrefix(storage, rc.Cluster, s, bs.Generation)
		label := "backup-" + secrets.Hash(op.ID)[:32]
		metadata := &domain.MySQLBackup{Label: label, Repository: prefix, Type: b.Type, ChainRootLabel: label, ClusterUID: s.UID, Generation: bs.Generation, ObjectPaths: []string{path.Join(prefix, "base", label)}}
		parentVersion := ""
		if b.Type == "incremental" {
			items, e := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
			if e != nil {
				return backupResult(cp, false), e
			}
			var parent *domain.MySQLBackup
			for _, item := range items {
				m := item.MySQL
				if item.Phase == domain.PhaseSucceeded && item.ProviderMatches(p.Name()) && m != nil && item.Storage == storage.Name && m.Repository == prefix && m.ClusterUID == s.UID && m.Generation == bs.Generation && m.PhysicalIdentity != "" && (parent == nil || m.EndTime.After(parent.EndTime)) {
					parent = m
				}
			}
			if parent == nil {
				return backupResult(cp, false), engine.Invalid("incremental backup requires a successful parent with physical provenance in the same MySQL repository incarnation; take a new full backup")
			}
			cp.Member, err = backupMemberIdentity(ctx, rc, s, parent.PhysicalIdentity)
			if err != nil {
				return backupResult(cp, false), engine.Invalid("incremental source data incarnation is unavailable; take a new full backup")
			}
			if !backupGTIDContinuitySafe(s, items, domain.Backup{MySQL: parent}, now(rc)) {
				return backupResult(cp, false), engine.Invalid("incremental parent crosses a PXC GTID discontinuity; take a new full backup")
			}
			metadata.PhysicalIdentity = parent.PhysicalIdentity
			metadata.ParentLabel = parent.Label
			metadata.ChainRootLabel = parent.ChainRootLabel
			metadata.FromLSN = parent.ToLSN
			parentVersion = parent.ServerVersion
		}
		source, e := live(ctx, rc, cp.Member)
		if e != nil {
			return backupResult(cp, false), e
		}
		if source.Version == "" {
			return backupResult(cp, false), engine.Invalid("backup source has no observed running version")
		}
		if b.Type == "incremental" && (source.PhysicalIdentity != metadata.PhysicalIdentity || major(source.Version) != major(parentVersion)) {
			return backupResult(cp, false), engine.Invalid("incremental source incarnation or server major changed; take a new full backup")
		}
		metadata.PhysicalIdentity = source.PhysicalIdentity
		b.MySQL = metadata
		b.Provider = p.Name()
		b.Storage = storage.Name
		b.SourceTopology = rc.Cluster.Spec.Topology
		b.SourceVersion = source.Version
		b.Location = metadata.ObjectPaths[0]
		b.OperationID = op.ID
		stamp := now(rc)
		b.StartedAt = &stamp
		b.Phase = domain.PhaseRunning
		b.RecoveryIdentity = "mysql-backup-" + secrets.Hash(b.Namespace + "/" + b.Cluster + "/" + metadata.Repository + "/" + b.Name)[:32]
		if err = captureBackupAccess(ctx, rc, b); err != nil {
			return backupResult(cp, false), err
		}
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return backupResult(cp, false), err
		}
		found := false
		for _, v := range bs.Storages {
			found = found || v == storage.Name
		}
		if !found {
			bs.Storages = append(bs.Storages, storage.Name)
			if err = saveBackupState(rc.Cluster, bs); err != nil {
				return backupResult(cp, false), err
			}
		}
		cp.Step = "dispatch"
		return backupResult(cp, false), nil
	}
	if b.MySQL == nil {
		return backupResult(cp, false), engine.Invalid("backup checkpoint has no immutable MySQL inventory")
	}
	if b.Type == "incremental" && b.MySQL.PhysicalIdentity == "" {
		return backupResult(cp, false), engine.Invalid("incremental backup checkpoint has no physical provenance; take a new full backup")
	}
	repo, err := repository(storage, b.MySQL.Repository)
	if err != nil {
		return backupResult(cp, false), err
	}
	var response myspec.BackupResponse
	if cp.Step == "dispatch" {
		if rc.Cluster.Spec.Backup.PITR.Enabled {
			writer, e := archiveWriter(ctx, rc, s)
			if e != nil || writer != cp.Member {
				return backupResult(cp, false), engine.Invalid("PITR archive writer changed before backup dispatch; take a fresh full from the current archive writer")
			}
		}
		err = mysqlAction(ctx, rc, cp.Member, myspec.ActionBackup, myspec.BackupRequest{Receipt: op.ID, Type: b.Type, BaseLSN: b.MySQL.FromLSN, Repository: repo, ObjectPrefix: b.MySQL.ObjectPaths[0], PhysicalIdentity: b.MySQL.PhysicalIdentity}, &response)
		if err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "poll"
		return backupResult(cp, false), nil
	}
	if err = mysqlAction(ctx, rc, cp.Member, myspec.ActionBackupInfo, map[string]string{"receipt": op.ID}, &response); err != nil {
		return backupResult(cp, false), err
	}
	if response.State == "failed" {
		b.Phase = domain.PhaseFailed
		b.Error = response.Error
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return backupResult(cp, false), err
		}
		return backupResult(cp, true), fmt.Errorf("MySQL backup failed: %s", response.Error)
	}
	if response.State != "done" {
		return backupResult(cp, false), nil
	}
	r := response.Result
	if r == nil || r.EndTime.IsZero() || r.ToLSN == 0 || r.ToLSN < r.FromLSN || r.ToolVersion == "" || major(r.ServerVersion) != major(b.SourceVersion) || r.ObjectPrefix != b.MySQL.ObjectPaths[0] || r.BackupType != b.Type || r.FromLSN != b.MySQL.FromLSN || b.MySQL.PhysicalIdentity != "" && r.PhysicalIdentity != b.MySQL.PhysicalIdentity {
		return backupResult(cp, false), engine.Invalid("agent returned inconsistent XtraBackup metadata")
	}
	if err = sealBackupManifest(ctx, rc, b, r.ManifestDigest, r.ManifestSize); err != nil {
		return backupResult(cp, false), err
	}
	b.MySQL.GTIDSet = r.GTIDSet
	b.MySQL.PhysicalIdentity = r.PhysicalIdentity
	b.MySQL.FromLSN = r.FromLSN
	b.MySQL.ToLSN = r.ToLSN
	b.MySQL.ServerVersion = r.ServerVersion
	b.MySQL.ToolVersion = r.ToolVersion
	b.MySQL.EndTime = r.EndTime
	b.SourceVersion = r.ServerVersion
	b.Size = r.Size
	b.CompletedAt = &r.EndTime
	b.LastWriteTime = &r.EndTime
	b.Phase = domain.PhaseSucceeded
	if err = rc.Store.UpdateBackup(ctx, b); err != nil {
		return backupResult(cp, false), err
	}
	if err = establishGTIDBaseline(ctx, rc, s, b); err != nil {
		return backupResult(cp, false), err
	}
	cp.Step = "done"
	return backupResult(cp, true), nil
}

func (p *Core) rememberRepository(ctx context.Context, rc *engine.Context, s *state) error {
	if rc.Cluster.Spec.Backup.Storage == "" {
		return nil
	}
	storage, err := p.backupStorage(ctx, rc, "", rc.Cluster.Ref().Namespace)
	if err != nil {
		return err
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	prefix := repoPrefix(storage, rc.Cluster, s, bs.Generation)
	if bs.Repositories == nil {
		bs.Repositories = map[string][]string{}
	}
	for _, old := range bs.Repositories[storage.Name] {
		if old == prefix {
			return nil
		}
	}
	bs.Repositories[storage.Name] = append(bs.Repositories[storage.Name], prefix)
	return saveBackupState(rc.Cluster, bs)
}
