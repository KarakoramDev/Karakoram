// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres/pgspec"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

// backupState contains identifiers only; repository credentials are resolved
// afresh from the sealed storage on every agent configuration or dispatch.
type backupState struct {
	Stanza             string         `json:"stanza,omitempty"`
	Replicas           bool           `json:"replicas,omitempty"`
	InitialQueued      bool           `json:"initialQueued,omitempty"`
	CloneComplete      bool           `json:"cloneComplete,omitempty"`
	CloneCredentials   bool           `json:"cloneCredentials,omitempty"`
	ManagedRolesHeld   bool           `json:"managedRolesHeld,omitempty"`
	Restore            *restoreSource `json:"restore,omitempty"`
	Generation         int            `json:"generation,omitempty"`
	RepositoryStorages []string       `json:"repositoryStorages,omitempty"`
	LineageStart       *time.Time     `json:"lineageStart,omitempty"`
}
type restoreSource struct {
	Namespace string `json:"namespace"`
	Cluster   string `json:"cluster"`
	Backup    string `json:"backup"`
	Time      string `json:"time,omitempty"`
}

func loadBackupState(c *domain.Cluster) (backupState, error) {
	var root map[string]json.RawMessage
	var s backupState
	if len(c.Internal) > 0 {
		if err := json.Unmarshal(c.Internal, &root); err != nil {
			return s, err
		}
		if len(root["postgresBackup"]) > 0 {
			if err := json.Unmarshal(root["postgresBackup"], &s); err != nil {
				return s, err
			}
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
	root["postgresBackup"] = b
	c.Internal, err = json.Marshal(root)
	return err
}
func repository(storage *domain.BackupStorage, prefix string) (pgspec.Repository, error) {
	u, err := url.Parse(storage.EndpointURL)
	if err != nil || u.Host == "" || (u.Scheme != "http" && u.Scheme != "https") {
		return pgspec.Repository{}, engine.Invalid("invalid backup storage endpoint")
	}
	port := 0
	if u.Port() != "" {
		port, err = strconv.Atoi(u.Port())
		if err != nil {
			return pgspec.Repository{}, err
		}
	}
	return pgspec.Repository{
		Stanza: "kk",
		Path:   "/" + strings.Trim(prefix, "/"),
		S3: pgspec.S3{
			Endpoint: storage.EndpointURL, Port: port,
			Bucket: storage.Bucket, Region: storage.Region,
			PathStyle: storage.ForcePathStyle, VerifyTLS: storage.VerifyTLS,
			AccessKey: storage.Credentials.AccessKeyID,
			SecretKey: storage.Credentials.SecretAccessKey,
		},
	}, nil
}
func repoPrefix(storage *domain.BackupStorage, c *domain.Cluster, s *state) string {
	identity := c.Name + "-" + s.UID
	if s.RepositoryGeneration > 0 {
		identity += "-g" + strconv.Itoa(s.RepositoryGeneration)
	}
	return path.Join(strings.Trim(storage.Prefix, "/"), "postgres", c.Ref().Namespace, identity)
}
func (p *Core) backupStorage(ctx context.Context, rc *engine.Context, name, namespace string) (*domain.BackupStorage, error) {
	st := rc.Store
	if st == nil {
		st = p.options.Store
	}
	if st == nil {
		return nil, engine.ErrUnavailable
	}
	if name == "" {
		name = rc.Cluster.Spec.Backup.Storage
	}
	if name == "" {
		return nil, engine.Invalid("backup storage is required")
	}
	return st.GetBackupStorage(ctx, namespace, name)
}
func primary(ctx context.Context, rc *engine.Context, s *state) (string, pgspec.StatusResponse, error) {
	for _, m := range s.data() {
		st, err := live(ctx, rc, m.Name)
		if err != nil {
			continue
		}
		if st.Running && (st.Role == "primary" || st.Role == "standalone") {
			return m.Name, st, nil
		}
	}
	return "", pgspec.StatusResponse{}, engine.ErrUnavailable
}
func pgAction(ctx context.Context, rc *engine.Context, member, action string, request, response any) error {
	a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), member)
	if err != nil {
		return fmt.Errorf("connect PostgreSQL member: %w", err)
	}
	return a.Engine(ctx, "postgres", action, request, response)
}

// EnsureStanza creates the configured repository's stanza, or upgrades its
// database history after a major upgrade. Both actions are idempotent.
func (p *Core) EnsureStanza(ctx context.Context, rc *engine.Context, upgrade bool) error {
	s, err := load(rc.Cluster)
	if err != nil {
		return err
	}
	member, _, err := primary(ctx, rc, s)
	if err != nil {
		return err
	}
	action := "stanza-create"
	if upgrade {
		action = "stanza-upgrade"
	}
	if err := pgAction(ctx, rc, member, action, pgspec.RepositoryRequest{}, nil); err != nil {
		return err
	}
	bs, err := loadBackupState(rc.Cluster)
	if err != nil {
		return err
	}
	for _, storageName := range bs.RepositoryStorages {
		if storageName == rc.Cluster.Spec.Backup.Storage {
			continue
		}
		storage, err := p.backupStorage(ctx, rc, storageName, rc.Cluster.Ref().Namespace)
		if err != nil {
			return err
		}
		repo, err := repository(storage, repoPrefix(storage, rc.Cluster, s))
		if err != nil {
			return err
		}
		if err = pgAction(ctx, rc, member, action, pgspec.RepositoryRequest{Repository: &repo}, nil); err != nil {
			return err
		}
	}
	return nil
}

// EnqueueFullBackup atomically queues a deterministic fresh base. Repeating the
// same key after a crash does not create a second backup or operation.
func (p *Core) EnqueueFullBackup(ctx context.Context, rc *engine.Context, key string) error {
	name := "full-" + secrets.Hash(key)[:24]
	if _, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name); err == nil {
		return nil
	} else if !missing(err) {
		return err
	}
	b := &domain.Backup{
		Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name,
		Name: name, Provider: rc.Cluster.Spec.Provider,
		Storage: rc.Cluster.Spec.Backup.Storage, Type: "full",
		Trigger: "restore", Phase: domain.PhasePending, CreatedAt: now(rc),
	}
	payload, _ := json.Marshal(map[string]string{"backup": name})
	op := &domain.Operation{Namespace: b.Namespace, Cluster: b.Cluster, Kind: domain.OperationBackup, Payload: payload}
	return rc.Store.EnqueueBackup(ctx, b, op)
}

type backupCheckpoint struct {
	Step                 string         `json:"step"`
	Member               string         `json:"member,omitempty"`
	Label                string         `json:"label,omitempty"`
	Timeline             int            `json:"timeline,omitempty"`
	Source               *restoreSource `json:"source,omitempty"`
	Index                int            `json:"index,omitempty"`
	Generation           int            `json:"generation,omitempty"`
	RepositoryGeneration int            `json:"repositoryGeneration,omitempty"`
	Failure              string         `json:"failure,omitempty"`
}

func backupResult(cp backupCheckpoint, done bool) engine.OpResult {
	raw, _ := json.Marshal(cp)
	return engine.OpResult{Done: done, Step: cp.Step, Checkpoint: raw, RequeueAfter: 2 * time.Second}
}
func operationResource(op *domain.Operation, key string) (string, error) {
	var payload map[string]string
	if err := json.Unmarshal(op.Payload, &payload); err != nil {
		return "", fmt.Errorf("decode operation payload: %w", err)
	}
	if payload[key] == "" {
		return "", engine.Invalid("operation has no %s", key)
	}
	return payload[key], nil
}
func backupEvent(ctx context.Context, rc *engine.Context, op *domain.Operation, reason, message string) error {
	if rc.Events == nil {
		return nil
	}
	return rc.Events.Record(ctx, domain.Event{
		Namespace: rc.Cluster.Ref().Namespace, Cluster: rc.Cluster.Name,
		Operation: op.ID, Type: "Normal", Reason: reason,
		Message: message, CreatedAt: now(rc),
	})
}
func (p *Core) runBackupOperation(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	var cp backupCheckpoint
	if len(op.Checkpoint) > 0 {
		if err := json.Unmarshal(op.Checkpoint, &cp); err != nil {
			return engine.OpResult{}, fmt.Errorf("decode PostgreSQL backup checkpoint: %w", err)
		}
	}
	if cp.Step == "failed" {
		return backupResult(cp, true), engine.Invalid("%s", cp.Failure)
	}
	if cp.Step == "done" {
		return backupResult(cp, true), nil
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
func (p *Core) runBackup(ctx context.Context, rc *engine.Context, op *domain.Operation, cp backupCheckpoint) (engine.OpResult, error) {
	name, err := operationResource(op, "backup")
	if err != nil {
		return backupResult(cp, false), err
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return backupResult(cp, false), err
	}
	if b.Type != "full" && b.Type != "diff" && b.Type != "incr" {
		return backupResult(cp, false), engine.Invalid("backup type must be full, diff or incr")
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return backupResult(cp, false), err
	}
	storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
	if err != nil {
		return backupResult(cp, false), err
	}
	prefix := repoPrefix(storage, rc.Cluster, s)
	if b.Postgres != nil && b.Postgres.Repository != "" {
		prefix = b.Postgres.Repository
	}
	repo, err := repository(storage, prefix)
	if err != nil {
		return backupResult(cp, false), err
	}
	member, st, err := primary(ctx, rc, s)
	if err != nil {
		return backupResult(cp, false), err
	}
	if cp.Member != "" && b.Phase == domain.PhaseRunning && (cp.Member != member || cp.Timeline != st.Timeline) {
		stamp := now(rc)
		b.Phase = domain.PhaseFailed
		b.CompletedAt = &stamp
		b.Error = "primary changed during pgBackRest backup"
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return backupResult(cp, false), err
		}
		return backupResult(cp, true), errors.New(b.Error)
	}
	if b.Phase != domain.PhaseRunning && cp.Step != "inventory" {
		cp.Member = member
		cp.Timeline = st.Timeline
	}
	switch cp.Step {
	case "":
		cp.Member = member
		cp.Timeline = st.Timeline
		cp.Step = "stanza"
		b.Provider = rc.Cluster.Spec.Provider
		b.Storage = storage.Name
		b.SourceTopology = rc.Cluster.Spec.Topology
		if st.Version == "" {
			return backupResult(cp, false), engine.ErrUnavailable
		}
		b.SourceVersion = st.Version
		b.Postgres = &domain.PostgresBackup{Repository: prefix, SystemID: st.SystemID, Version: st.Version}
		b.RecoveryIdentity = "backup-" + secrets.Hash(b.Namespace + "/" + b.Cluster + "/" + prefix + "/" + b.Name)[:32]
		for _, user := range systemNames {
			pw, err := rc.Secrets.GetClusterSecret(ctx, rc.Cluster.Ref(), "users/"+user)
			if err != nil {
				return backupResult(cp, false), err
			}
			if err = rc.Secrets.SetClusterSecret(ctx, domain.ClusterRef{Namespace: b.Namespace, Name: b.RecoveryIdentity}, "users/"+user, pw); err != nil {
				return backupResult(cp, false), err
			}
		}
		if err = p.captureManagedRoles(ctx, rc, b); err != nil {
			return backupResult(cp, false), err
		}
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return backupResult(cp, false), err
		}
	case "stanza":
		if err = pgAction(ctx, rc, member, "stanza-create", pgspec.RepositoryRequest{Repository: &repo}, nil); err != nil {
			return backupResult(cp, false), err
		}
		bs, err := loadBackupState(rc.Cluster)
		if err != nil {
			return backupResult(cp, false), err
		}
		if b.Storage != rc.Cluster.Spec.Backup.Storage && !slices.Contains(bs.RepositoryStorages, b.Storage) {
			bs.RepositoryStorages = append(bs.RepositoryStorages, b.Storage)
			if err = saveBackupState(rc.Cluster, bs); err != nil {
				return backupResult(cp, false), err
			}
		}
		cp.Step = "publish"
	case "publish":
		if err := p.reloadBackupMembers(ctx, rc, s); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "backup"
	case "backup":
		var response pgspec.BackupResponse
		err = pgAction(ctx, rc, member, "backup", pgspec.BackupRequest{Receipt: op.ID, Type: b.Type, Repository: &repo}, &response)
		if err != nil {
			return backupResult(cp, false), err
		}
		if b.StartedAt == nil {
			stamp := now(rc)
			b.StartedAt = &stamp
			b.Phase = domain.PhaseRunning
			if err = backupEvent(ctx, rc, op, "BackupStarted", "pgBackRest backup started"); err != nil {
				return backupResult(cp, false), err
			}
		}
		if response.State == "failed" {
			stamp := now(rc)
			b.Phase = domain.PhaseFailed
			b.CompletedAt = &stamp
			b.Error = response.Message
			if err = rc.Store.UpdateBackup(ctx, b); err != nil {
				return backupResult(cp, false), err
			}
			return backupResult(cp, true), errors.New("pgBackRest backup failed")
		}
		if response.State == "done" {
			cp.Label = response.Label
			cp.Step = "inventory"
		}
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return backupResult(cp, false), err
		}
	case "inventory":
		var inv pgspec.BackupInfoResponse
		if err = pgAction(ctx, rc, member, "backup-info", pgspec.RepositoryRequest{Repository: &repo}, &inv); err != nil {
			return backupResult(cp, false), err
		}
		found := false
		for _, item := range inv.Backups {
			if item.Label != cp.Label {
				continue
			}
			start, e := time.Parse(time.RFC3339Nano, item.StartedAt)
			if e != nil {
				return backupResult(cp, false), e
			}
			stop, e := time.Parse(time.RFC3339Nano, item.StoppedAt)
			if e != nil {
				return backupResult(cp, false), e
			}
			// Native inventory is authoritative if it disagrees with the
			// observed source major; never stamp the desired target version.
			if item.Version == "" {
				return backupResult(cp, false), errors.New("backup inventory has no PostgreSQL version")
			}
			if major(b.SourceVersion) != major(item.Version) {
				b.SourceVersion = item.Version
			}
			b.StartedAt = &start
			b.CompletedAt = &stop
			b.LastWriteTime = &stop
			b.Size = item.SizeBytes
			b.Phase = domain.PhaseSucceeded
			b.Location = prefix + "/backup/kk/" + item.Label
			b.Postgres = &domain.PostgresBackup{
				Label: item.Label, Repository: prefix,
				Prior: item.Prior, References: item.References,
				WALStart: item.WALStart, WALStop: item.WALStop,
				SystemID: item.SystemID, Version: item.Version,
				RepoBytes: item.RepoBytes,
			}
			found = true
		}
		if !found {
			return backupResult(cp, false), errors.New("completed backup missing from pgBackRest inventory")
		}
		if err = rc.Store.UpdateBackup(ctx, b); err != nil {
			return backupResult(cp, false), err
		}
		bs, err := loadBackupState(rc.Cluster)
		if err != nil {
			return backupResult(cp, false), err
		}
		if b.Type == "full" {
			bs.Replicas = true
			if err = saveBackupState(rc.Cluster, bs); err != nil {
				return backupResult(cp, false), err
			}
			if err = p.reloadBackupMembers(ctx, rc, s); err != nil {
				return backupResult(cp, false), err
			}
		}
		if err = backupEvent(ctx, rc, op, "BackupCompleted", "pgBackRest backup completed"); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "done"
		return backupResult(cp, true), nil
	default:
		return backupResult(cp, false), engine.Invalid("unknown backup checkpoint %q", cp.Step)
	}
	return backupResult(cp, false), nil
}
func (p *Core) reloadBackupMembers(ctx context.Context, rc *engine.Context, s *state) error {
	// MemberConfig is pulled from the durable cluster state. Persisted backup
	// state is visible to the immediate reload as well as the next regular pull.
	for _, m := range s.data() {
		a, err := rc.Agents.Dial(ctx, rc.Cluster.Ref(), m.Name)
		if err != nil {
			return err
		}
		if err = a.Reload(ctx); err != nil {
			return err
		}
	}
	return nil
}
func removedBackupLabels(items []domain.Backup, label string) map[string]bool {
	removed := map[string]bool{label: true}
	for changed := true; changed; {
		changed = false
		for _, b := range items {
			if b.Postgres == nil || removed[b.Postgres.Label] {
				continue
			}
			dep := removed[b.Postgres.Prior]
			for _, ref := range b.Postgres.References {
				dep = dep || removed[ref]
			}
			if dep {
				removed[b.Postgres.Label] = true
				changed = true
			}
		}
	}
	return removed
}
func (p *Core) runDeleteBackup(ctx context.Context, rc *engine.Context, op *domain.Operation, cp backupCheckpoint) (engine.OpResult, error) {
	name, err := operationResource(op, "backup")
	if err != nil {
		return backupResult(cp, false), err
	}
	b, err := rc.Store.GetBackup(ctx, rc.Cluster.Ref(), name)
	if missing(err) {
		cp.Step = "done"
		return backupResult(cp, true), nil
	}
	if err != nil {
		return backupResult(cp, false), err
	}
	if b.Postgres == nil || b.Postgres.Label == "" {
		if err = deleteRecoverySecrets(ctx, rc.Secrets, *b); err != nil {
			return backupResult(cp, false), err
		}
		if err = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), name); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "done"
		return backupResult(cp, true), nil
	}
	storage, err := p.backupStorage(ctx, rc, b.Storage, b.Namespace)
	if err != nil {
		return backupResult(cp, false), err
	}
	repo, err := repository(storage, b.Postgres.Repository)
	if err != nil {
		return backupResult(cp, false), err
	}
	s, err := load(rc.Cluster)
	if err != nil {
		return backupResult(cp, false), err
	}
	if b.Postgres.Repository != repoPrefix(storage, rc.Cluster, s) {
		// No member writes to retired generations. Use the same native-inventory
		// cleanup as kept backups, including removal of the final full set.
		if err = deleteRetiredBackup(ctx, *storage, *b, rc.Store, rc.Secrets); err != nil {
			return backupResult(cp, false), err
		}
		cp.Step = "done"
		return backupResult(cp, true), nil
	}
	member, _, err := primary(ctx, rc, s)
	if err != nil {
		return backupResult(cp, false), err
	}
	if cp.Step == "" {
		var inventory pgspec.BackupInfoResponse
		if err = pgAction(ctx, rc, member, "backup-info", pgspec.RepositoryRequest{Repository: &repo}, &inventory); err != nil {
			return backupResult(cp, false), err
		}
		if b.Type == "full" {
			present, newerFull := false, false
			for _, item := range inventory.Backups {
				present = present || item.Label == b.Postgres.Label
				if item.Type == "full" && item.Label > b.Postgres.Label {
					newerFull = true
				}
			}
			if present && !newerFull {
				return p.failBackupDeletion(ctx, rc, b, cp, "pgBackRest cannot expire the newest full backup; create a newer full backup before deleting this set")
			}
		}
		if err = pgAction(ctx, rc, member, "expire", pgspec.ExpireRequest{Set: b.Postgres.Label, Repository: &repo}, nil); err != nil {
			return p.failBackupDeletion(ctx, rc, b, cp, fmt.Sprintf("pgBackRest expire failed for set %s: %v; inspect repository inventory before retrying deletion", b.Postgres.Label, err))
		}
		cp.Step = "observe"
		cp.Label = b.Postgres.Label
		return backupResult(cp, false), nil
	}
	var inv pgspec.BackupInfoResponse
	if err = pgAction(ctx, rc, member, "backup-info", pgspec.RepositoryRequest{Repository: &repo}, &inv); err != nil {
		return backupResult(cp, false), err
	}
	present := map[string]bool{}
	for _, item := range inv.Backups {
		present[item.Label] = true
	}
	if present[cp.Label] {
		return p.failBackupDeletion(ctx, rc, b, cp, fmt.Sprintf("pgBackRest expire returned without removing set %s; backup retained and deletion failed", cp.Label))
	}
	items, err := rc.Store.ListBackups(ctx, rc.Cluster.Ref())
	if err != nil {
		return backupResult(cp, false), err
	}
	removed := removedBackupLabels(items, cp.Label)
	for _, item := range items {
		if item.Storage != b.Storage || item.Postgres == nil || item.Postgres.Repository != b.Postgres.Repository || !removed[item.Postgres.Label] {
			continue
		}
		if present[item.Postgres.Label] {
			return backupResult(cp, false), errors.New("pgBackRest retained an expected expired dependent")
		}
		if err = deleteRecoverySecrets(ctx, rc.Secrets, item); err != nil {
			return backupResult(cp, false), err
		}
		if err = rc.Store.DeleteBackup(ctx, rc.Cluster.Ref(), item.Name); err != nil && !missing(err) {
			return backupResult(cp, false), err
		}
	}
	if err = backupEvent(ctx, rc, op, "BackupDeleted", "pgBackRest backup and dependent sets deleted"); err != nil {
		return backupResult(cp, false), err
	}
	cp.Step = "done"
	return backupResult(cp, true), nil
}

func (p *Core) failBackupDeletion(ctx context.Context, rc *engine.Context, b *domain.Backup, cp backupCheckpoint, message string) (engine.OpResult, error) {
	// A rejected deletion must release the serialized queue so the newer full
	// needed for expiration can run. Keep the backup available for a later retry.
	b.Phase = domain.PhaseSucceeded
	if err := rc.Store.UpdateBackup(ctx, b); err != nil {
		return backupResult(cp, false), err
	}
	cp.Step, cp.Failure = "failed", message
	return backupResult(cp, true), engine.Invalid("%s", message)
}
