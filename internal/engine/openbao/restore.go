// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package openbao

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao/baoapi"
	"github.com/minio/minio-go/v7"
)

// RestoreUnsealSecret is a write-only request staging key. Its contents never
// enter an operation checkpoint, cluster spec, or native member configuration.
func RestoreUnsealSecret(name string) string { return "openbao/restore-unseal/" + name }

type RestoreUnsealShares struct {
	Shares    []string `json:"shares"`
	Threshold int      `json:"threshold"`
	SealType  string   `json:"sealType,omitempty"`
	Recovery  bool     `json:"recovery,omitempty"`
}

func (v RestoreUnsealShares) Validate() error {
	if v.Threshold < 1 || v.Threshold > 10 || len(v.Shares) < v.Threshold || len(v.Shares) > 10 {
		return invalid("openbaoUnsealShares requires shares and a valid threshold (1..10)")
	}
	seen := map[string]bool{}
	for _, share := range v.Shares {
		if share == "" || seen[share] {
			return invalid("openbaoUnsealShares requires nonempty distinct shares")
		}
		seen[share] = true
	}
	if v.SealType != "" && v.SealType != "shamir" && v.SealType != "transit" && v.SealType != "awskms" {
		return invalid("invalid openbaoUnsealShares sealType")
	}
	if v.Recovery && v.SealType == "shamir" {
		return invalid("Shamir snapshots require unseal shares, not recovery shares")
	}
	return nil
}
func snapshotSource(ctx context.Context, rc *engine.Context, source domain.ClusterRef, name string) (*domain.Backup, error) {
	if source.Namespace == "" {
		source.Namespace = rc.Cluster.Ref().Namespace
	}
	if source.Name == "" {
		source.Name = rc.Cluster.Name
	}
	if name == "" {
		return nil, invalid("OpenBao restore requires a raft-snapshot backup; PITR is not supported")
	}
	b, err := rc.Store.GetBackup(ctx, source, name)
	if err != nil {
		return nil, err
	}
	if b.Provider != "openbao" || b.Type != "raft-snapshot" || b.Phase != domain.PhaseSucceeded || b.Location == "" {
		return nil, invalid("restore source is not a successful OpenBao raft-snapshot backup")
	}
	return b, nil
}

// SnapshotBelongsToCluster compares immutable Raft backup provenance, not just
// names: a deleted cluster's name may already have been reused.
func SnapshotBelongsToCluster(c *domain.Cluster, b *domain.Backup) bool {
	if c == nil || b == nil || b.Provider != "openbao" || b.Type != "raft-snapshot" || b.Cluster != c.Name || domain.NormalizedNamespace(b.Namespace) != c.Ref().Namespace {
		return false
	}
	s, err := loadState(c)
	if err != nil || s.UID == "" {
		return false
	}
	parts := strings.Split(b.Location, "/")
	return len(parts) >= 3 && parts[len(parts)-3] == s.UID && parts[len(parts)-2] == "raft" && parts[len(parts)-1] == b.Name+".snap"
}

func (p *Provider) prepareSnapshotMaterial(ctx context.Context, rc *engine.Context, source domain.ClusterRef, request string, force bool, b *domain.Backup) error {
	if !force {
		return nil
	}
	targetOptions, err := parseOptions(rc.Cluster.Spec.EngineOptions)
	if err != nil {
		return err
	}
	var raw []byte
	err = engine.ErrNotFound
	// A retained backup may be restored under its former name. The newly
	// bootstrapped target's keys must not be mistaken for that deleted source.
	useSource := source != rc.Cluster.Ref()
	if useSource {
		current, lookupErr := rc.Store.GetCluster(ctx, source)
		if lookupErr != nil && !missing(lookupErr) {
			return lookupErr
		}
		if lookupErr == nil && !SnapshotBelongsToCluster(current, b) {
			useSource = false
		}
	}
	if useSource {
		raw, err = rc.Secrets.GetClusterSecret(ctx, source, SecretUnseal)
	}
	if missing(err) {
		raw, err = secret(ctx, rc, RestoreUnsealSecret(request))
		if missing(err) {
			return invalid("restoring an OpenBao snapshot from a deleted source requires openbaoUnsealShares")
		}
		if err != nil {
			return err
		}
		var supplied RestoreUnsealShares
		if json.Unmarshal(raw, &supplied) != nil {
			return invalid("invalid supplied OpenBao restore material")
		}
		if err = supplied.Validate(); err != nil {
			return err
		}
		seal := supplied.SealType
		if seal == "" {
			seal = targetOptions.Seal.Type
		}
		raw, err = json.Marshal(storedUnseal{Shares: supplied.Shares, Threshold: supplied.Threshold, SealType: seal, Recovery: supplied.Recovery || seal != "shamir"})
	}
	if err != nil {
		return err
	}
	var material storedUnseal
	if json.Unmarshal(raw, &material) != nil || material.Threshold < 1 || len(material.Shares) < material.Threshold {
		return invalid("source OpenBao unseal material is unavailable")
	}
	if material.SealType != targetOptions.Seal.Type {
		return invalid("source snapshot seal type must match the target OpenBao seal configuration")
	}
	if material.Recovery && targetOptions.Seal.Type == "shamir" {
		return invalid("recovery shares cannot unseal a Shamir target")
	}
	return putSecret(ctx, rc, SecretUnseal, raw)
}

func (p *Provider) importSnapshot(ctx context.Context, rc *engine.Context, s *internalState, b *domain.Backup, force bool) error {
	ctx, cancel := context.WithTimeout(ctx, 10*time.Minute)
	defer cancel()
	storage, err := snapshotStorage(ctx, rc, b)
	if err != nil {
		return err
	}
	if err = snapshotLocation(*storage, *b); err != nil {
		return err
	}
	client, closeClient, err := snapshotS3Client(*storage, storage.Credentials)
	if err != nil {
		return err
	}
	defer closeClient()
	object, err := client.GetObject(ctx, storage.Bucket, b.Location, minio.GetObjectOptions{})
	if err != nil {
		return snapshotS3Error("download", err)
	}
	defer object.Close()
	info, err := object.Stat()
	if err != nil {
		return snapshotS3Error("download", err)
	}
	if info.Size <= 0 || info.Size > maxSnapshotBytes {
		return invalid("snapshot is empty or exceeds size limit")
	}
	token, err := secret(ctx, rc, SecretManagementToken)
	if err != nil {
		return err
	}
	native, _, err := p.activeClient(ctx, rc, s, string(token))
	if err != nil {
		return err
	}
	return native.WithTimeout(10*time.Minute).RestoreSnapshot(ctx, &snapshotBoundedReader{r: io.LimitReader(object, info.Size), remaining: maxSnapshotBytes}, force)
}

type snapshotRestoreMarker struct {
	Policy   string `json:"policy"`
	Location string `json:"location"`
	Armed    bool   `json:"armed"`
}

// The random ACL policy is a barrier-backed marker written AFTER the snapshot.
// Restoring that snapshot removes it, including on same-cluster restores that
// neither seal members nor invalidate a still-present token. Persisting Armed
// only after the policy write prevents a lost marker write from proving import.
func snapshotMarker(ctx context.Context, rc *engine.Context, native *baoapi.Client, b *domain.Backup, request string) (snapshotRestoreMarker, error) {
	key := RestoreUnsealSecret(request) + "/marker"
	var marker snapshotRestoreMarker
	raw, err := secret(ctx, rc, key)
	if missing(err) {
		var id [32]byte
		if _, err = rand.Read(id[:]); err != nil {
			return marker, err
		}
		marker = snapshotRestoreMarker{Policy: "karakoram-restore-" + hex.EncodeToString(id[:]), Location: b.Location}
		raw, err = json.Marshal(marker)
		if err != nil {
			return marker, err
		}
		if err = putSecret(ctx, rc, key, raw); err != nil {
			return marker, err
		}
	} else if err != nil {
		return marker, err
	} else if json.Unmarshal(raw, &marker) != nil || marker.Location != b.Location || !strings.HasPrefix(marker.Policy, "karakoram-restore-") || len(marker.Policy) != len("karakoram-restore-")+64 {
		return marker, invalid("invalid OpenBao restore marker")
	}
	if !marker.Armed {
		// A deny-only ACL grants no additional access to any identity.
		if err = native.Do(ctx, http.MethodPut, "sys/policies/acl/"+marker.Policy, map[string]any{"policy": `path "karakoram/restore-marker" { capabilities = ["deny"] }`}, nil); err != nil {
			return marker, err
		}
		marker.Armed = true
		raw, err = json.Marshal(marker)
		if err != nil {
			return marker, err
		}
		if err = putSecret(ctx, rc, key, raw); err != nil {
			return marker, err
		}
	}
	return marker, nil
}

func previousSnapshotMaterial(ctx context.Context, rc *engine.Context, request string) ([]storedUnseal, error) {
	raw, err := secret(ctx, rc, RestoreUnsealSecret(request)+"/previous")
	if missing(err) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var material storedUnseal
	if json.Unmarshal(raw, &material) != nil || material.Threshold < 1 || len(material.Shares) < material.Threshold {
		return nil, invalid("invalid previous OpenBao restore material")
	}
	return []storedUnseal{material}, nil
}

type snapshotSealRefresh struct {
	ConfigVersion     int64 `json:"configVersion"`
	ProcessGeneration int64 `json:"processGeneration"`
	Ready             bool  `json:"ready"`
}

// A foreign Shamir snapshot can seal a process before postUnseal invalidates
// its cached barrier configuration. Refresh only an already sealed process,
// and persist its exact generation before publishing config or requesting reload.
// This is access recovery, never evidence that snapshot import succeeded.
func (p *Provider) refreshSnapshotSeal(ctx context.Context, rc *engine.Context, s *internalState, request string, m *member, create bool) (bool, error) {
	key := RestoreUnsealSecret(request) + "/seal-refresh"
	plans := map[string]snapshotSealRefresh{}
	raw, err := secret(ctx, rc, key)
	if err != nil && !missing(err) {
		return false, err
	}
	if err == nil && json.Unmarshal(raw, &plans) != nil {
		return false, invalid("invalid OpenBao restore seal refresh")
	}
	if plans == nil {
		return false, invalid("invalid OpenBao restore seal refresh")
	}
	save := func() error {
		raw, err := json.Marshal(plans)
		if err != nil {
			return err
		}
		return putSecret(ctx, rc, key, raw)
	}
	plan, exists := plans[m.Name]
	if !exists {
		if !create {
			return false, nil
		}
		plan = snapshotSealRefresh{ConfigVersion: s.ConfigVersion + 1, ProcessGeneration: m.ProcessGeneration + 1}
		plans[m.Name] = plan
		return true, save()
	}
	if plan.ConfigVersion < 1 || plan.ProcessGeneration < 1 {
		return false, invalid("invalid OpenBao restore seal refresh fence")
	}
	if plan.Ready {
		return false, nil
	}
	if m.ConfigVersion < plan.ConfigVersion || m.ProcessGeneration < plan.ProcessGeneration {
		client, err := p.nativeClient(ctx, rc, m.Name, "")
		if err != nil {
			return true, err
		}
		seal, err := client.SealStatus(ctx)
		if err != nil {
			return true, err
		}
		if !seal.Sealed {
			plan.Ready = true
			plans[m.Name] = plan
			return true, save()
		}
		if s.ConfigVersion < plan.ConfigVersion {
			s.ConfigVersion = plan.ConfigVersion
		}
		activateConfig(rc.Cluster, s, m)
		if m.ProcessGeneration < plan.ProcessGeneration {
			m.ProcessGeneration = plan.ProcessGeneration
		}
		// Yield once so the ordinary operation/Sync persistence boundary
		// publishes this exact config before the agent is asked to reload it.
		return true, s.save(rc.Cluster)
	}
	status, err := p.agentStatus(ctx, rc, m.Name)
	if err != nil {
		return true, err
	}
	native, err := p.nativeStatus(ctx, rc, m.Name)
	if err != nil {
		return true, err
	}
	if status.ConfigVersion >= plan.ConfigVersion && status.Processes["openbao"].State == "running" && native.ProcessGeneration >= plan.ProcessGeneration {
		plan.Ready = true
		plans[m.Name] = plan
		return true, save()
	}
	// Recheck at the disruption boundary. An externally recovered member
	// must not be restarted merely because an earlier pass found it sealed.
	client, err := p.nativeClient(ctx, rc, m.Name, "")
	if err != nil {
		return true, err
	}
	seal, err := client.SealStatus(ctx)
	if err != nil {
		return true, err
	}
	if !seal.Sealed {
		plan.Ready = true
		plans[m.Name] = plan
		return true, save()
	}
	agent, err := p.agent(ctx, rc, m.Name)
	if err != nil {
		return true, err
	}
	defer agent.Close()
	return true, agent.Reload(ctx)
}

// Recover at most one sealed member per pass; normal Sync is suspended while
// restore owns convergence. Native thresholds and rejections select keys.
func (p *Provider) recoverSnapshotMember(ctx context.Context, rc *engine.Context, s *internalState, request string, material storedUnseal, previous []storedUnseal) (bool, error) {
	for _, m := range ordered(s) {
		if busy, err := p.refreshSnapshotSeal(ctx, rc, s, request, m, false); busy || err != nil {
			return busy, err
		}
		native, err := p.nativeClient(ctx, rc, m.Name, "")
		if err != nil {
			return false, err
		}
		status, err := native.SealStatus(ctx)
		if err != nil {
			return false, err
		}
		if !status.Sealed {
			continue
		}
		for i := 0; i <= len(previous); i++ {
			candidate := material
			if i > 0 {
				candidate = previous[i-1]
			}
			if candidate.Recovery {
				return false, fmt.Errorf("%w: waiting for OpenBao auto-unseal during restore", engine.ErrUnavailable)
			}
			status, err = native.UnsealWith(ctx, candidate.Shares[:candidate.Threshold])
			if err == nil || (!errors.Is(err, baoapi.ErrInsufficientShares) && !baoapi.IsStatus(err, http.StatusBadRequest)) {
				break
			}
		}
		if err != nil {
			if len(previous) > 0 && (errors.Is(err, baoapi.ErrInsufficientShares) || baoapi.IsStatus(err, http.StatusBadRequest)) {
				if busy, refreshErr := p.refreshSnapshotSeal(ctx, rc, s, request, m, true); busy || refreshErr != nil {
					return busy, refreshErr
				}
			}
			return false, err
		}
		if status.Sealed {
			return false, fmt.Errorf("%w: OpenBao member remains sealed during restore", engine.ErrUnavailable)
		}
		return true, nil
	}
	return false, nil
}

// Import is replayed only while its snapshot-specific marker remains. A token
// error or a sealed member is not evidence that the snapshot was applied.
func (p *Provider) snapshotRestoreStep(ctx context.Context, rc *engine.Context, s *internalState, b *domain.Backup, source domain.ClusterRef, request, step string) (string, error) {
	force := source != rc.Cluster.Ref() || !SnapshotBelongsToCluster(rc.Cluster, b)
	switch step {
	case "":
		return "prepare", nil
	case "prepare":
		material, err := readUnseal(ctx, rc)
		if err != nil {
			return step, err
		}
		previous, err := previousSnapshotMaterial(ctx, rc, request)
		if err != nil {
			return step, err
		}
		if len(previous) > 0 {
			material = previous[0] // A prepare replay may follow the foreign-key swap.
		}
		if recovered, err := p.recoverSnapshotMember(ctx, rc, s, request, material, nil); err != nil || recovered {
			return step, err
		}
		token, err := p.ensureManagementTokenWithMaterials(ctx, rc, s, previous)
		if err != nil {
			return step, err
		}
		native, _, err := p.activeClient(ctx, rc, s, token)
		if err != nil {
			return step, err
		}
		if _, err = snapshotMarker(ctx, rc, native, b, request); err != nil {
			return step, err
		}
		if force {
			key := RestoreUnsealSecret(request) + "/previous"
			if _, err := secret(ctx, rc, key); missing(err) {
				raw, err := secret(ctx, rc, SecretUnseal)
				if err != nil {
					return step, err
				}
				if err = putSecret(ctx, rc, key, raw); err != nil {
					return step, err
				}
			} else if err != nil {
				return step, err
			}
		}
		if err := p.prepareSnapshotMaterial(ctx, rc, source, request, force, b); err != nil {
			return step, err
		}
		return "post", nil
	case "post":
		material, err := readUnseal(ctx, rc)
		if err != nil {
			return step, err
		}
		previous, err := previousSnapshotMaterial(ctx, rc, request)
		if err != nil {
			return step, err
		}
		if recovered, err := p.recoverSnapshotMember(ctx, rc, s, request, material, previous); err != nil || recovered {
			return step, err
		}
		token, err := p.ensureManagementTokenWithMaterials(ctx, rc, s, previous)
		if err != nil {
			return step, err
		}
		native, _, err := p.activeClient(ctx, rc, s, token)
		if err != nil {
			return step, err
		}
		marker, err := snapshotMarker(ctx, rc, native, b, request)
		if err != nil {
			return step, err
		}
		var policies struct {
			Data *struct {
				Keys []string `json:"keys"`
			} `json:"data"`
		}
		if err = native.Do(ctx, "LIST", "sys/policies/acl", nil, &policies); err != nil {
			return step, err
		}
		if policies.Data == nil || policies.Data.Keys == nil {
			return step, invalid("OpenBao returned no restore policy inventory")
		}
		present := false
		for _, name := range policies.Data.Keys {
			if name == marker.Policy {
				present = true
				break
			}
		}
		if present {
			// The marker survived: import has not rewound this barrier state.
			if err = p.importSnapshot(ctx, rc, s, b, force); err != nil {
				return step, err
			}
		}
		return "unseal", nil
	case "unseal":
		material, err := readUnseal(ctx, rc)
		if err != nil {
			return step, err
		}
		for _, m := range ordered(s) {
			if busy, err := p.refreshSnapshotSeal(ctx, rc, s, request, m, false); busy || err != nil {
				return step, err
			}
			native, e := p.nativeClient(ctx, rc, m.Name, "")
			if e != nil {
				return step, e
			}
			status, e := native.SealStatus(ctx)
			if e != nil {
				return step, e
			}
			if status.Sealed {
				if material.Recovery {
					return step, fmt.Errorf("%w: waiting for OpenBao auto-unseal after restore", engine.ErrUnavailable)
				}
				status, e = native.UnsealWith(ctx, material.Shares[:material.Threshold])
				if e != nil {
					if force && (errors.Is(e, baoapi.ErrInsufficientShares) || baoapi.IsStatus(e, http.StatusBadRequest)) {
						if busy, refreshErr := p.refreshSnapshotSeal(ctx, rc, s, request, m, true); busy || refreshErr != nil {
							return step, refreshErr
						}
					}
					return step, e
				}
				if status.Sealed {
					return step, fmt.Errorf("%w: OpenBao member remains sealed after restore", engine.ErrUnavailable)
				}
				return step, nil // Disrupt/recover one member per invocation.
			}
		}
		return "management", nil
	case "management":
		if _, err := p.ensureManagementToken(ctx, rc, s); err != nil {
			return step, err
		}
		raw, err := secret(ctx, rc, RestoreUnsealSecret(request)+"/marker")
		if err == nil {
			var marker snapshotRestoreMarker
			if json.Unmarshal(raw, &marker) != nil || !strings.HasPrefix(marker.Policy, "karakoram-restore-") {
				return step, invalid("invalid OpenBao restore marker")
			}
			token, err := secret(ctx, rc, SecretManagementToken)
			if err != nil {
				return step, err
			}
			native, _, err := p.activeClient(ctx, rc, s, string(token))
			if err != nil {
				return step, err
			}
			if err = native.Do(ctx, http.MethodDelete, "sys/policies/acl/"+marker.Policy, nil, nil); err != nil && !baoapi.IsStatus(err, http.StatusNotFound) {
				return step, err
			}
		} else if !missing(err) {
			return step, err
		}
		if err = rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), RestoreUnsealSecret(request)+"/marker"); err != nil && !missing(err) {
			return step, err
		}
		if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), RestoreUnsealSecret(request)); err != nil && !missing(err) {
			return step, err
		}
		if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), RestoreUnsealSecret(request)+"/previous"); err != nil && !missing(err) {
			return step, err
		}
		if err := rc.Secrets.DeleteClusterSecret(ctx, rc.Cluster.Ref(), RestoreUnsealSecret(request)+"/seal-refresh"); err != nil && !missing(err) {
			return step, err
		}
		return "complete", nil
	default:
		return step, errors.New("unknown snapshot restore step")
	}
}

func (p *Provider) restoreOp(ctx context.Context, rc *engine.Context, op *domain.Operation) (engine.OpResult, error) {
	cp, err := snapshotLoadCheckpoint(op)
	if err != nil {
		return engine.OpResult{}, err
	}
	if cp.Step == "done" {
		return snapshotResult(cp), nil
	}
	if p.postgresStorage(rc.Cluster) {
		return snapshotResult(cp), invalid("PostgreSQL-storage OpenBao clusters do not support raft-snapshot restore")
	}
	name, err := snapshotPayload(op, "restore")
	if err != nil {
		return snapshotResult(cp), err
	}
	r, err := rc.Store.GetRestore(ctx, rc.Cluster.Ref(), name)
	if err != nil {
		return snapshotResult(cp), err
	}
	if r.Source.PITRTime != nil {
		return snapshotResult(cp), invalid("OpenBao does not support PITR")
	}
	source := domain.ClusterRef{Namespace: r.SourceNamespace, Name: r.Source.SourceCluster}
	if source.Namespace == "" {
		source.Namespace = rc.Cluster.Ref().Namespace
	}
	if source.Name == "" {
		source.Name = rc.Cluster.Name
	}
	b, err := snapshotSource(ctx, rc, source, r.Source.Backup)
	if err != nil {
		return snapshotResult(cp), err
	}
	s, err := loadState(rc.Cluster)
	if err != nil {
		return snapshotResult(cp), err
	}
	switch cp.Step {
	case "":
		stamp := now(rc)
		r.StartedAt = &stamp
		r.Phase = domain.PhaseRunning
		r.Error = ""
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return snapshotResult(cp), err
		}
		if err = snapshotEvent(ctx, rc, op, "RestoreStarted", fmt.Sprintf("OpenBao snapshot restore from %s/%s into %s/%s started", source.Namespace, source.Name, rc.Cluster.Ref().Namespace, rc.Cluster.Name)); err != nil {
			return snapshotResult(cp), err
		}
		cp.Step = "prepare"
	case "complete":
		stamp := now(rc)
		r.CompletedAt = &stamp
		r.Phase = domain.PhaseSucceeded
		r.Error = ""
		if err = rc.Store.UpdateRestore(ctx, r); err != nil {
			return snapshotResult(cp), err
		}
		if err = snapshotEvent(ctx, rc, op, "RestoreCompleted", fmt.Sprintf("OpenBao snapshot restored from %s/%s into %s/%s", source.Namespace, source.Name, rc.Cluster.Ref().Namespace, rc.Cluster.Name)); err != nil {
			return snapshotResult(cp), err
		}
		cp.Step = "done"
	default:
		cp.Step, err = p.snapshotRestoreStep(ctx, rc, s, b, source, name, cp.Step)
	}
	return snapshotResult(cp), err
}

// InitialDataSourceApplied reports the durable snapshot clone completion receipt.
// Healthy voters and an observed generation do not establish snapshot import.
func (*Provider) InitialDataSourceApplied(c *domain.Cluster) (bool, error) {
	s, err := loadState(c)
	if err != nil {
		return false, err
	}
	return s.CloneRestoreStep == "done", nil
}

func (p *Provider) restoreDataSource(ctx context.Context, rc *engine.Context, s *internalState) (bool, error) {
	d := rc.Cluster.Spec.DataSource
	if d == nil || s.CloneRestoreStep == "done" {
		return false, nil
	}
	if p.postgresStorage(rc.Cluster) || d.PITRTime != nil {
		return true, invalid("OpenBao dataSource requires a Raft snapshot and does not support PITR")
	}
	source := domain.ClusterRef{Namespace: d.Namespace, Name: d.Cluster}
	if source.Namespace == "" {
		source.Namespace = rc.Cluster.Ref().Namespace
	}
	b, err := snapshotSource(ctx, rc, source, d.Backup)
	if err != nil {
		return true, err
	}
	step, err := p.snapshotRestoreStep(ctx, rc, s, b, source, "clone", s.CloneRestoreStep)
	if err != nil {
		return true, err
	}
	s.CloneRestoreStep = step
	if step == "complete" {
		if err = snapshotEvent(ctx, rc, &domain.Operation{}, "CloneCompleted", fmt.Sprintf("OpenBao snapshot cloned from %s/%s into %s/%s", source.Namespace, source.Name, rc.Cluster.Ref().Namespace, rc.Cluster.Name)); err != nil {
			return true, err
		}
		s.CloneRestoreStep = "done"
	}
	return true, s.save(rc.Cluster)
}
