// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors
package temporal

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/agentapi"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
)

type backupBindings struct {
	*fakeBindings
	backups, restores int
	keys              []string
	phase             string
	restorePhase      string
	shared            bool
	backupError       bool
}

func (b *backupBindings) Backup(_ context.Context, _, _, key string) (pgbinding.BackupRef, error) {
	b.backups++
	b.keys = append(b.keys, key)
	if b.backupError {
		return pgbinding.BackupRef{}, errors.New("secret-diagnostic")
	}
	return pgbinding.BackupRef{ID: "source-backup"}, nil
}

func (b *backupBindings) BackupStatus(context.Context, string, string) (pgbinding.State, error) {
	return pgbinding.State{Phase: b.phase, Error: "secret-diagnostic"}, nil
}

func (b *backupBindings) Restore(_ context.Context, _, _, key string, ack bool) (pgbinding.OperationRef, error) {
	b.restores++
	b.keys = append(b.keys, key)
	if b.shared && !ack {
		return pgbinding.OperationRef{}, pgbinding.ErrSharedSource
	}
	return pgbinding.OperationRef{ID: "source-restore"}, nil
}

func (b *backupBindings) OperationStatus(context.Context, string, string) (pgbinding.State, error) {
	return pgbinding.State{Phase: b.restorePhase, Error: "secret-diagnostic"}, nil
}

type operationInventory struct {
	engine.ClusterStore
	c *domain.Cluster
	b *domain.Backup
	r *domain.Restore
}

func (i *operationInventory) GetCluster(context.Context, domain.ClusterRef) (*domain.Cluster, error) {
	return i.c, nil
}

func (i *operationInventory) GetBackup(context.Context, domain.ClusterRef, string) (*domain.Backup, error) {
	return i.b, nil
}

func (i *operationInventory) UpdateBackup(_ context.Context, b *domain.Backup) error {
	i.b = b
	return nil
}

func (i *operationInventory) DeleteBackup(context.Context, domain.ClusterRef, string) error {
	i.b = nil
	return nil
}

func (i *operationInventory) GetRestore(context.Context, domain.ClusterRef, string) (*domain.Restore, error) {
	return i.r, nil
}

func (i *operationInventory) UpdateRestore(_ context.Context, r *domain.Restore) error {
	i.r = r
	return nil
}

func stepOperation(t *testing.T, p *Provider, rc *engine.Context, op *domain.Operation) engine.OpResult {
	t.Helper()
	out, e := p.RunOperation(t.Context(), rc, op)
	if e != nil {
		t.Fatal(e)
	}
	op.Checkpoint = out.Checkpoint
	rc.Clock.(*testClock).now = rc.Clock.Now().Add(time.Minute)
	return out
}

func finishOperation(t *testing.T, p *Provider, rc *engine.Context, op *domain.Operation) {
	t.Helper()
	for range 150 {
		if stepOperation(t, p, rc, op).Done {
			return
		}
	}
	t.Fatal("operation did not complete")
}

func TestAccessCertificateValidationAndJournal(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	for _, cn := range []string{"", "bad.cn", "-bad", "bad-", strings.Repeat("x", 65)} {
		if _, e := p.IssueClientCertificate(t.Context(), rc, cn, time.Hour); !errors.Is(e, engine.ErrInvalid) {
			t.Fatal(cn, e)
		}
	}
	for _, ttl := range []time.Duration{0, time.Hour - time.Second, 398 * 24 * time.Hour} {
		if _, e := p.IssueClientCertificate(t.Context(), rc, "worker", ttl); !errors.Is(e, engine.ErrInvalid) {
			t.Fatal(ttl, e)
		}
	}
	out, e := p.IssueClientCertificate(t.Context(), rc, "worker-1", time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	if !out.CurrentCA {
		t.Fatal("new certificate is not current")
	}
	block, _ := pem.Decode([]byte(out.CertificatePEM))
	leaf, e := x509.ParseCertificate(block.Bytes)
	if e != nil {
		t.Fatal(e)
	}
	if !reflect.DeepEqual(leaf.ExtKeyUsage, []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}) {
		t.Fatal(leaf.ExtKeyUsage)
	}
	raw := rc.Secrets.(*memorySecrets).values[certificateJournalKey]
	if strings.Contains(string(raw), "PRIVATE") || strings.Contains(string(raw), out.PrivateKeyPEM) {
		t.Fatal("private key persisted")
	}
	if strings.Contains(string(raw), "currentCa") {
		t.Fatal("derived current CA flag persisted")
	}
	journal, e := p.ListClientCertificates(t.Context(), rc)
	if e != nil || len(journal) != 1 || journal[0] != out.ClientCertificate {
		t.Fatal(journal, e)
	}
	if e = p.SetUIOIDCClientSecret(t.Context(), rc, ""); !errors.Is(e, engine.ErrInvalid) {
		t.Fatal(e)
	}
	if e = p.SetUIOIDCClientSecret(t.Context(), rc, strings.Repeat("x", 4097)); !errors.Is(e, engine.ErrInvalid) {
		t.Fatal(e)
	}
	if e = p.SetUIOIDCClientSecret(t.Context(), rc, "private-oidc-secret"); e != nil {
		t.Fatal(e)
	}
	converge(t, p, rc)
	status, e := p.Status(t.Context(), rc)
	if e != nil {
		t.Fatal(e)
	}
	public, _ := json.Marshal(status)
	if strings.Contains(string(rc.Cluster.Internal)+string(public), "private-oidc-secret") {
		t.Fatal("OIDC secret leaked")
	}
}

func TestNamespaceRefusalsAndErrorMapping(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	rc.Cluster.Spec.EngineOptions = json.RawMessage(`{
		"persistence":{"postgres":{"cluster":"pg1"}},
		"namespaces":[{"name":"orders","retention":"72h"}]
	}`)
	for _, name := range []string{"default", "temporal-system", "orders"} {
		if e := p.DeleteNamespace(t.Context(), rc, name); !errors.Is(e, engine.ErrConflict) {
			t.Fatal(name, e)
		}
	}
	for code, want := range map[int]error{404: engine.ErrNotFound, 409: engine.ErrConflict, 422: engine.ErrInvalid, 500: engine.ErrUnavailable} {
		if e := namespaceError(&agentapi.HTTPError{StatusCode: code, Message: "secret"}); !errors.Is(e, want) || strings.Contains(e.Error(), "secret") {
			t.Fatal(e)
		}
	}
}

func TestRestartOrderAndResume(t *testing.T) {
	p, rc, _, agents := fixture(t, "split")
	converge(t, p, rc)
	op := &domain.Operation{ID: "restart-1", Kind: domain.OperationRestart}
	out := stepOperation(t, p, rc, op)
	var cp operationCheckpoint
	json.Unmarshal(out.Checkpoint, &cp)
	want := []string{"history", "matching", "frontend", "worker", "ui"}
	if !reflect.DeepEqual(cp.Order, want) {
		t.Fatal(cp.Order)
	}
	stepOperation(t, p, rc, op)
	s, _ := loadState(rc.Cluster)
	version := s.Members["history"].ConfigVersion
	agents.unready = "history"
	out = stepOperation(t, p, rc, op)
	json.Unmarshal(out.Checkpoint, &cp)
	if cp.Index != 0 || !cp.Waiting {
		t.Fatal(cp)
	}
	s, _ = loadState(rc.Cluster)
	if s.Members["history"].ConfigVersion != version {
		t.Fatal("resume republished restart")
	}
	agents.unready = ""
	finishOperation(t, p, rc, op)
}

func TestRotationStagesAndJournalInvalidation(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	converge(t, p, rc)
	issued, e := p.IssueClientCertificate(t.Context(), rc, "sdk", time.Hour)
	if e != nil {
		t.Fatal(e)
	}
	if e = p.stageRotation(t.Context(), rc, "rotate-1"); e != nil {
		t.Fatal(e)
	}
	raw := string(rc.Secrets.(*memorySecrets).values[rotationStageKey])
	if e = p.stageRotation(t.Context(), rc, "rotate-1"); e != nil {
		t.Fatal(e)
	}
	if string(rc.Secrets.(*memorySecrets).values[rotationStageKey]) != raw {
		t.Fatal("retry minted another CA")
	}
	journal, e := p.ListClientCertificates(t.Context(), rc)
	if e != nil || len(journal) != 1 || !journal[0].CurrentCA {
		t.Fatal("staged trust must retain the old signing CA", journal, e)
	}
	op := &domain.Operation{ID: "rotate-1", Kind: domain.OperationRotateTLS}
	for range 150 {
		stepOperation(t, p, rc, op)
		var cp operationCheckpoint
		if e = json.Unmarshal(op.Checkpoint, &cp); e != nil {
			t.Fatal(e)
		}
		if cp.Stage == "issue" {
			stepOperation(t, p, rc, op)
			break
		}
	}
	journal, e = p.ListClientCertificates(t.Context(), rc)
	if e != nil || len(journal) != 1 || journal[0].CurrentCA {
		t.Fatal("signing switch must invalidate old metadata without new issuance", journal, e)
	}
	finishOperation(t, p, rc, op)
	ca, e := ensureCA(t.Context(), rc)
	if e != nil {
		t.Fatal(e)
	}
	journal, e = p.ListClientCertificates(t.Context(), rc)
	if e != nil || journal[0].CurrentCA || journal[0].CAFingerprintSHA256 != issued.CAFingerprintSHA256 || journal[0].CAFingerprintSHA256 == fingerprint(ca.Cert.Raw) {
		t.Fatal(journal, e)
	}
	if _, ok := rc.Secrets.(*memorySecrets).values[rotationStageKey]; ok {
		t.Fatal("staging key remains")
	}
	if _, ok := rc.Secrets.(*memorySecrets).values[rotationTrustKey]; ok {
		t.Fatal("trust bundle remains")
	}
	replacement, e := p.IssueClientCertificate(t.Context(), rc, "replacement", time.Hour)
	if e != nil || !replacement.CurrentCA {
		t.Fatal(replacement, e)
	}
	journal, e = p.ListClientCertificates(t.Context(), rc)
	if e != nil || len(journal) != 2 || journal[0].CurrentCA || !journal[1].CurrentCA {
		t.Fatal(journal, e)
	}
	if strings.Contains(string(rc.Secrets.(*memorySecrets).values[certificateJournalKey]), "currentCa") {
		t.Fatal("derived flag persisted when appending to journal")
	}
	converge(t, p, rc)
}

func TestBackupResumeRestoreAndSharedRefusal(t *testing.T) {
	for _, phase := range []string{pgbinding.PhaseCompleted, pgbinding.PhaseFailed} {
		t.Run(phase, func(t *testing.T) {
			p, rc, bindings, _ := fixture(t, "combined")
			converge(t, p, rc)
			b := &backupBindings{fakeBindings: bindings, phase: phase, restorePhase: pgbinding.PhaseCompleted}
			p.options.Bindings = b
			inventory := &operationInventory{
				c: rc.Cluster, b: &domain.Backup{Name: "snapshot", Type: "full"},
				r: &domain.Restore{Name: "restore", Source: domain.RestoreSource{Backup: "snapshot"}},
			}
			rc.Store = inventory
			op := &domain.Operation{ID: "backup-1", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snapshot"}`)}
			stepOperation(t, p, rc, op)
			stepOperation(t, p, rc, op)
			if b.backups != 1 || b.keys[0] != op.ID {
				t.Fatal(b)
			}
			out, e := p.RunOperation(t.Context(), rc, op)
			if phase == pgbinding.PhaseFailed {
				if !errors.Is(e, engine.ErrInvalid) || strings.Contains(e.Error(), "secret") {
					t.Fatal(e)
				}
				return
			}
			if e != nil || !out.Done {
				t.Fatal(out, e)
			}
			op.Checkpoint = out.Checkpoint
			stepOperation(t, p, rc, op)
			if b.backups != 1 {
				t.Fatal("second backup on resume")
			}
			restore := &domain.Operation{ID: "restore-1", Kind: domain.OperationRestore, Payload: json.RawMessage(`{"restore":"restore"}`)}
			_, e = p.RunOperation(t.Context(), rc, restore)
			if !errors.Is(e, pgbinding.ErrSharedSource) || b.restores != 0 {
				t.Fatal(e, b.restores)
			}
			restore.Payload = json.RawMessage(`{"restore":"restore","acknowledgeSharedSource":true}`)
			finishOperation(t, p, rc, restore)
			if b.restores != 1 || inventory.r.Phase != domain.PhaseSucceeded {
				t.Fatal(b.restores, inventory.r)
			}
			remove := &domain.Operation{ID: "delete", Kind: domain.OperationDeleteBackup, Payload: json.RawMessage(`{"backup":"snapshot"}`)}
			finishOperation(t, p, rc, remove)
			s, _ := loadState(rc.Cluster)
			if len(s.Backups) != 0 || inventory.b != nil {
				t.Fatal("inventory remains")
			}
		})
	}
}

func TestRestoreCompatibility(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	_ = p
	o, _ := optionsFor(rc.Cluster)
	m := backupManifest{
		Version: rc.Cluster.Spec.Version, HistoryShards: o.HistoryShards, ClusterName: rc.Cluster.Name,
		Databases: []string{o.Persistence.Postgres.Database, o.Persistence.Postgres.VisibilityDatabase},
		Source:    domain.ClusterRef{Namespace: o.Persistence.Postgres.Namespace, Name: o.Persistence.Postgres.Cluster},
		Complete:  true, SourceID: "backup",
	}
	if e := compatibleManifest(rc.Cluster, o, m); e != nil {
		t.Fatal(e)
	}
	for _, mutate := range []func(*backupManifest){
		func(m *backupManifest) { m.HistoryShards++ },
		func(m *backupManifest) { m.ClusterName = "other" },
		func(m *backupManifest) { m.Version = "1.32.1" },
		func(m *backupManifest) { m.Version = "1.29.1" },
		func(m *backupManifest) { m.Complete = false },
	} {
		copy := m
		mutate(&copy)
		if e := compatibleManifest(rc.Cluster, o, copy); !errors.Is(e, engine.ErrInvalid) {
			t.Fatal(copy, e)
		}
	}
}

func TestBackupAmbiguousRequestUsesOperationID(t *testing.T) {
	p, rc, bindings, _ := fixture(t, "combined")
	converge(t, p, rc)
	op := &domain.Operation{ID: "retry-backup", Kind: domain.OperationBackup, Payload: json.RawMessage(`{"backup":"snapshot"}`)}
	if _, e := p.RunOperation(t.Context(), rc, op); !errors.Is(e, engine.ErrInvalid) || !strings.Contains(e.Error(), "does not support backups") {
		t.Fatal(e)
	}
	b := &backupBindings{fakeBindings: bindings, phase: pgbinding.PhaseCompleted, backupError: true}
	p.options.Bindings = b
	rc.Store = &operationInventory{c: rc.Cluster, b: &domain.Backup{Name: "snapshot", Type: "diff"}}
	stepOperation(t, p, rc, op)
	if _, e := p.RunOperation(t.Context(), rc, op); !errors.Is(e, engine.ErrUnavailable) || strings.Contains(e.Error(), "secret") {
		t.Fatal(e)
	}
	b.backupError = false
	finishOperation(t, p, rc, op)
	if !reflect.DeepEqual(b.keys, []string{op.ID, op.ID}) {
		t.Fatal(b.keys)
	}
}

func TestRotationCompletionCheckpointReplay(t *testing.T) {
	p, rc, _, _ := fixture(t, "combined")
	converge(t, p, rc)
	op := &domain.Operation{ID: "rotate-replay", Kind: domain.OperationRotateTLS}
	finishOperation(t, p, rc, op)
	// Simulate a crash after deleting sealed staging but before persisting Done.
	op.Checkpoint, _ = json.Marshal(operationCheckpoint{Stage: "prune", Order: []string{"server", "ui"}, Index: 2})
	if out, e := p.RunOperation(t.Context(), rc, op); e != nil || !out.Done {
		t.Fatal(out, e)
	}
}

func TestRestoreBindingFailureAndSharedErrors(t *testing.T) {
	for _, shared := range []bool{false, true} {
		t.Run(map[bool]string{false: "failed", true: "shared"}[shared], func(t *testing.T) {
			p, rc, bindings, _ := fixture(t, "combined")
			converge(t, p, rc)
			b := &backupBindings{fakeBindings: bindings, restorePhase: pgbinding.PhaseFailed, shared: shared}
			p.options.Bindings = b
			o, _ := optionsFor(rc.Cluster)
			s, _ := loadState(rc.Cluster)
			s.Backups = map[string]backupManifest{"snapshot": {
				Version: rc.Cluster.Spec.Version, HistoryShards: o.HistoryShards, ClusterName: rc.Cluster.Name,
				Databases: []string{o.Persistence.Postgres.Database, o.Persistence.Postgres.VisibilityDatabase},
				Source:    domain.ClusterRef{Namespace: o.Persistence.Postgres.Namespace, Name: o.Persistence.Postgres.Cluster},
				Complete:  true, SourceID: "backup",
			}}
			if e := saveState(rc.Cluster, s); e != nil {
				t.Fatal(e)
			}
			rc.Store = &operationInventory{c: rc.Cluster, r: &domain.Restore{Name: "restore", Source: domain.RestoreSource{Backup: "snapshot"}}}
			stage := "wait-restore"
			want := engine.ErrInvalid
			if shared {
				stage = "restore-source"
				want = pgbinding.ErrSharedSource
			}
			cp, _ := json.Marshal(operationCheckpoint{Stage: stage, SourceID: "source-op"})
			op := &domain.Operation{
				ID: "restore-binding", Kind: domain.OperationRestore,
				Payload: json.RawMessage(`{"restore":"restore"}`), Checkpoint: cp,
			}
			var err error
			for range 40 {
				out, e := p.RunOperation(t.Context(), rc, op)
				op.Checkpoint = out.Checkpoint
				if e != nil {
					err = e
					break
				}
			}
			if !errors.Is(err, want) || strings.Contains(err.Error(), "secret") {
				t.Fatal(err)
			}
		})
	}
}
