// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func testStore(t *testing.T) (*Store, string) {
	t.Helper()
	key, _ := secrets.New(strings.Repeat("m", 32))
	path := filepath.Join(t.TempDir(), "state.db")
	s, err := Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}
func TestClusterLifecycle(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c := domain.Cluster{Name: "demo", Spec: domain.ClusterSpec{Provider: "psmdb"}}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	if err := s.CreateCluster(ctx, &c); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflict %v", err)
	}
	if err := s.UpdateClusterSpec(ctx, domain.ParseClusterKey(c.Name), c.Spec); err != nil {
		t.Fatal(err)
	}
	got, _ := s.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if got.Generation != 1 {
		t.Fatal(got.Generation)
	}
	c.Spec.Version = "8.0"
	if err := s.UpdateClusterSpec(ctx, domain.ParseClusterKey(c.Name), c.Spec); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if got.Generation != 2 {
		t.Fatal(got.Generation)
	}
	c.Internal = json.RawMessage(`{"step":2}`)
	c.Status.Phase = domain.PhaseReady
	c.Status.ObservedGeneration = 1
	if err := s.UpdateClusterState(ctx, &c); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if got.Generation != 2 || got.Status.ObservedGeneration != 1 || got.Spec.Version != "8.0" || string(got.Internal) != `{"step":2}` {
		t.Fatalf("state %+v", got)
	}
	if err := s.MarkClusterDeleting(ctx, domain.ParseClusterKey(c.Name), domain.DeleteOptions{KeepVolumes: true}); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if !got.Deleting || !got.DeleteOptions.KeepVolumes {
		t.Fatal("deletion flags")
	}
	if err := s.SuspendCluster(ctx, got); err != nil {
		t.Fatal(err)
	}
	if err := s.ResumeCluster(ctx, domain.ParseClusterKey(c.Name)); err != nil {
		t.Fatal(err)
	}
	got, _ = s.GetCluster(ctx, domain.ParseClusterKey(c.Name))
	if got.Generation != 3 || got.Status.Phase != domain.PhaseCreating {
		t.Fatal("resume")
	}
	if err := s.DeleteCluster(ctx, domain.ParseClusterKey(c.Name)); err != nil {
		t.Fatal(err)
	}
	if _, err := s.GetCluster(ctx, domain.ParseClusterKey(c.Name)); !errors.Is(err, ErrNotFound) {
		t.Fatal(err)
	}
}
func TestResourcesAndSealedStorage(t *testing.T) {
	s, path := testStore(t)
	ctx := context.Background()
	storage := domain.BackupStorage{Name: "archive", Credentials: domain.StorageCredentials{AccessKeyID: "unique-access-key", SecretAccessKey: "unique-secret-value"}}
	if err := s.CreateBackupStorage(ctx, &storage); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetBackupStorage(ctx, domain.DefaultNamespace, storage.Name)
	if err != nil || got.Credentials != storage.Credentials {
		t.Fatalf("storage %+v %v", got, err)
	}
	storages, _ := s.ListBackupStorages(ctx, "")
	if storages[0].Credentials.SecretAccessKey != "" {
		t.Fatal("list exposed credentials")
	}
	storage.Bucket = "new"
	if err = s.UpdateBackupStorage(ctx, &storage); err != nil {
		t.Fatal(err)
	}
	if err = s.SetClusterSecret(ctx, domain.ParseClusterKey("demo"), "key", []byte("unique-cluster-key")); err != nil {
		t.Fatal(err)
	}
	plain, err := s.GetClusterSecret(ctx, domain.ParseClusterKey("demo"), "key")
	if err != nil || string(plain) != "unique-cluster-key" {
		t.Fatal(err)
	}
	agent := domain.AgentToken{Cluster: "demo", Member: "demom0", Token: "unique-agent-secret"}
	if err = s.SetAgentToken(ctx, &agent); err != nil {
		t.Fatal(err)
	}
	ag, err := s.FindAgentTokenByHash(ctx, secrets.Hash(agent.Token))
	if err != nil || ag.Token != agent.Token {
		t.Fatal(err)
	}
	for _, file := range []string{path, path + "-wal"} {
		data, _ := os.ReadFile(file)
		for _, secret := range []string{"unique-secret-value", "unique-cluster-key", "unique-agent-secret"} {
			if bytes.Contains(data, []byte(secret)) {
				t.Fatalf("plaintext secret in %s", file)
			}
		}
	}
	b := domain.Backup{Name: "b1", Cluster: "demo"}
	if err = s.CreateBackup(ctx, &b); err != nil {
		t.Fatal(err)
	}
	b.Phase = domain.PhaseSucceeded
	if err = s.UpdateBackup(ctx, &b); err != nil {
		t.Fatal(err)
	}
	bg, _ := s.GetBackup(ctx, domain.ParseClusterKey("demo"), "b1")
	if bg.Phase != domain.PhaseSucceeded {
		t.Fatal(bg)
	}
	r := domain.Restore{Name: "r1", Cluster: "demo", Source: domain.RestoreSource{Backup: "b1"}}
	if err = s.CreateRestore(ctx, &r); err != nil {
		t.Fatal(err)
	}
	if _, err = s.GetRestore(ctx, domain.ParseClusterKey("demo"), "r1"); err != nil {
		t.Fatal(err)
	}
	v := domain.RetainedVolume{Cluster: "demo", Member: "demom0", Hostname: "demom0v"}
	if err = s.CreateRetainedVolume(ctx, &v); err != nil {
		t.Fatal(err)
	}
	volumes, _ := s.ListRetainedVolumes(ctx, domain.ParseClusterKey("demo"))
	if len(volumes) != 1 {
		t.Fatal(volumes)
	}
	if err = s.DeleteRetainedVolume(ctx, domain.ParseClusterKey("demo"), "demom0"); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteClusterSecret(ctx, domain.ParseClusterKey("demo"), "key"); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteAgentToken(ctx, domain.ParseClusterKey("demo"), "demom0"); err != nil {
		t.Fatal(err)
	}
}
func TestAccountsTokensAndSessions(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	a := domain.Account{Name: "admin", PasswordHash: "argon2hash", Roles: []string{"role:admin"}, MustChangePassword: true}
	if err := s.CreateAccount(ctx, &a); err != nil {
		t.Fatal(err)
	}
	ag, err := s.GetAccount(ctx, "admin")
	if err != nil || ag.PasswordHash != a.PasswordHash || !ag.MustChangePassword {
		t.Fatal(err)
	}
	ag.Disabled = true
	if err = s.UpdateAccount(ctx, ag); err != nil {
		t.Fatal(err)
	}
	tok := domain.ApiToken{ID: "token1", Owner: "admin", Hash: "tokenhash"}
	if err = s.CreateApiToken(ctx, &tok); err != nil {
		t.Fatal(err)
	}
	tg, err := s.FindApiTokenByHash(ctx, "tokenhash")
	if err != nil || tg.Owner != "admin" {
		t.Fatal(err)
	}
	if err = s.TouchApiToken(ctx, tok.ID, time.Now()); err != nil {
		t.Fatal(err)
	}
	tg, _ = s.GetApiToken(ctx, tok.ID)
	if tg.LastUsedAt == nil {
		t.Fatal("last used")
	}
	session := domain.Session{ID: "s1", Owner: "admin", RefreshHash: "old", ExpiresAt: time.Now().Add(time.Hour)}
	if err = s.CreateSession(ctx, &session); err != nil {
		t.Fatal(err)
	}
	if err = s.RotateSession(ctx, "s1", "old", "new", time.Now().Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if err = s.RotateSession(ctx, "s1", "old", "other", time.Now().Add(time.Hour)); !errors.Is(err, ErrNotFound) {
		t.Fatal("refresh replay", err)
	}
	if _, err = s.FindSessionByHash(ctx, "new"); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteSession(ctx, "s1"); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteApiToken(ctx, tok.ID); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteAccount(ctx, "admin"); err != nil {
		t.Fatal(err)
	}
}
func TestOperationsResumeAndAtomicBackup(t *testing.T) {
	s, path := testStore(t)
	ctx := context.Background()
	first := domain.Operation{ID: "first", Cluster: "demo", Kind: "backup"}
	second := domain.Operation{ID: "second", Cluster: "demo", Kind: "restart"}
	for _, op := range []*domain.Operation{&first, &second} {
		if err := s.EnqueueOperation(ctx, op); err != nil {
			t.Fatal(err)
		}
	}
	op, err := s.ClaimOperation(ctx, domain.ParseClusterKey("demo"))
	if err != nil || op.ID != "first" {
		t.Fatal(op, err)
	}
	op.Checkpoint = json.RawMessage(`{"step":"started"}`)
	if err = s.UpdateOperation(ctx, op); err != nil {
		t.Fatal(err)
	}
	key, _ := secrets.New(strings.Repeat("m", 32))
	other, err := Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	resumed, err := other.ClaimOperation(ctx, domain.ParseClusterKey("demo"))
	if err != nil || resumed.ID != "first" || resumed.Attempts != 2 || string(resumed.Checkpoint) != string(op.Checkpoint) {
		t.Fatal(resumed, err)
	}
	resumed.Phase = domain.PhaseSucceeded
	if err = other.UpdateOperation(ctx, resumed); err != nil {
		t.Fatal(err)
	}
	op, err = s.ClaimOperation(ctx, domain.ParseClusterKey("demo"))
	if err != nil || op.ID != "second" {
		t.Fatal(op, err)
	}
	b := domain.Backup{Name: "rollback", Cluster: "demo"}
	duplicate := domain.Operation{ID: "first", Cluster: "demo"}
	if err = s.EnqueueBackup(ctx, &b, &duplicate); !errors.Is(err, ErrConflict) {
		t.Fatal(err)
	}
	if _, err = s.GetBackup(ctx, domain.ParseClusterKey("demo"), "rollback"); !errors.Is(err, ErrNotFound) {
		t.Fatal("transaction partial", err)
	}
	ops, err := s.ListOperations(ctx, domain.ParseClusterKey("demo"), domain.PhaseRunning)
	if err != nil || len(ops) != 1 {
		t.Fatal(ops, err)
	}
}
func TestEventsSettingsPoliciesMetricsAndConcurrency(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := s.AppendEvent(ctx, &domain.Event{Cluster: "demo", Type: "Normal", Message: "event"}); err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	events, err := s.ListEvents(ctx, EventFilter{Cluster: "demo", Limit: 10})
	if err != nil || len(events) != 10 {
		t.Fatal(len(events), err)
	}
	next, _ := s.ListEvents(ctx, EventFilter{SinceID: events[9].ID})
	if len(next) != 10 || next[0].ID <= events[9].ID {
		t.Fatal("event pagination")
	}
	if err = s.SetSetting(ctx, "oidc", json.RawMessage(`{"enabled":true}`)); err != nil {
		t.Fatal(err)
	}
	setting, _ := s.GetSetting(ctx, "oidc")
	if string(setting) != `{"enabled":true}` {
		t.Fatal(string(setting))
	}
	rules := []domain.PolicyRule{{PType: "p", Values: []string{"role:admin", "*", "*", "*"}}}
	if err = s.ReplacePolicyRules(ctx, rules); err != nil {
		t.Fatal(err)
	}
	got, _ := s.ListPolicyRules(ctx)
	if len(got) != 1 || got[0].Values[0] != "role:admin" {
		t.Fatal(got)
	}
	if err = s.ReplacePolicyRules(ctx, nil); err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	samples := []domain.MetricSample{{Cluster: "demo", Member: "m0", Metric: "cpu", Timestamp: now.Add(-time.Hour), Value: 1}, {Cluster: "demo", Member: "m0", Metric: "cpu", Timestamp: now, Value: 2}, {Cluster: "demo", Member: "m1", Metric: "cpu", Timestamp: now, Value: 3}}
	if err = s.InsertMetricSamples(ctx, samples); err != nil {
		t.Fatal(err)
	}
	series, err := s.QueryMetricSamples(ctx, MetricQuery{Cluster: "demo", Member: "m0", Metric: "cpu", Start: now.Add(-time.Minute), End: now.Add(time.Minute)})
	if err != nil || len(series) != 1 || series[0].Value != 2 {
		t.Fatal(series, err)
	}
	n, err := s.PruneMetricSamples(ctx, now.Add(-time.Minute))
	if err != nil || n != 1 {
		t.Fatal(n, err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err = s.ListClusters(cancelled, ""); !errors.Is(err, context.Canceled) {
		t.Fatal("context", err)
	}
	var journal string
	if err = s.reader.QueryRow("PRAGMA journal_mode").Scan(&journal); err != nil || journal != "wal" {
		t.Fatal(journal, err)
	}
	var fk int
	s.reader.QueryRow("PRAGMA foreign_keys").Scan(&fk)
	if fk != 1 {
		t.Fatal("foreign keys disabled")
	}
}

func TestNamespacePersistenceIsolation(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	now := time.Now().UTC()
	if err := s.CreateNamespace(ctx, &domain.Namespace{Name: "dev", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	for _, namespace := range []string{domain.DefaultNamespace, "dev"} {
		c := domain.Cluster{Namespace: namespace, Name: "orders", Spec: domain.ClusterSpec{Provider: "psmdb"}}
		if err := s.CreateClusterWithSecrets(ctx, &c, map[string][]byte{"password": []byte(namespace + "-secret")}); err != nil {
			t.Fatal(err)
		}
		storage := domain.BackupStorage{Namespace: namespace, Name: "archive", Credentials: domain.StorageCredentials{AccessKeyID: namespace, SecretAccessKey: namespace + "-storage-secret"}}
		if err := s.CreateBackupStorage(ctx, &storage); err != nil {
			t.Fatal(err)
		}
		backup := domain.Backup{Namespace: namespace, Cluster: c.Name, Name: "snapshot", Storage: storage.Name}
		op := domain.Operation{Namespace: namespace, Cluster: c.Name, ID: namespace + "-backup"}
		if err := s.EnqueueBackup(ctx, &backup, &op); err != nil {
			t.Fatal(err)
		}
		if err := s.SetAgentToken(ctx, &domain.AgentToken{Namespace: namespace, Cluster: c.Name, Member: "ordersm0", Token: namespace + "-member-token"}); err != nil {
			t.Fatal(err)
		}
		if err := s.AppendEvent(ctx, &domain.Event{Namespace: namespace, Cluster: c.Name, Type: "Normal"}); err != nil {
			t.Fatal(err)
		}
		if err := s.InsertMetricSamples(ctx, []domain.MetricSample{{Namespace: namespace, Cluster: c.Name, Member: "ordersm0", Metric: "cpu", Timestamp: now}}); err != nil {
			t.Fatal(err)
		}
		if err := s.PutOrphanedVolume(ctx, &domain.OrphanedVolume{Namespace: namespace, Cluster: c.Name, Hostname: "ordersm0v", ProcessPending: true, JanitorFailures: 2}); err != nil {
			t.Fatal(err)
		}
	}
	all, err := s.ListClusters(ctx, "")
	if err != nil || len(all) != 2 {
		t.Fatal(all, err)
	}
	for _, namespace := range []string{domain.DefaultNamespace, "dev"} {
		ref := domain.ClusterRef{Namespace: namespace, Name: "orders"}
		cluster, err := s.GetCluster(ctx, ref)
		if err != nil || cluster.Namespace != namespace {
			t.Fatal(cluster, err)
		}
		secret, err := s.GetClusterSecret(ctx, ref, "password")
		if err != nil || string(secret) != namespace+"-secret" {
			t.Fatal("wrong cluster secret", err)
		}
		storage, err := s.GetBackupStorage(ctx, namespace, "archive")
		if err != nil || storage.Credentials.AccessKeyID != namespace {
			t.Fatal(storage, err)
		}
		token, err := s.FindAgentTokenByHash(ctx, secrets.Hash(namespace+"-member-token"))
		if err != nil || token.Namespace != namespace || token.Cluster != "orders" {
			t.Fatal(token, err)
		}
		backups, err := s.ListBackups(ctx, domain.ClusterRef{Namespace: namespace})
		if err != nil || len(backups) != 1 || backups[0].Namespace != namespace {
			t.Fatal(backups, err)
		}
		events, err := s.ListEvents(ctx, EventFilter{Namespace: namespace})
		if err != nil || len(events) != 1 || events[0].Namespace != namespace {
			t.Fatal(events, err)
		}
		metrics, err := s.QueryMetricSamples(ctx, MetricQuery{Namespace: namespace, Cluster: "orders"})
		if err != nil || len(metrics) != 1 || metrics[0].Namespace != namespace {
			t.Fatal(metrics, err)
		}
		orphan, err := s.GetOrphanedVolume(ctx, namespace, "ordersm0v")
		if err != nil || orphan.Namespace != namespace || !orphan.ProcessPending || orphan.JanitorFailures != 2 {
			t.Fatal(orphan, err)
		}
	}
	if err := s.DeleteCluster(ctx, domain.ClusterRef{Namespace: "dev", Name: "orders"}); err != nil {
		t.Fatal(err)
	}
	op, err := s.GetOperation(ctx, "default-backup")
	if err != nil || op.Phase != domain.PhasePending {
		t.Fatal("deletion crossed namespace", op, err)
	}
	if _, err := s.GetCluster(ctx, domain.ClusterRef{Name: "orders"}); err != nil {
		t.Fatal("default cluster deleted", err)
	}
}
