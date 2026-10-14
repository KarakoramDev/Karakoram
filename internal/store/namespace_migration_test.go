// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"encoding/json"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

type namespaceMigrationSecret struct {
	cluster, key, plaintext string
}

type namespaceMigrationSealedRecord struct {
	kind, scope, key, ciphertext, hash string
}

type namespaceMigrationFixture struct {
	clusters []domain.Cluster
	secrets  []namespaceMigrationSecret
	agents   []domain.AgentToken
	storage  domain.BackupStorage
	backup   domain.Backup
	restore  domain.Restore
	ops      []domain.Operation
	events   []domain.Event
	metrics  []domain.MetricSample
	orphan   domain.OrphanedVolume
	policies []domain.PolicyRule
	sealed   []namespaceMigrationSealedRecord
}

func TestNamespaceMigrationPreservesLegacyData(t *testing.T) {
	ctx := context.Background()
	sealer, err := secrets.New(strings.Repeat("m", 32))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy.db")
	fixture := seedNamespaceMigrationDatabase(t, path, sealer, true)

	s, err := Open(path, sealer)
	if err != nil {
		t.Fatalf("open legacy database: %v", err)
	}
	t.Cleanup(func() {
		if s != nil {
			if err := s.Close(); err != nil {
				t.Errorf("close migrated database: %v", err)
			}
		}
	})
	t.Run("first_open", func(t *testing.T) {
		assertNamespaceMigrationFixture(t, s, fixture)
	})

	// Creating a colliding name must not replace any legacy state or ciphertext.
	if err := s.CreateNamespace(ctx, &domain.Namespace{Name: "dev", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	other := domain.Cluster{Namespace: "dev", Name: "orders", Spec: domain.ClusterSpec{Provider: "psmdb", Version: "8.0"}, Internal: json.RawMessage(`{}`)}
	if err := s.CreateCluster(ctx, &other); err != nil {
		t.Fatalf("create same cluster name in another namespace: %v", err)
	}
	if err := s.SetClusterSecret(ctx, other.Ref(), "root-password", []byte("synthetic-dev-password")); err != nil {
		t.Fatalf("set other namespace secret: %v", err)
	}
	got, err := s.GetCluster(ctx, other.Ref())
	assertNamespaceMigrationValue(t, "other namespace cluster", got, &other, err)
	plain, err := s.GetClusterSecret(ctx, other.Ref(), "root-password")
	assertNamespaceMigrationValue(t, "other namespace secret", string(plain), "synthetic-dev-password", err)
	all, err := s.ListClusters(ctx, "")
	if err != nil || len(all) != 3 {
		t.Fatalf("all namespaces cluster count = %d, want 3; error: %v", len(all), err)
	}
	t.Run("default_unchanged", func(t *testing.T) {
		assertNamespaceMigrationFixture(t, s, fixture)
	})

	if err := s.Close(); err != nil {
		t.Fatalf("close before reopening: %v", err)
	}
	s = nil
	s, err = Open(path, sealer)
	if err != nil {
		t.Fatalf("reopen migrated database: %v", err)
	}
	t.Run("second_open", func(t *testing.T) {
		assertNamespaceMigrationFixture(t, s, fixture)
		got, err := s.GetCluster(ctx, other.Ref())
		assertNamespaceMigrationValue(t, "persisted other namespace cluster", got, &other, err)
		plain, err := s.GetClusterSecret(ctx, other.Ref(), "root-password")
		assertNamespaceMigrationValue(t, "persisted other namespace secret", string(plain), "synthetic-dev-password", err)
		all, err := s.ListClusters(ctx, "")
		if err != nil || len(all) != 3 {
			t.Fatalf("reopened cluster count = %d, want 3; error: %v", len(all), err)
		}
	})
}

func TestNamespaceMigrationFromPreActorSchema(t *testing.T) {
	key, err := secrets.New(strings.Repeat("m", 32))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "legacy.db")
	fixture := seedNamespaceMigrationDatabase(t, path, key, false)
	s, err := Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	assertNamespaceMigrationFixture(t, s, fixture)
}

func seedNamespaceMigrationDatabase(t *testing.T, path string, sealer *secrets.Sealer, upstream bool) namespaceMigrationFixture {
	t.Helper()
	ctx := context.Background()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	exec := func(query string, args ...any) sql.Result {
		t.Helper()
		result, err := db.ExecContext(ctx, query, args...)
		if err != nil {
			t.Fatalf("seed legacy database (%s): %v", query, err)
		}
		return result
	}
	exec("CREATE TABLE IF NOT EXISTS schema_migrations (name TEXT PRIMARY KEY)")
	// Seed the published pre-namespace schema; the older schema also exercises
	// sequential upgrades through both actor attribution and namespaces.
	names := []string{"001.sql", "002.sql"}
	if upstream {
		names = append(names, "003.sql")
	}
	for _, name := range names {
		script, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = tx.ExecContext(ctx, string(script)); err == nil {
			_, err = tx.ExecContext(ctx, "INSERT INTO schema_migrations(name) VALUES(?)", name)
		}
		if err != nil {
			tx.Rollback()
			t.Fatalf("seed migration %s: %v", name, err)
		}
		if err := tx.Commit(); err != nil {
			t.Fatal(err)
		}
	}
	assertNamespaceMigrationNames(t, db, names)

	at := time.Date(2026, time.January, 2, 3, 4, 5, 123456789, time.UTC)
	f := namespaceMigrationFixture{
		clusters: []domain.Cluster{
			{Name: "ledger", Spec: domain.ClusterSpec{Provider: "psmdb", Version: "8.0", Topology: domain.Topology{Type: "replicaSet"}}, Status: domain.ClusterStatus{Phase: domain.PhaseSuspended, ObservedGeneration: 5}, Internal: json.RawMessage(`{"step":"retained","member":"ledgerm0"}`), Generation: 5, Deleting: true, DeleteOptions: domain.DeleteOptions{KeepVolumes: true, KeepBackups: true}, CreatedAt: at, UpdatedAt: at.Add(time.Hour)},
			{Name: "orders", Spec: domain.ClusterSpec{Provider: "psmdb", Version: "7.0", Backup: domain.BackupConfig{Storage: "archive"}}, Status: domain.ClusterStatus{Phase: domain.PhaseReady, ObservedGeneration: 3, Message: "Ready"}, Internal: json.RawMessage(`{"step":"ready","member":"ordersm0"}`), Generation: 3, CreatedAt: at, UpdatedAt: at.Add(time.Minute)},
		},
		secrets: []namespaceMigrationSecret{
			{"orders", "root-password", "synthetic-orders-password"},
			{"orders", "keyfile", "synthetic-orders-keyfile"},
			{"ledger", "root-password", "synthetic-ledger-password"},
		},
		agents: []domain.AgentToken{
			{Cluster: "orders", Member: "ordersm0", Token: "synthetic-orders-agent-token"},
			{Cluster: "ledger", Member: "ledgerm0", Token: "synthetic-ledger-agent-token"},
		},
		storage: domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example.com", Region: "test-region", Bucket: "test-backups", Prefix: "snapshots", VerifyTLS: true, Credentials: domain.StorageCredentials{AccessKeyID: "synthetic-access-key", SecretAccessKey: "synthetic-secret-key"}, Description: "Legacy archive"},
		backup:  domain.Backup{Name: "snapshot", Cluster: "orders", Storage: "archive", Type: "logical", Trigger: "manual", Phase: domain.PhaseSucceeded, PBMName: "snapshot-pbm", PBMPrefix: "snapshots/orders", SourceVersion: "7.0", SourceTopology: domain.Topology{Type: "replicaSet"}, Size: 4096, StartedAt: &at, CompletedAt: &at, CreatedAt: at},
		restore: domain.Restore{Name: "recovery", Cluster: "ledger", Source: domain.RestoreSource{Backup: "snapshot", SourceCluster: "orders"}, Phase: domain.PhaseRunning, StartedAt: &at, CreatedAt: at},
		ops: []domain.Operation{
			{ID: "op-backup", Cluster: "orders", Kind: domain.OperationBackup, Phase: domain.PhaseSucceeded, Step: "complete", Payload: json.RawMessage(`{"backup":"snapshot"}`), Checkpoint: json.RawMessage(`{"pbmName":"snapshot-pbm"}`), Attempts: 1, CreatedAt: at, UpdatedAt: at, CompletedAt: &at, Actor: "user:alice"},
			{ID: "op-restore", Cluster: "ledger", Kind: domain.OperationRestore, Phase: domain.PhaseRunning, Step: "restore", Payload: json.RawMessage(`{"restore":"recovery"}`), Checkpoint: json.RawMessage(`{"started":true}`), Attempts: 2, CreatedAt: at.Add(time.Second), UpdatedAt: at.Add(time.Minute), StartedAt: &at, Actor: "user:alice"},
		},
		events: []domain.Event{
			{Cluster: "orders", Operation: "op-backup", Type: "BackupSucceeded", Reason: "Completed", Message: "Backup completed", Actor: "user:alice", CreatedAt: at},
			{Cluster: "ledger", Operation: "op-restore", Type: "RestoreStarted", Reason: "Requested", Message: "Restore started", Actor: "user:alice", CreatedAt: at.Add(time.Second)},
		},
		metrics: []domain.MetricSample{
			{Cluster: "orders", Member: "ordersm0", Metric: "connections", Timestamp: at, Value: 12, Labels: map[string]string{"role": "primary"}},
			{Cluster: "ledger", Member: "ledgerm0", Metric: "connections", Timestamp: at.Add(time.Second), Value: 4},
		},
		orphan: domain.OrphanedVolume{Hostname: "ordersm1v", ServiceID: "service-test-volume", Cluster: "orders", Member: "ordersm1", FirstError: "volume deletion failed", LastError: "volume deletion pending", FirstFailedAt: at, OrphanedAt: at.Add(time.Minute), LastAttemptAt: at.Add(2 * time.Minute), NextAttemptAt: at.Add(3 * time.Minute), Attempts: 3, LastProcessID: "process-test-delete", ProcessPending: true, JanitorFailures: 2},
		policies: []domain.PolicyRule{
			{PType: "p", Values: []string{"user:alice", "clusters", "read", "*", "allow"}},
			{PType: "p", Values: []string{"user:alice", "backups", "read", "orders*", "allow"}},
		},
	}
	marshal := func(value any) []byte {
		t.Helper()
		data, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		return data
	}
	seal := func(identity string, plaintext []byte) string {
		t.Helper()
		sealed, err := sealer.SealFor(identity, plaintext)
		if err != nil {
			t.Fatal(err)
		}
		return sealed
	}
	// The old writer stored unqualified cluster scopes, empty storage/orphan
	// scopes, and JSON without namespace. Never seed through current store APIs.
	record := func(kind, scope, key string, value any, ciphertext, hash, phase string) {
		t.Helper()
		data := marshal(value)
		var object map[string]json.RawMessage
		if err := json.Unmarshal(data, &object); err != nil {
			t.Fatal(err)
		}
		if _, exists := object["namespace"]; exists {
			t.Fatalf("legacy %s fixture unexpectedly has a namespace", kind)
		}
		exec("INSERT INTO records(kind,scope,key,data,secret,hash,phase,created,updated) VALUES(?,?,?,?,?,?,?,?,?)", kind, scope, key, data, ciphertext, hash, phase, at.UnixNano(), at.UnixNano())
		if ciphertext != "" {
			f.sealed = append(f.sealed, namespaceMigrationSealedRecord{kind, scope, key, ciphertext, hash})
		}
	}
	for i := range f.clusters {
		c := &f.clusters[i]
		if upstream {
			c.RequestedBy = "user:alice"
			exec("INSERT INTO clusters(name,spec,status,internal,generation,deleting,delete_options,created,updated,requested_by) VALUES(?,?,?,?,?,?,?,?,?,?)", c.Name, marshal(c.Spec), marshal(c.Status), []byte(c.Internal), c.Generation, c.Deleting, marshal(c.DeleteOptions), c.CreatedAt.UnixNano(), c.UpdatedAt.UnixNano(), c.RequestedBy)
		} else {
			exec("INSERT INTO clusters(name,spec,status,internal,generation,deleting,delete_options,created,updated) VALUES(?,?,?,?,?,?,?,?,?)", c.Name, marshal(c.Spec), marshal(c.Status), []byte(c.Internal), c.Generation, c.Deleting, marshal(c.DeleteOptions), c.CreatedAt.UnixNano(), c.UpdatedAt.UnixNano())
		}
		c.Namespace = domain.DefaultNamespace
	}
	for _, secret := range f.secrets {
		ciphertext := seal("cluster/"+secret.cluster+"/"+secret.key, []byte(secret.plaintext))
		record("secret", secret.cluster, secret.key, struct{}{}, ciphertext, "", "")
	}
	for i := range f.agents {
		agent := &f.agents[i]
		agent.Hash = secrets.Hash(agent.Token)
		ciphertext := seal("agent/"+agent.Cluster+"/"+agent.Member, marshal(agent.Token))
		record("agent-token", agent.Cluster, agent.Member, agent, ciphertext, agent.Hash, "")
		agent.Namespace = domain.DefaultNamespace
	}
	record("storage", "", f.storage.Name, f.storage, seal("storage/"+f.storage.Name, marshal(f.storage.Credentials)), "", "")
	f.storage.Namespace = domain.DefaultNamespace
	record("backup", f.backup.Cluster, f.backup.Name, f.backup, "", "", string(f.backup.Phase))
	f.backup.Namespace = domain.DefaultNamespace
	record("restore", f.restore.Cluster, f.restore.Name, f.restore, "", "", string(f.restore.Phase))
	f.restore.Namespace = domain.DefaultNamespace
	f.restore.SourceNamespace = domain.DefaultNamespace
	for i := range f.ops {
		op := &f.ops[i]
		record("operation", op.Cluster, op.ID, op, "", "", string(op.Phase))
		op.Namespace = domain.DefaultNamespace
	}
	for i := range f.events {
		event := &f.events[i]
		var result sql.Result
		if upstream {
			event.Resource, event.Target = "clusters", event.Cluster
			result = exec("INSERT INTO events(cluster,type,actor,resource,data,created) VALUES(?,?,?,?,?,?)", event.Cluster, event.Type, event.Actor, event.Resource, marshal(event), event.CreatedAt.UnixNano())
		} else {
			result = exec("INSERT INTO events(cluster,type,data,created) VALUES(?,?,?,?)", event.Cluster, event.Type, marshal(event), event.CreatedAt.UnixNano())
			event.Resource, event.Target = "clusters", event.Cluster
		}
		event.ID, err = result.LastInsertId()
		if err != nil {
			t.Fatal(err)
		}
		event.Namespace = domain.DefaultNamespace
	}
	for i := range f.metrics {
		metric := &f.metrics[i]
		exec("INSERT INTO metrics(cluster,member,metric,timestamp,data) VALUES(?,?,?,?,?)", metric.Cluster, metric.Member, metric.Metric, metric.Timestamp.UnixNano(), marshal(metric))
		metric.Namespace = domain.DefaultNamespace
	}
	// Preserve the legacy wrapper's private janitor bookkeeping fields too.
	type legacyVolume domain.OrphanedVolume
	orphan := struct {
		legacyVolume
		ProcessPending  bool `json:"processPending"`
		JanitorFailures int  `json:"janitorFailures"`
	}{legacyVolume(f.orphan), f.orphan.ProcessPending, f.orphan.JanitorFailures}
	record("orphaned-volume", "", f.orphan.Hostname, orphan, "", "", "")
	f.orphan.Namespace = domain.DefaultNamespace
	for i, policy := range f.policies {
		exec("INSERT INTO policies(position,data) VALUES(?,?)", i, marshal(policy))
	}
	f.backup.OperationID = "op-backup"
	f.restore.OperationID = "op-restore"
	f.ops[0].Target = domain.OperationTarget{Kind: "backup", Name: "snapshot"}
	f.ops[0].Message = "backup completed successfully"
	f.ops[1].Target = domain.OperationTarget{Kind: "restore", Name: "recovery"}
	return f
}

func assertNamespaceMigrationFixture(t *testing.T, s *Store, f namespaceMigrationFixture) {
	t.Helper()
	ctx := context.Background()
	assertNamespaceMigrationNames(t, s.reader, []string{"001.sql", "002.sql", "003.sql", "004.sql", "005_idempotency.sql", "006_operations.sql"})
	clusters, err := s.ListClusters(ctx, domain.DefaultNamespace)
	assertNamespaceMigrationValue(t, "default clusters", clusters, f.clusters, err)
	for _, want := range f.clusters {
		got, err := s.GetCluster(ctx, want.Ref())
		assertNamespaceMigrationValue(t, "cluster "+want.Name, got, &want, err)
	}
	for _, secret := range f.secrets {
		got, err := s.GetClusterSecret(ctx, domain.ClusterRef{Namespace: domain.DefaultNamespace, Name: secret.cluster}, secret.key)
		assertNamespaceMigrationValue(t, "secret "+secret.cluster+"/"+secret.key, string(got), secret.plaintext, err)
	}
	for _, want := range f.agents {
		got, err := s.FindAgentTokenByHash(ctx, want.Hash)
		assertNamespaceMigrationValue(t, "agent hash lookup "+want.Member, got, &want, err)
		got, err = s.GetAgentToken(ctx, domain.ClusterRef{Namespace: domain.DefaultNamespace, Name: want.Cluster}, want.Member)
		assertNamespaceMigrationValue(t, "agent direct lookup "+want.Member, got, &want, err)
	}
	storage, err := s.GetBackupStorage(ctx, domain.DefaultNamespace, f.storage.Name)
	assertNamespaceMigrationValue(t, "storage credentials", storage, &f.storage, err)
	storages, err := s.ListBackupStorages(ctx, domain.DefaultNamespace)
	publicStorage := f.storage
	publicStorage.Credentials = domain.StorageCredentials{}
	assertNamespaceMigrationValue(t, "storage list", storages, []domain.BackupStorage{publicStorage}, err)
	backupRef := domain.ClusterRef{Namespace: domain.DefaultNamespace, Name: f.backup.Cluster}
	backup, err := s.GetBackup(ctx, backupRef, f.backup.Name)
	assertNamespaceMigrationValue(t, "backup", backup, &f.backup, err)
	backups, err := s.ListBackups(ctx, backupRef)
	assertNamespaceMigrationValue(t, "backup list", backups, []domain.Backup{f.backup}, err)
	restoreRef := domain.ClusterRef{Namespace: domain.DefaultNamespace, Name: f.restore.Cluster}
	restore, err := s.GetRestore(ctx, restoreRef, f.restore.Name)
	assertNamespaceMigrationValue(t, "restore", restore, &f.restore, err)
	restores, err := s.ListRestores(ctx, restoreRef)
	assertNamespaceMigrationValue(t, "restore list", restores, []domain.Restore{f.restore}, err)
	ops, err := s.ListOperations(ctx, domain.ClusterRef{Namespace: domain.DefaultNamespace})
	assertNamespaceMigrationValue(t, "operation list", ops, f.ops, err)
	for _, want := range f.ops {
		got, err := s.GetOperation(ctx, want.ID)
		assertNamespaceMigrationValue(t, "operation "+want.ID, got, &want, err)
	}
	events, err := s.ListEvents(ctx, EventFilter{Namespace: domain.DefaultNamespace})
	assertNamespaceMigrationValue(t, "events", events, f.events, err)
	metrics, err := s.QueryMetricSamples(ctx, MetricQuery{Namespace: domain.DefaultNamespace, Start: f.metrics[0].Timestamp, End: f.metrics[1].Timestamp})
	assertNamespaceMigrationValue(t, "metrics", metrics, f.metrics, err)
	orphan, err := s.GetOrphanedVolume(ctx, domain.DefaultNamespace, f.orphan.Hostname)
	assertNamespaceMigrationValue(t, "orphan with janitor bookkeeping", orphan, &f.orphan, err)
	orphans, err := s.ListOrphanedVolumes(ctx, domain.DefaultNamespace)
	assertNamespaceMigrationValue(t, "orphan list", orphans, []domain.OrphanedVolume{f.orphan}, err)
	policies, err := s.ListPolicyRules(ctx)
	assertNamespaceMigrationValue(t, "unchanged legacy policies", policies, f.policies, err)
	for _, want := range f.sealed {
		var ciphertext, hash string
		err := s.reader.QueryRowContext(ctx, "SELECT secret,hash FROM records WHERE kind=? AND scope=? AND key=?", want.kind, want.scope, want.key).Scan(&ciphertext, &hash)
		assertNamespaceMigrationValue(t, "unchanged ciphertext "+want.kind+"/"+want.scope+"/"+want.key, ciphertext, want.ciphertext, err)
		assertNamespaceMigrationValue(t, "unchanged hash "+want.kind+"/"+want.scope+"/"+want.key, hash, want.hash, nil)
	}
}

func assertNamespaceMigrationNames(t *testing.T, db *sql.DB, want []string) {
	t.Helper()
	rows, err := db.QueryContext(context.Background(), "SELECT name FROM schema_migrations ORDER BY name")
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	var got []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			t.Fatal(err)
		}
		got = append(got, name)
	}
	assertNamespaceMigrationValue(t, "applied migrations", got, want, rows.Err())
}

func assertNamespaceMigrationValue(t *testing.T, label string, got, want any, err error) {
	t.Helper()
	if err != nil {
		t.Fatalf("%s: %v", label, err)
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("%s: got %+v, want %+v", label, got, want)
	}
}
