// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package store

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
)

func TestAuditEventMigrationPreservesHistoricalActors(t *testing.T) {
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec("CREATE TABLE schema_migrations (name TEXT PRIMARY KEY)"); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"001.sql", "002.sql"} {
		script, err := migrations.ReadFile("migrations/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(string(script)); err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec("INSERT INTO schema_migrations(name) VALUES(?)", name); err != nil {
			t.Fatal(err)
		}
	}
	for _, event := range []struct {
		cluster string
		data    string
	}{
		{"demo", `{"cluster":"demo","type":"Normal","reason":"PhaseChanged","message":"ready"}`},
		{"demo", `{"cluster":"demo","type":"Normal","actor":"scheduler","message":"backup"}`},
		{"", `{"type":"Audit","actor":"alice","resource":"accounts","target":"bob","message":"created"}`},
		{"", `{"type":"Audit","message":"unknown legacy action"}`},
	} {
		if _, err = db.Exec("INSERT INTO events(cluster,type,data,created) VALUES(?,?,?,?)", event.cluster, "Normal", []byte(event.data), time.Now().UnixNano()); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = db.Exec(`INSERT INTO clusters(name,spec,status,internal,generation,deleting,delete_options,created,updated) VALUES('demo','{}','{}','{}',1,0,'{}',0,0)`); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	key, err := secrets.New(strings.Repeat("m", 32))
	if err != nil {
		t.Fatal(err)
	}
	s, err := Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	ctx := context.Background()
	events, err := s.ListEvents(ctx, EventFilter{})
	if err != nil || len(events) != 4 {
		t.Fatalf("events=%+v err=%v", events, err)
	}
	if events[0].Actor != "" || events[0].Resource != "clusters" || events[0].Target != "demo" || events[1].Actor != "scheduler" || events[3].Actor != "" || events[3].Resource != "" {
		t.Fatalf("historical attribution=%+v", events)
	}
	if events[0].Namespace != domain.DefaultNamespace || events[1].Namespace != domain.DefaultNamespace || events[2].Namespace != "" || events[3].Namespace != "" {
		t.Fatalf("historical namespace attribution=%+v", events)
	}
	filtered, err := s.ListEvents(ctx, EventFilter{Namespace: domain.DefaultNamespace, Actor: "scheduler", Resource: "clusters"})
	if err != nil || len(filtered) != 1 || filtered[0].ID != events[1].ID {
		t.Fatalf("migrated filter=%+v err=%v", filtered, err)
	}
	filtered, err = s.ListEvents(ctx, EventFilter{Actor: "alice", Resource: "accounts"})
	if err != nil || len(filtered) != 1 || filtered[0].Target != "bob" || filtered[0].Cluster != "" {
		t.Fatalf("noncluster migration=%+v err=%v", filtered, err)
	}
	c, err := s.GetCluster(ctx, domain.ParseClusterKey("demo"))
	if err != nil || c.RequestedBy != "" {
		t.Fatalf("historical generation=%+v err=%v", c, err)
	}
	for _, filter := range []struct {
		column string
		value  string
	}{
		{"actor", "alice"}, {"resource", "accounts"},
	} {
		var id, parent, unused int
		var detail string
		err = s.reader.QueryRowContext(ctx, "EXPLAIN QUERY PLAN SELECT id,data FROM events WHERE id>? AND "+filter.column+"=? ORDER BY id LIMIT ?", 0, filter.value, 100).Scan(&id, &parent, &unused, &detail)
		if err != nil || !strings.Contains(detail, "events_"+filter.column) {
			t.Fatalf("%s query plan=%q err=%v", filter.column, detail, err)
		}
	}
}

func TestAuditEventsFiltersDefaultsAndClusterDeletion(t *testing.T) {
	s, path := testStore(t)
	ctx := context.Background()
	c := domain.Cluster{Name: "demo"}
	if err := s.CreateCluster(ctx, &c); err != nil {
		t.Fatal(err)
	}
	for _, event := range []domain.Event{
		{Resource: "accounts", Target: "bob", Actor: "alice", Type: "Audit"},
		{Resource: "settings", Target: "oidc", Actor: "alice", Type: "Audit"},
		{Resource: "accounts", Target: "carol", Actor: "bob", Type: "Audit"},
		{Cluster: "demo", Type: "Normal", Actor: "scheduler"},
		{Cluster: "demo", Type: "Normal"},
		{Resource: "auth", Type: "Audit"},
	} {
		if err := s.AppendEvent(ctx, &event); err != nil {
			t.Fatal(err)
		}
		if event.Actor == "" {
			t.Fatal("new event has no actor")
		}
	}
	contextEvent := domain.Event{Resource: "rbac", Type: "Audit"}
	if err := s.AppendEvent(WithActor(ctx, "carol"), &contextEvent); err != nil || contextEvent.Actor != "carol" {
		t.Fatalf("context event=%+v err=%v", contextEvent, err)
	}
	if err := s.DeleteCluster(ctx, domain.ParseClusterKey("demo")); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	key, _ := secrets.New(strings.Repeat("m", 32))
	reopened, err := Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	for _, tc := range []struct {
		name   string
		filter EventFilter
		count  int
	}{
		{"noncluster", EventFilter{Resource: "accounts"}, 2},
		{"actor", EventFilter{Actor: "alice"}, 2},
		{"combined", EventFilter{Actor: "alice", Resource: "accounts", Type: "Audit"}, 1},
		{"cluster-retained", EventFilter{Cluster: "demo", Resource: "clusters"}, 2},
		{"automatic", EventFilter{Actor: "reconciler"}, 2},
		{"context", EventFilter{Actor: "carol", Resource: "rbac"}, 1},
		{"paged", EventFilter{Resource: "accounts", SinceID: 1, Limit: 1}, 1},
		{"all", EventFilter{}, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			events, err := reopened.ListEvents(ctx, tc.filter)
			if err != nil || len(events) != tc.count {
				t.Fatalf("events=%+v err=%v", events, err)
			}
			for _, event := range events {
				if event.ID <= tc.filter.SinceID || event.Actor == "" {
					t.Fatalf("invalid event=%+v", event)
				}
				if event.Cluster != "" && (event.Resource != "clusters" || event.Target != event.Cluster) {
					t.Fatalf("cluster resource=%+v", event)
				}
			}
		})
	}
}

func TestDesiredGenerationRequesterSurvivesRestartAndStateWrites(t *testing.T) {
	s, path := testStore(t)
	ctx := context.Background()
	if err := s.CreateNamespace(ctx, &domain.Namespace{Name: "dev", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	c := domain.Cluster{Namespace: "dev", Name: "demo", RequestedBy: "untrusted-input"}
	if err := s.CreateCluster(WithActor(ctx, "alice"), &c); err != nil {
		t.Fatal(err)
	}
	if c.RequestedBy != "alice" {
		t.Fatalf("create requester=%q", c.RequestedBy)
	}
	c.Spec.Version = "8.0"
	if err := s.UpdateClusterSpec(WithActor(ctx, "bob"), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateClusterSpec(WithActor(ctx, "carol"), c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	c.RequestedBy = "automatic-overwrite"
	c.Status.Phase = domain.PhaseReady
	if err := s.UpdateClusterState(WithActor(ctx, "reconciler"), &c); err != nil {
		t.Fatal(err)
	}
	assertRequester := func(s *Store, generation int64, actor string) *domain.Cluster {
		t.Helper()
		got, err := s.GetCluster(ctx, c.Ref())
		if err != nil || got.Generation != generation || got.RequestedBy != actor {
			t.Fatalf("cluster=%+v err=%v want generation=%d actor=%q", got, err, generation, actor)
		}
		return got
	}
	assertRequester(s, 2, "bob")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	key, _ := secrets.New(strings.Repeat("m", 32))
	reopened, err := Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertRequester(reopened, 2, "bob")
	if err := reopened.MarkClusterDeleting(WithActor(ctx, "deleter"), c.Ref(), domain.DeleteOptions{KeepVolumes: true}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.MarkClusterDeleting(WithActor(ctx, "other"), c.Ref(), domain.DeleteOptions{KeepVolumes: true}); err != nil {
		t.Fatal(err)
	}
	got := assertRequester(reopened, 3, "deleter")
	got.RequestedBy = "automatic-overwrite"
	if err := reopened.SuspendCluster(ctx, got); err != nil {
		t.Fatal(err)
	}
	assertRequester(reopened, 3, "deleter")
	if err := reopened.ResumeCluster(WithActor(ctx, "resumer"), c.Ref()); err != nil {
		t.Fatal(err)
	}
	assertRequester(reopened, 4, "resumer")
}

func TestSecretGenerationsAndOperationStatePreserveRequester(t *testing.T) {
	s, _ := testStore(t)
	ctx := context.Background()
	c := domain.Cluster{Name: "demo"}
	values := map[string][]byte{"tls/ca.key": []byte("first")}
	if err := s.CreateClusterWithSecrets(WithActor(ctx, "creator"), &c, values); err != nil {
		t.Fatal(err)
	}
	values["tls/ca.key"] = []byte("second")
	if err := s.UpdateClusterSpecWithSecrets(WithActor(ctx, "editor"), c.Ref(), c.Spec, values); err != nil {
		t.Fatal(err)
	}
	if err := s.UpdateClusterSpecWithSecrets(WithActor(ctx, "noop"), c.Ref(), c.Spec, values); err != nil {
		t.Fatal(err)
	}
	op := domain.Operation{Cluster: c.Name}
	if err := s.EnqueueOperation(WithActor(ctx, "operator"), &op); err != nil {
		t.Fatal(err)
	}
	running, err := s.ClaimOperation(ctx, c.Ref())
	if err != nil || running.Actor != "operator" {
		t.Fatalf("operation=%+v err=%v", running, err)
	}
	c.RequestedBy = "stale-create"
	if err := s.UpdateOperationAndClusterState(ctx, running, &c); err != nil {
		t.Fatal(err)
	}
	got, err := s.GetCluster(ctx, c.Ref())
	if err != nil || got.Generation != 2 || got.RequestedBy != "editor" {
		t.Fatalf("cluster=%+v err=%v", got, err)
	}
	scheduled := domain.Operation{Cluster: c.Name, Actor: "scheduler"}
	if err := s.EnqueueOperation(WithActor(ctx, "operator"), &scheduled); err != nil || scheduled.Actor != "scheduler" {
		t.Fatalf("scheduler operation=%+v err=%v", scheduled, err)
	}
}

func TestClusterEventKeepsExplicitResourceAndTarget(t *testing.T) {
	s, _ := testStore(t)
	event := domain.Event{Cluster: "demo", Resource: "database-users", Target: "demo/app/alice", Actor: "admin", Type: "Normal"}
	if err := s.AppendEvent(t.Context(), &event); err != nil {
		t.Fatal(err)
	}
	items, err := s.ListEvents(t.Context(), EventFilter{Resource: "database-users"})
	if err != nil || len(items) != 1 || items[0].Target != "demo/app/alice" || items[0].Cluster != "demo" {
		t.Fatalf("explicit event metadata lost: %+v %v", items, err)
	}
}

func TestAuditNamespaceActorResourceFiltersCompose(t *testing.T) {
	s, _ := testStore(t)
	ctx := t.Context()
	for _, event := range []domain.Event{
		{Namespace: "dev", Cluster: "demo", Actor: "alice", Resource: "clusters", Type: "Audit"},
		{Namespace: "prod", Cluster: "demo", Actor: "alice", Resource: "clusters", Type: "Audit"},
		{Namespace: "dev", Actor: "alice", Resource: "namespaces", Type: "Audit"},
		{Namespace: "dev", Actor: "bob", Resource: "clusters", Type: "Audit"},
		{Actor: "alice", Resource: "accounts", Type: "Audit"},
	} {
		if err := s.AppendEvent(ctx, &event); err != nil {
			t.Fatal(err)
		}
	}
	items, err := s.ListEvents(ctx, EventFilter{Namespace: "dev", Actor: "alice", Resource: "clusters", Cluster: "demo", Type: "Audit"})
	if err != nil || len(items) != 1 || items[0].Namespace != "dev" {
		t.Fatalf("composed filter: %+v %v", items, err)
	}
	items, err = s.ListEvents(ctx, EventFilter{Namespace: "dev", Actor: "alice", Resource: "namespaces"})
	if err != nil || len(items) != 1 || items[0].Cluster != "" || items[0].Namespace != "dev" {
		t.Fatalf("namespace-level filter: %+v %v", items, err)
	}
	items, err = s.ListEvents(ctx, EventFilter{Actor: "alice", Resource: "accounts"})
	if err != nil || len(items) != 1 || items[0].Namespace != "" {
		t.Fatalf("global audit must remain unscoped: %+v %v", items, err)
	}
}
