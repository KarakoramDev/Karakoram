// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type unavailableNamespaces struct{}

func (unavailableNamespaces) Backend(string) (infra.Backend, error) {
	return nil, fmt.Errorf("token invalid")
}
func (unavailableNamespaces) Dialer(string) engine.ContextDialer {
	panic("persisted read resolved dialer")
}
func (unavailableNamespaces) AgentURL(string) string { panic("persisted read resolved agent URL") }

func TestCreateClusterRejectsInvalidIdentityBeforeLookup(t *testing.T) {
	s, admin := namespaceServer(t)
	// Authentication retains its store; cluster creation must not reach its store
	// or resolve an unavailable namespace for an invalid submitted identity.
	s.Store = nil
	s.namespaces = unavailableNamespaces{}
	for _, name := range []string{"prod:orders", "prod/orders", "", "a", "Orders", "orders-prod", "orders1234567"} {
		for _, query := range []string{"", "?namespace=dev"} {
			body, err := json.Marshal(api.CreateClusterRequest{Name: name, Spec: api.ClusterSpec{Provider: "fake", Version: "8.0", Topology: api.Topology{Type: "replicaSet"}}})
			if err != nil {
				t.Fatal(err)
			}
			got := decodeNamespaceResponse[api.Error](t, call(s, "POST", "/api/v1/clusters"+query, string(body), admin), 400)
			if got.Code != "invalid_request" || !strings.Contains(got.Message, "cluster name") {
				t.Fatalf("name %q%s: %+v", name, query, got)
			}
		}
	}
	for _, query := range []string{"?namespace=prod:orders", "?namespace=dev/orders", "?namespace="} {
		got := decodeNamespaceResponse[api.Error](t, call(s, "POST", "/api/v1/clusters"+query, `{"name":"orders","spec":{"provider":"fake","version":"8.0","topology":{"type":"replicaSet"}}}`, admin), 400)
		if got.Code != "invalid_request" {
			t.Fatal(query, got)
		}
	}
}

func TestClusterReferenceValidationFailuresAreClientErrors(t *testing.T) {
	s, _ := namespaceServer(t)
	for _, ref := range []domain.ClusterRef{{Name: "prod:orders"}, {Name: "prod/orders"}, {Namespace: "prod:orders", Name: "orders"}} {
		_, err := s.Store.GetCluster(t.Context(), ref)
		if err == nil {
			t.Fatalf("accepted reference %+v", ref)
		}
		for _, failure := range []error{err, fmt.Errorf("load cluster: %w", err)} {
			r := s.failure(failure)
			got := r.body.(api.Error)
			if r.status != 400 || got.Code != "invalid_request" {
				t.Fatalf("reference %+v: %d %+v", ref, r.status, got)
			}
		}
	}
}

func TestInvalidPathIdentitiesCannotReadRetainedBackups(t *testing.T) {
	s, _ := namespaceServer(t)
	ctx := t.Context()
	if err := s.Store.CreateBackup(ctx, &domain.Backup{Namespace: "prod", Cluster: "orders", Name: "saved", Phase: domain.PhaseSucceeded}); err != nil {
		t.Fatal(err)
	}
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "limited"}, "limited-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(ctx, s.RBAC.Policy()+"\np, user:limited, *, *, default/**, allow\n"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(ctx, "limited", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"GET", "DELETE"} {
		for _, path := range []string{"/api/v1/clusters/prod:orders/backups/saved", "/api/v1/clusters/prod:orders/backups"} {
			if method == "DELETE" && strings.HasSuffix(path, "/backups") {
				continue
			}
			w := call(s, method, path, "", token)
			if w.Code != 404 && w.Code != 403 {
				t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
			}
		}
	}
	for _, path := range []string{"/api/v1/clusters/prod?namespace=prod:orders", "/api/v1/clusters?namespace=", "/api/v1/clusters?namespace=dev&namespace=default"} {
		if w := call(s, "GET", path, "", token); w.Code != 400 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
	if w := call(s, "GET", "/api/v1/namespaces/prod:orders", "", token); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestRetainedBackupAuthorizesPersistedIdentity(t *testing.T) {
	s, sealer, _ := testServer(t)
	path := filepath.Join(t.TempDir(), "retained.db")
	st, err := store.Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s.Store = st
	ctx := t.Context()
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "limited"}, "limited-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(ctx, s.RBAC.Policy()+"\np, user:limited, backups, *, default/**, allow\n"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(ctx, "limited", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Deliberately inconsistent persisted metadata must never widen authorization.
	b := domain.Backup{Cluster: "gone", Name: "saved"}
	if err = s.Store.CreateBackup(ctx, &b); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.ExecContext(ctx, `UPDATE records SET data=? WHERE kind='backup' AND scope='gone'`, `{"namespace":"prod","cluster":"orders","name":"saved"}`); err != nil {
		t.Fatal(err)
	}
	for _, method := range []string{"GET", "DELETE"} {
		if w := call(s, method, "/api/v1/clusters/gone/backups/saved", "", token); w.Code != 403 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	list := decodeNamespaceResponse[api.BackupList](t, call(s, "GET", "/api/v1/clusters/gone/backups", "", token), 200)
	if len(list.Items) != 0 {
		t.Fatal("unauthorized persisted backup returned", list)
	}
}

func TestOperationByIDAuthorizesStoredNamespace(t *testing.T) {
	s, admin := namespaceServer(t)
	ctx := t.Context()
	op := domain.Operation{Namespace: "dev", Cluster: "prod", Kind: "restart"}
	if err := s.Store.EnqueueOperation(ctx, &op); err != nil {
		t.Fatal(err)
	}
	for _, query := range []string{"", "?namespace=default"} {
		got := decodeNamespaceResponse[api.Operation](t, call(s, "GET", "/api/v1/operations/"+op.ID+query, "", admin), 200)
		if value(got.Namespace) != "dev" {
			t.Fatal(got)
		}
	}
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "limited"}, "limited-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(ctx, s.RBAC.Policy()+"\np, user:limited, operations, read, default/**, allow\n"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(ctx, "limited", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	if w := call(s, "GET", "/api/v1/operations/"+op.ID, "", token); w.Code != 403 {
		t.Fatal(w.Code, w.Body.String())
	}
	list := decodeNamespaceResponse[api.OperationList](t, call(s, "GET", "/api/v1/clusters/prod/operations", "", admin), 200)
	if len(list.Items) != 0 {
		t.Fatal("cluster operation list crossed namespace", list)
	}
}

func TestPersistedReadsSurviveUnavailableNamespace(t *testing.T) {
	s, admin := namespaceServer(t)
	s.namespaces = unavailableNamespaces{}
	for _, namespace := range []string{"default", "dev"} {
		for _, route := range []string{"", "/status", "/members", "/backups", "/restores", "/operations"} {
			path := "/api/v1/clusters/prod" + route + "?namespace=" + namespace
			if w := call(s, "GET", path, "", admin); w.Code != 200 {
				t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
			}
		}
		if w := call(s, "GET", "/api/v1/clusters/prod/connection?namespace="+namespace, "", admin); w.Code != 503 || !strings.Contains(w.Body.String(), "Namespace "+namespace) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestScheduleStorageFallsBackToPrimary(t *testing.T) {
	s, admin := namespaceServer(t)
	if err := s.Store.CreateBackupStorage(t.Context(), &domain.BackupStorage{Namespace: "dev", Name: "archive"}); err != nil {
		t.Fatal(err)
	}
	w := call(s, "PATCH", "/api/v1/clusters/prod?namespace=dev", `{"spec":{"backup":{"storage":"archive","schedules":[{"name":"daily","cron":"0 0 * * *","type":"logical","retainCount":1,"enabled":true}]}}}`, admin)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestNamespacesPaginateAfterAuthorization(t *testing.T) {
	s, admin := namespaceTestServer(t)
	for _, ns := range []string{"alpha", "hidden", "omega"} {
		if err := s.Store.CreateNamespace(t.Context(), &domain.Namespace{Name: ns, Phase: "Ready", Token: domain.NamespaceTokenStatus{Source: "stored"}, Relay: domain.NamespaceRelayStatus{Phase: "Pending"}}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "limited"}, "limited-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(t.Context(), s.RBAC.Policy()+"\np, user:limited, namespaces, read, alpha, allow\np, user:limited, namespaces, read, omega, allow\n"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(t.Context(), "limited", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	first := decodeNamespaceResponse[api.NamespaceList](t, call(s, "GET", "/api/v1/namespaces?limit=1", "", token), 200)
	if len(first.Items) != 1 || first.Items[0].Name != "alpha" || first.NextCursor == nil {
		t.Fatal(first)
	}
	second := decodeNamespaceResponse[api.NamespaceList](t, call(s, "GET", "/api/v1/namespaces?limit=1&cursor="+*first.NextCursor, "", token), 200)
	if len(second.Items) != 1 || second.Items[0].Name != "omega" || second.NextCursor != nil {
		t.Fatal(second)
	}
	empty := decodeNamespaceResponse[api.NamespaceList](t, call(s, "GET", "/api/v1/namespaces?cursor=MTAw", "", token), 200)
	if len(empty.Items) != 0 || empty.NextCursor != nil {
		t.Fatal(empty)
	}
	if w := call(s, "GET", "/api/v1/namespaces?cursor=invalid", "", admin); w.Code != 400 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestLegacyCloneCanBePatchedAfterMigration(t *testing.T) {
	s, sealer, admin := testServer(t)
	path := filepath.Join(t.TempDir(), "legacy.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	for _, migration := range []string{"001.sql", "002.sql"} {
		script, err := os.ReadFile(filepath.Join("..", "store", "migrations", migration))
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(string(script)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = db.Exec(`CREATE TABLE schema_migrations(name TEXT PRIMARY KEY); INSERT INTO schema_migrations(name) VALUES('001.sql'),('002.sql')`); err != nil {
		t.Fatal(err)
	}
	p := psmdb.New(psmdb.Options{})
	spec := domain.ClusterSpec{}
	p.ApplyDefaults(&spec)
	spec.Components.Engine.Replicas = 1
	for _, name := range []string{"source", "clone"} {
		next := spec
		if name == "clone" {
			next.DataSource = &domain.DataSource{Cluster: "source", Backup: "saved"}
		}
		data, err := json.Marshal(next)
		if err != nil {
			t.Fatal(err)
		}
		if _, err = db.Exec(`INSERT INTO clusters(name,spec,status,internal,generation,deleting,delete_options,created,updated) VALUES(?,?,'{"phase":"Ready"}','{}',1,0,'{}',1,1)`, name, data); err != nil {
			t.Fatal(err)
		}
	}
	backup := domain.Backup{Cluster: "source", Name: "saved", Phase: domain.PhaseSucceeded, SourceVersion: spec.Version, SourceTopology: spec.Topology}
	data, err := json.Marshal(backup)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec(`INSERT INTO records(kind,scope,key,data,phase,created,updated) VALUES('backup','source','saved',?,'Succeeded',1,1)`, data); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s.Store = st
	s.Registry, err = engine.NewRegistry(psmdb.New(psmdb.Options{Store: st}))
	if err != nil {
		t.Fatal(err)
	}
	old, err := st.GetCluster(t.Context(), domain.ClusterRef{Name: "clone"})
	if err != nil || old.Spec.DataSource.Namespace != "" {
		t.Fatal(old, err)
	}
	w := call(s, "PATCH", "/api/v1/clusters/clone", `{"spec":{"components":{"engine":{"replicas":3}}}}`, admin)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	updated, err := st.GetCluster(t.Context(), domain.ClusterRef{Name: "clone"})
	if err != nil || updated.Spec.Components.Engine.Replicas != 3 || updated.Spec.DataSource.Namespace != "default" {
		t.Fatal(updated, err)
	}
}

func TestCrossNamespaceCloneFromSucceededLogicalBackup(t *testing.T) {
	s, _, admin := testServer(t)
	ctx := t.Context()
	if err := s.Store.CreateNamespace(ctx, &domain.Namespace{Name: "prod", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	provider := psmdb.New(psmdb.Options{Store: s.Store})
	var err error
	s.Registry, err = engine.NewRegistry(provider)
	if err != nil {
		t.Fatal(err)
	}
	spec := domain.ClusterSpec{}
	provider.ApplyDefaults(&spec)
	spec.Backup.Storage = "archive"
	spec.Exposure.Public = true
	spec.Exposure.Allowlist = []string{"2001:db8::1/128"}
	spec.Monitoring.Enabled = true
	source := domain.Cluster{Namespace: "prod", Name: "orders", Spec: spec, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
	if err := s.Store.CreateCluster(ctx, &source); err != nil {
		t.Fatal(err)
	}
	storage := domain.BackupStorage{Namespace: "prod", Name: "archive", Type: "s3", EndpointURL: "https://s3.example.com", Bucket: "backups", Region: "us-east-1", Credentials: domain.StorageCredentials{AccessKeyID: "test-access", SecretAccessKey: "test-secret"}}
	if err := s.Store.CreateBackupStorage(ctx, &storage); err != nil {
		t.Fatal(err)
	}
	backup := domain.Backup{Namespace: "prod", Cluster: source.Name, Name: "saved", Storage: storage.Name, Type: "logical", Trigger: "manual", Phase: domain.PhaseSucceeded, SourceVersion: spec.Version, SourceTopology: spec.Topology}
	if err := s.Store.CreateBackup(ctx, &backup); err != nil {
		t.Fatal(err)
	}
	backupAPI := decodeNamespaceResponse[api.Backup](t, call(s, "GET", "/api/v1/clusters/orders/backups/saved?namespace=prod", "", admin), 200)
	if value(backupAPI.Namespace) != "prod" || backupAPI.Phase != api.BackupPhaseSucceeded {
		t.Fatal(backupAPI)
	}
	sourceAPI := decodeNamespaceResponse[api.Cluster](t, call(s, "GET", "/api/v1/clusters/orders?namespace=prod", "", admin), 200)
	cloneSpec := sourceAPI.Spec
	cloneSpec.Backup = nil
	cloneSpec.DataSource = &api.DataSource{Namespace: ptr("prod"), Cluster: source.Name, Backup: &backupAPI.Name}
	body, err := json.Marshal(api.CreateClusterRequest{Name: "copy", Spec: cloneSpec})
	if err != nil {
		t.Fatal(err)
	}
	clone := decodeNamespaceResponse[api.Cluster](t, call(s, "POST", "/api/v1/clusters?namespace=default", string(body), admin), 201)
	if value(clone.Namespace) != "default" || value(clone.Spec.DataSource.Namespace) != "prod" || clone.Spec.DataSource.Cluster != source.Name || value(clone.Spec.DataSource.Backup) != backup.Name {
		t.Fatal(clone)
	}
	persisted, err := s.Store.GetCluster(ctx, domain.ClusterRef{Namespace: "default", Name: "copy"})
	if err != nil || persisted.Spec.Backup.Storage != "" || len(persisted.Spec.Backup.Schedules) != 0 {
		t.Fatal(persisted, err)
	}
}
