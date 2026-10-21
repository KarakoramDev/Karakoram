// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"time"
)

type handlerNamespaces struct{}

func (handlerNamespaces) Backend(namespace string) (infra.Backend, error) {
	if namespace == "default" || namespace == "dev" {
		return fakeBackend{}, nil
	}
	return nil, fmt.Errorf("namespace unavailable")
}
func (handlerNamespaces) Dialer(string) engine.ContextDialer { return nil }
func (handlerNamespaces) AgentURL(string) string             { return "https://example.com" }

func namespaceServer(t *testing.T) (*Server, string) {
	t.Helper()
	s, _, admin := testServer(t)
	s.namespaces = handlerNamespaces{}
	s.Namespaces = s.namespaces
	if err := s.Store.CreateNamespace(t.Context(), &domain.Namespace{Name: "dev", Phase: "Ready"}); err != nil {
		t.Fatal(err)
	}
	c := domain.Cluster{Namespace: "dev", Name: "prod", Spec: domain.ClusterSpec{Provider: "fake", Version: "8.0", Topology: domain.Topology{Type: "replicaSet"}}, Status: domain.ClusterStatus{Phase: domain.PhaseReady}}
	(&fakeProvider{}).ApplyDefaults(&c.Spec)
	if err := s.Store.CreateCluster(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	return s, admin
}

func decodeNamespaceResponse[T any](t *testing.T, w *httptest.ResponseRecorder, status int) T {
	t.Helper()
	if w.Code != status {
		t.Fatalf("status %d, want %d: %s", w.Code, status, w.Body.String())
	}
	var value T
	if err := json.Unmarshal(w.Body.Bytes(), &value); err != nil {
		t.Fatal(err)
	}
	return value
}

func TestNamespaceDefaultsAndSameNameIsolation(t *testing.T) {
	s, admin := namespaceServer(t)
	for _, tc := range []struct{ query, namespace string }{{"", "default"}, {"?namespace=default", "default"}, {"?namespace=dev", "dev"}} {
		c := decodeNamespaceResponse[api.Cluster](t, call(s, "GET", "/api/v1/clusters/prod"+tc.query, "", admin), 200)
		if value(c.Namespace) != tc.namespace {
			t.Fatal(c)
		}
	}
	decodeNamespaceResponse[api.Cluster](t, call(s, "PATCH", "/api/v1/clusters/prod?namespace=dev", `{"spec":{"engineConfig":"dev-config"}}`, admin), 200)
	defaultCluster, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || defaultCluster.Spec.EngineConfig != "" {
		t.Fatal(defaultCluster, err)
	}
	for _, namespace := range []string{"default", "dev"} {
		path := "/api/v1/clusters/prod/restart?namespace=" + namespace
		decodeNamespaceResponse[api.Operation](t, call(s, "POST", path, "", admin), 202)
		ops := decodeNamespaceResponse[api.OperationList](t, call(s, "GET", "/api/v1/clusters/prod/operations?namespace="+namespace, "", admin), 200)
		expected := 1
		if namespace == "dev" {
			expected++
		} // The desired-state patch also has a tracking record.
		if len(ops.Items) != expected || value(ops.Items[len(ops.Items)-1].Namespace) != namespace {
			t.Fatal(ops)
		}
		for _, route := range []string{"database-users", "metrics", "members", "backups", "restores"} {
			w := call(s, "GET", "/api/v1/clusters/prod/"+route+"?namespace="+namespace, "", admin)
			if w.Code != 200 {
				t.Fatal(route, w.Code, w.Body.String())
			}
		}
	}
	if w := call(s, "DELETE", "/api/v1/clusters/prod?namespace=dev", "", admin); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	defaultCluster, err = s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || defaultCluster.Deleting {
		t.Fatal(defaultCluster, err)
	}
}

func TestNamespaceCollectionAuthorization(t *testing.T) {
	s, admin := namespaceServer(t)
	ctx := t.Context()
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "developer"}, "developer-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(ctx, s.RBAC.Policy()+"\np, user:developer, *, read, dev/**, allow\n"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(ctx, "developer", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, ns := range []string{"default", "dev"} {
		for _, cluster := range []string{"prod", ""} {
			if err = s.Context.Events.Record(ctx, domain.Event{Namespace: ns, Cluster: cluster, Type: "changed", Message: "changed"}); err != nil {
				t.Fatal(err)
			}
		}
		if err = s.Store.EnqueueOperation(ctx, &domain.Operation{Namespace: ns, Cluster: "prod", Kind: "restart"}); err != nil {
			t.Fatal(err)
		}
		v := domain.OrphanedVolume{Namespace: ns, Cluster: "prod", Hostname: "prodm0vol", ServiceID: "volume-test"}
		if err = s.Store.PutOrphanedVolume(ctx, &v); err != nil {
			t.Fatal(err)
		}
	}
	for _, path := range []string{"clusters", "operations", "events", "orphaned-volumes"} {
		w := call(s, "GET", "/api/v1/"+path, "", token)
		if w.Code != 200 || strings.Contains(w.Body.String(), `"namespace":"default"`) || !strings.Contains(w.Body.String(), `"namespace":"dev"`) {
			t.Fatal(path, w.Code, w.Body.String())
		}
		w = call(s, "GET", "/api/v1/"+path+"?namespace=default", "", token)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"items":[]`) {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	all := decodeNamespaceResponse[api.ClusterList](t, call(s, "GET", "/api/v1/clusters", "", admin), 200)
	if len(all.Items) != 3 {
		t.Fatal(all)
	}
	dev := decodeNamespaceResponse[api.ClusterList](t, call(s, "GET", "/api/v1/clusters?namespace=dev", "", admin), 200)
	if len(dev.Items) != 1 {
		t.Fatal(dev)
	}
}

const namespaceClusterSpec = `"spec":{"provider":"fake","version":"8.0","topology":{"type":"replicaSet"},"components":{"engine":{"replicas":1,"resources":{"cpuMode":"SHARED","minCpu":1,"maxCpu":1,"minRam":1,"maxRam":1,"minDisk":5,"maxDisk":5}}}`

func TestCrossNamespaceRestoreAndCloneExportPermission(t *testing.T) {
	s, _ := namespaceServer(t)
	ctx := t.Context()
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "cloner"}, "cloner-test-password"); err != nil {
		t.Fatal(err)
	}
	policy := s.RBAC.Policy() + "\np, user:cloner, clusters, create, dev/**, allow\np, user:cloner, restores, create, dev/**, allow\np, user:cloner, backups, read, default/**, allow\n"
	if err := s.RBAC.ReplacePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(ctx, "cloner", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	backup := domain.Backup{Namespace: "default", Cluster: "prod", Name: "saved", Provider: "fake", Type: "logical", Phase: domain.PhaseSucceeded}
	if err = s.Store.CreateBackup(ctx, &backup); err != nil {
		t.Fatal(err)
	}
	clone := `{"name":"copy",` + namespaceClusterSpec + `,"dataSource":{"namespace":"default","cluster":"prod","backup":"saved"}}}`
	restore := `{"name":"restore-copy","sourceNamespace":"default","source":{"sourceCluster":"prod","backup":"saved"}}`
	for _, tc := range []struct{ path, body string }{{"/api/v1/clusters?namespace=dev", clone}, {"/api/v1/clusters/prod/restores?namespace=dev", restore}} {
		w := call(s, "POST", tc.path, tc.body, token)
		if w.Code != 403 || !strings.Contains(w.Body.String(), "backups/export") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if err = s.RBAC.ReplacePolicy(ctx, policy+"p, user:cloner, backups, export, default/**, allow\n"); err != nil {
		t.Fatal(err)
	}
	c := decodeNamespaceResponse[api.Cluster](t, call(s, "POST", "/api/v1/clusters?namespace=dev", clone, token), 201)
	if value(c.Namespace) != "dev" || value(c.Spec.DataSource.Namespace) != "default" {
		t.Fatal(c)
	}
	r := decodeNamespaceResponse[api.Restore](t, call(s, "POST", "/api/v1/clusters/prod/restores?namespace=dev", restore, token), 202)
	if value(r.Namespace) != "dev" {
		t.Fatal(r)
	}
	for _, ns := range []string{"default", "dev"} {
		events, err := s.Store.ListEvents(ctx, store.EventFilter{Namespace: ns, Type: "Audit"})
		if err != nil || len(events) != 2 {
			t.Fatal(ns, events, err)
		}
		for _, e := range events {
			if e.Actor == "" || !strings.Contains(e.Actor, "cloner (token: test [") || e.Resource != "restores" {
				t.Fatal("cross-namespace event lost actor or resource", e)
			}
		}
	}
}

func TestNamespaceStorageIsolationAndCreateValidation(t *testing.T) {
	s, admin := namespaceServer(t)
	body := `{"name":"shared","type":"s3","endpointURL":"https://s3.example.com","region":"us-east-1","bucket":"test","credentials":{"accessKeyId":"test-access","secretAccessKey":"test-secret"}}`
	for _, query := range []string{"", "?namespace=dev"} {
		b := decodeNamespaceResponse[api.BackupStorage](t, call(s, "POST", "/api/v1/backup-storages"+query, body, admin), 201)
		if value(b.Namespace) != domain.NormalizedNamespace(strings.TrimPrefix(query, "?namespace=")) {
			t.Fatal(b)
		}
	}
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "storage-reader"}, "storage-reader-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(t.Context(), s.RBAC.Policy()+"\np, user:storage-reader, backup-storages, read, dev/**, allow\n"); err != nil {
		t.Fatal(err)
	}
	reader, _, err := s.Auth.CreateToken(t.Context(), "storage-reader", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	list := decodeNamespaceResponse[api.BackupStorageList](t, call(s, "GET", "/api/v1/backup-storages", "", reader), 200)
	if len(list.Items) != 1 || value(list.Items[0].Namespace) != "dev" {
		t.Fatal(list)
	}
	if w := call(s, "POST", "/api/v1/backup-storages?namespace=dev", body, reader); w.Code != 403 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "POST", "/api/v1/backup-storages?namespace=missing", body, admin); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "POST", "/api/v1/clusters?namespace=missing", `{"name":"new",`+namespaceClusterSpec+`}}`, admin); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	defaultCluster, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	defaultCluster.Spec.Backup.Storage = "shared"
	if err = s.Store.UpdateClusterSpec(t.Context(), domain.ClusterRef{Name: "prod"}, defaultCluster.Spec); err != nil {
		t.Fatal(err)
	}
	if w := call(s, "DELETE", "/api/v1/backup-storages/shared?namespace=dev", "", admin); w.Code != 204 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "PATCH", "/api/v1/clusters/prod?namespace=dev", `{"spec":{"backup":{"storage":"shared"}}}`, admin); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "DELETE", "/api/v1/backup-storages/shared", "", admin); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if _, err = s.Store.GetBackupStorage(context.Background(), "default", "shared"); err != nil {
		t.Fatal(err)
	}
}

func TestNamespaceEventStreamFiltersPerEvent(t *testing.T) {
	s, _ := namespaceServer(t)
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "event-reader"}, "event-reader-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(t.Context(), s.RBAC.Policy()+"\np, user:event-reader, events, read, dev/**, allow\np, user:event-reader, audit, read, dev/**, allow\n"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(t.Context(), "event-reader", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, event := range []domain.Event{
		{Namespace: "default", Cluster: "prod", Type: "changed", Message: "hidden-default"},
		{Namespace: "dev", Cluster: "prod", Type: "changed", Message: "visible-cluster"},
		{Namespace: "dev", Type: "NamespaceValidated", Message: "visible-namespace"},
	} {
		if err := s.Context.Events.Record(t.Context(), event); err != nil {
			t.Fatal(err)
		}
	}
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	r := httptest.NewRequest(http.MethodGet, "/api/v1/events/stream?namespace=dev", nil).WithContext(ctx)
	r.Header.Set("Authorization", "Bearer "+token)
	ctx = context.WithValue(ctx, requestKey{}, r)
	w := &callbackStreamWriter{ResponseRecorder: httptest.NewRecorder()}
	w.flushed = func() {
		if strings.Contains(w.Body.String(), `"message":"visible-namespace"`) {
			cancel()
		}
	}
	if err := testStream(s, ctx, w); err != nil && !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	body := w.Body.String()
	if strings.Contains(body, "hidden-default") || !strings.Contains(body, `"message":"visible-cluster"`) || !strings.Contains(body, `"message":"visible-namespace"`) {
		t.Fatal(body)
	}
}

func TestNamespaceCreateAndSourceDefaults(t *testing.T) {
	s, admin := namespaceServer(t)
	for _, query := range []string{"", "?namespace=dev"} {
		c := decodeNamespaceResponse[api.Cluster](t, call(s, "POST", "/api/v1/clusters"+query, `{"name":"sharedname",`+namespaceClusterSpec+`}}`, admin), 201)
		if value(c.Namespace) != domain.NormalizedNamespace(strings.TrimPrefix(query, "?namespace=")) {
			t.Fatal(c)
		}
	}
	backup := domain.Backup{Namespace: "dev", Cluster: "prod", Name: "saved", Provider: "fake", Type: "logical", Phase: domain.PhaseSucceeded}
	if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
		t.Fatal(err)
	}
	restore := `{"name":"local-restore","source":{"sourceCluster":"prod","backup":"saved"}}`
	decodeNamespaceResponse[api.Restore](t, call(s, "POST", "/api/v1/clusters/prod/restores?namespace=dev", restore, admin), 202)
	r, err := s.Store.GetRestore(t.Context(), domain.ClusterRef{Namespace: "dev", Name: "prod"}, "local-restore")
	if err != nil || r.SourceNamespace != "dev" {
		t.Fatal(r, err)
	}
	clone := `{"name":"localclone",` + namespaceClusterSpec + `,"dataSource":{"cluster":"prod","backup":"saved"}}}`
	c := decodeNamespaceResponse[api.Cluster](t, call(s, "POST", "/api/v1/clusters?namespace=dev", clone, admin), 201)
	if value(c.Spec.DataSource.Namespace) != "dev" {
		t.Fatal(c)
	}
}
