// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"
	"time"

	specification "github.com/KarakoramDev/Karakoram/api"
	"github.com/KarakoramDev/Karakoram/internal/apispec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/openbao"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
	"github.com/KarakoramDev/Karakoram/pkg/client"
)

// Cancel a streaming request only after authentication and the initial flush.
// A deadline can otherwise expire during authentication on a busy test runner.
type cancelOnFlushRecorder struct {
	*httptest.ResponseRecorder
	cancel context.CancelFunc
}

func (w cancelOnFlushRecorder) Flush() {
	w.ResponseRecorder.Flush()
	w.cancel()
}

// Enumerate the specification, not a hand-maintained subset of GET operations.
// Populated inventories ensure item schemas are exercised, not just envelopes.
func TestEveryGETResponseConforms(t *testing.T) {
	s, token := namespaceTestServer(t)
	enableTestOAuth(t, s)
	oauthFixtureGrant(t, s, "conformance", "admin", "kko_conformance", time.Now().Add(time.Minute))
	ctx := t.Context()
	var err error
	inventoryElastic := newInventoryElasticProvider(t, s)
	s.Registry, err = engine.NewRegistry(&fakeElasticAccess{}, psmdb.New(psmdb.Options{}), inventoryElastic, rabbitmq.New(rabbitmq.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	elasticRegistry := s.Registry
	rabbitRegistry, err := engine.NewRegistry(&fakeRabbitMQAccess{}, psmdb.New(psmdb.Options{}), inventoryElastic, rabbitmq.New(rabbitmq.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	postgresRegistry, err := engine.NewRegistry(&fakePostgresAccess{roles: []postgres.Role{{Name: "custom", Login: true, ConnectionLimit: -1, MemberOf: []string{}}}}, psmdb.New(psmdb.Options{}), inventoryElastic, rabbitmq.New(rabbitmq.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	if err := postgresRegistry.Register(&bindingAccessProvider{fakePostgresAccess: &fakePostgresAccess{}}); err != nil {
		t.Fatal(err)
	}
	bindingSource := domain.Cluster{Name: "bindingsrc", Spec: domain.ClusterSpec{Provider: "postgres", Version: "17.11.1"}}
	(&fakeProvider{}).ApplyDefaults(&bindingSource.Spec)
	if err := s.Store.CreateCluster(ctx, &bindingSource); err != nil {
		t.Fatal(err)
	}
	binding := domain.PostgresBinding{ID: "identity-binding", SourceCluster: bindingSource.Name, ConsumerKind: "keycloak", ConsumerName: "identity", Databases: []string{"identity"}, Extensions: []string{}, RoleName: "kb_identity", State: "ready"}
	if _, err := s.Store.EnsurePostgresBinding(ctx, &binding); err != nil {
		t.Fatal(err)
	}
	supabaseRegistry, err := engine.NewRegistry(&fakeProvider{}, &fakeSupabaseAccess{names: []string{"hello"}}, psmdb.New(psmdb.Options{}), inventoryElastic, rabbitmq.New(rabbitmq.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	openbaoRegistry, err := engine.NewRegistry(&fakeProvider{}, &fakeOpenBaoAccess{connections: []openbao.DatabaseConnection{{Name: "app", TargetCluster: "prod", TargetProvider: "psmdb", MountPath: "database", Phase: "ready", Roles: []openbao.DatabaseRole{}, CreatedAt: time.Now()}}}, psmdb.New(psmdb.Options{}), inventoryElastic, rabbitmq.New(rabbitmq.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	openbaoCluster := domain.Cluster{Name: "bao", Spec: domain.ClusterSpec{Provider: "openbao", Version: "2.7.1", Topology: domain.Topology{Type: "cluster"}}}
	(&fakeProvider{}).ApplyDefaults(&openbaoCluster.Spec)
	if err := s.Store.CreateCluster(ctx, &openbaoCluster); err != nil {
		t.Fatal(err)
	}
	supabaseCluster := domain.Cluster{Name: "supabase", Spec: domain.ClusterSpec{Provider: "supabase", Version: "17.11", Topology: domain.Topology{Type: "single"}}}
	(&fakeProvider{}).ApplyDefaults(&supabaseCluster.Spec)
	if err := s.Store.CreateCluster(ctx, &supabaseCluster); err != nil {
		t.Fatal(err)
	}
	cluster, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	cluster.Spec.Exposure = domain.Exposure{Public: true, IPType: "IPV6", Allowlist: []string{"2001:db8::/32"}}
	cluster.Status.Connection = domain.Connection{Hosts: []string{"prodm0:27017"}, PublicHosts: []string{"db.example:27017"}, URI: "mongodb://prodm0:27017"}
	if err := s.Store.UpdateClusterSpec(ctx, domain.ClusterRef{Name: "prod"}, cluster.Spec); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.UpdateClusterState(ctx, cluster); err != nil {
		t.Fatal(err)
	}
	storage := domain.BackupStorage{Name: "archive", Type: "s3", EndpointURL: "https://s3.example", Region: "test", Bucket: "backups"}
	if err := s.Store.CreateBackupStorage(ctx, &storage); err != nil {
		t.Fatal(err)
	}
	backup := domain.Backup{Name: "saved", Cluster: "prod", Storage: "archive", Type: "logical", Trigger: "manual", Phase: domain.PhaseSucceeded}
	if err := s.Store.CreateBackup(ctx, &backup); err != nil {
		t.Fatal(err)
	}
	restore := domain.Restore{Name: "restored", Cluster: "prod", Source: domain.RestoreSource{Backup: "saved", SourceCluster: "prod"}, Phase: domain.PhaseSucceeded}
	if err := s.Store.CreateRestore(ctx, &restore); err != nil {
		t.Fatal(err)
	}
	operation := domain.Operation{Cluster: "prod", Kind: "restart"}
	if err := s.Store.EnqueueOperation(ctx, &operation); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.PutOrphanedVolume(ctx, &domain.OrphanedVolume{Cluster: "prod", Member: "prodm0", Hostname: "prodm0vol", ServiceID: "volume"}); err != nil {
		t.Fatal(err)
	}
	for _, event := range []domain.Event{
		{Cluster: "prod", Type: "Normal", Reason: "ClusterReady", Message: "Ready"},
		{Type: "Audit", Reason: "LoginSucceeded", Message: "Signed in", Resource: "auth", Actor: "admin"},
	} {
		if err := s.Store.AppendEvent(ctx, &event); err != nil {
			t.Fatal(err)
		}
	}
	_, apiToken, err := s.Auth.CreateToken(ctx, "admin", "inventory", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := apispec.Load()
	if err != nil {
		t.Fatal(err)
	}
	paths := make([]string, 0, doc.Paths.Len())
	for path, item := range doc.Paths.Map() {
		if item.Get != nil {
			paths = append(paths, path)
		}
	}
	sort.Strings(paths)
	for _, path := range paths {
		operationID := doc.Paths.Value(path).Get.OperationID
		t.Run(operationID, func(t *testing.T) {
			s.Registry = elasticRegistry
			if strings.Contains(path, "/rabbitmq/") {
				s.Registry = rabbitRegistry
			}
			url := "/api/v1" + strings.NewReplacer("{account}", "admin", "{token}", apiToken.ID, "{cluster}", "prod", "{backup}", "saved", "{restore}", "restored", "{storage}", "archive", "{operation}", operation.ID, "{provider}", "fake", "{namespace}", "default", "{member}", "prodm0", "{database}", "app", "{username}", "app", "{role}", "custom", "{vhost}", "app", "{user}", "app", "{name}", "app").Replace(path)
			if strings.Contains(path, "/postgres/") {
				s.Registry = postgresRegistry
				if operationID == "listPostgresBindings" {
					url = strings.Replace(url, "/clusters/prod/", "/clusters/bindingsrc/", 1)
				}
				if operationID == "listPostgresGrants" {
					url += "?database=app"
				}
			}
			if strings.Contains(path, "/elasticsearch/") {
				url = strings.Replace(url, "/clusters/prod/", "/clusters/search/", 1)
			}
			if strings.Contains(path, "/supabase/") {
				s.Registry = supabaseRegistry
				url = strings.Replace(url, "/clusters/prod/", "/clusters/supabase/", 1)
			}
			if strings.Contains(path, "/openbao/") {
				s.Registry = openbaoRegistry
				url = strings.Replace(url, "/clusters/prod/", "/clusters/bao/", 1)
				url = strings.ReplaceAll(url, "{connection}", "app")
			}
			if path == "/healthz" || path == "/metrics" {
				url = path
			}
			if operationID == "oidcCallback" {
				url += "?code=test&state=test"
			}
			requestCtx, cancel := context.WithCancel(ctx)
			defer cancel()
			r := httptest.NewRequest(http.MethodGet, url, nil).WithContext(requestCtx)
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			var responseWriter http.ResponseWriter = w
			if operationID == "streamEvents" {
				responseWriter = cancelOnFlushRecorder{ResponseRecorder: w, cancel: cancel}
			}
			testHandler(s).ServeHTTP(responseWriter, r)
			want := http.StatusOK
			if operationID == "oidcLogin" {
				want = http.StatusServiceUnavailable
			}
			if operationID == "oidcCallback" {
				want = http.StatusUnauthorized
			}
			if w.Code != want {
				t.Fatalf("%s: %d, want %d: %s", url, w.Code, want, w.Body.String())
			}
			if strings.Contains(path, "/elasticsearch/roles") {
				for _, field := range []string{`"applications"`, `"kibana-.kibana"`, `"query"`, `"fieldSecurity"`, `"grant"`, `"except"`} {
					if !strings.Contains(w.Body.String(), field) {
						t.Fatalf("native role response omits %s: %s", field, w.Body.String())
					}
				}
			}
			if strings.HasPrefix(operationID, "list") {
				var envelope struct {
					Items []json.RawMessage `json:"items"`
				}
				if err := json.Unmarshal(w.Body.Bytes(), &envelope); err != nil {
					t.Fatal(err)
				}
				if len(envelope.Items) == 0 {
					t.Fatalf("%s did not exercise populated items", operationID)
				}
			}
		})
	}
	// Exercise the real provider's presets as well as the fake DataPlane catalog.
	if w := call(s, "GET", "/api/v1/providers/psmdb", "", token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	t.Run("rabbitmq-native", testRabbitMQNativeGETResponses)
	if w := call(s, "GET", "/api/v1/providers/elasticsearch", "", token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, path := range []string{"/api/v1/clusters/search", "/api/v1/clusters/search/backups", "/api/v1/clusters/search/backups/snapshot"} {
		w := call(s, "GET", path, "", token)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		if strings.Contains(path, "/backups") {
			for _, field := range []string{`"provider":"elasticsearch"`, `"type":"snapshot"`, `"location":"s3://backups/search?repository=archive#snapshot"`, `"completedAt"`} {
				if !strings.Contains(w.Body.String(), field) {
					t.Fatalf("native snapshot response omits %s: %s", field, w.Body.String())
				}
			}
		}
	}
}

func TestOpenAPISpecIsPublicAndCacheable(t *testing.T) {
	s, _, _ := testServer(t)
	w := call(s, "GET", "/api/v1/openapi.yaml", "", "")
	if w.Code != 200 || w.Header().Get("Content-Type") != "application/yaml" || w.Header().Get("Cache-Control") != "public, max-age=300" || !bytes.Equal(w.Body.Bytes(), specification.YAML) {
		t.Fatalf("spec response: status=%d headers=%v", w.Code, w.Header())
	}
	parsed, err := client.ParseGetOpenAPISpecResponse(w.Result())
	if err != nil || parsed.YAML200 == nil || (*parsed.YAML200)["openapi"] != "3.0.3" {
		t.Fatalf("generated specification client: %v (%v)", parsed, err)
	}
}

func TestOIDCErrorsUseJSONContract(t *testing.T) {
	s, _, _ := testServer(t)
	for _, path := range []string{"/api/v1/auth/oidc/login", "/api/v1/auth/oidc/callback?code=test&state=test"} {
		w := call(s, "GET", path, "", "")
		var body struct{ Code, Message string }
		if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body.Code == "" || body.Message == "" {
			t.Fatalf("%s: %s (%v)", path, w.Body.String(), err)
		}
	}
}
