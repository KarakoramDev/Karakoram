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
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/pkg/client"
)

// Enumerate the specification, not a hand-maintained subset of GET operations.
// Populated inventories ensure item schemas are exercised, not just envelopes.
func TestEveryGETResponseConforms(t *testing.T) {
	s, token := namespaceTestServer(t)
	ctx := t.Context()
	var err error
	s.Registry, err = engine.NewRegistry(&fakeRabbitMQAccess{}, psmdb.New(psmdb.Options{}))
	if err != nil {
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
	_, apiToken, err := s.Auth.CreateToken(ctx, "admin", "inventory", nil)
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
			url := "/api/v1" + strings.NewReplacer("{account}", "admin", "{token}", apiToken.ID, "{cluster}", "prod", "{backup}", "saved", "{restore}", "restored", "{storage}", "archive", "{operation}", operation.ID, "{provider}", "fake", "{namespace}", "default", "{member}", "prodm0", "{database}", "app", "{username}", "app", "{role}", "custom", "{vhost}", "app", "{user}", "app", "{name}", "app").Replace(path)
			if path == "/healthz" || path == "/metrics" {
				url = path
			}
			if operationID == "oidcCallback" {
				url += "?code=test&state=test"
			}
			requestCtx := ctx
			if operationID == "streamEvents" {
				var cancel context.CancelFunc
				requestCtx, cancel = context.WithTimeout(ctx, 10*time.Millisecond)
				defer cancel()
			}
			r := httptest.NewRequest(http.MethodGet, url, nil).WithContext(requestCtx)
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			testHandler(s).ServeHTTP(w, r)
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
