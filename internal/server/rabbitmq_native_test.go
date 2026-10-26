// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
)

// Keep the real provider, encrypted secrets, namespace dialing and full HTTP
// handler chain in these regressions; only the remote broker is in-process.
func nativeRabbitMQServer(t *testing.T, management http.Handler) (*Server, string) {
	t.Helper()
	s, _, token := testServer(t)
	broker := httptest.NewServer(management)
	t.Cleanup(broker.Close)
	s.namespaces = rabbitMQAccessNamespaces{StaticNamespaces: engine.StaticNamespaces{DefaultBackend: fakeBackend{}}, address: broker.Listener.Addr().String()}
	var err error
	s.Registry, err = engine.NewRegistry(rabbitmq.New(rabbitmq.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	c := domain.Cluster{Name: "brokers", Spec: domain.ClusterSpec{Provider: "rabbitmq", Version: "4.3.6", TLS: domain.TLSConfig{Mode: "disabled"}}, Internal: json.RawMessage(`{"members":[{"name":"brokersm0","stage":"ready","tlsMode":"disabled"}]}`)}
	if err := s.Store.CreateCluster(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	c.Status.Phase, c.Status.ObservedGeneration = domain.PhaseReady, c.Generation
	if err := s.Store.UpdateClusterState(t.Context(), &c); err != nil {
		t.Fatal(err)
	}
	for name, secret := range map[string]string{"karakoram": "control-secret", "admin": "admin-secret"} {
		if err := s.Store.SetClusterSecret(t.Context(), c.Ref(), "users/"+name, []byte(secret)); err != nil {
			t.Fatal(err)
		}
	}
	return s, token
}

func TestRabbitMQNativeValidationErrors(t *testing.T) {
	writes := 0
	s, token := nativeRabbitMQServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/api/overview" {
			fmt.Fprint(w, `{}`)
			return
		}
		if r.Method == "GET" && strings.HasPrefix(r.URL.Path, "/api/topic-permissions/") {
			fmt.Fprint(w, `[{"vhost":"/","user":"app","exchange":"events","write":"","read":""}]`)
			return
		}
		if r.Method == "GET" {
			fmt.Fprint(w, `{}`)
			return
		}
		writes++
		w.WriteHeader(400)
		if strings.Contains(r.URL.Path, "policies/") {
			fmt.Fprint(w, `{"error":"bad_request","reason":"[unknown-definition] are not recognised policy settings; submitted-secret"}`)
			return
		}
		fmt.Fprint(w, `{"error":"bad_request","reason":["invalid_regexp","[submitted-secret",["missing terminating ] for character class",17]]}`)
	}))
	for _, tc := range []struct{ resource, body, field string }{
		{"permissions", `{"vhost":"/","user":"app","configure":"[submitted-secret","write":"","read":""}`, "configure,write,read"},
		{"topic-permissions", `{"vhost":"/","user":"app","exchange":"events","write":"[submitted-secret","read":""}`, "write,read"},
		{"policies", `{"vhost":"/","name":"app","pattern":".*","applyTo":"queues","priority":0,"definition":{"unknown-definition":"submitted-secret"}}`, "definition"},
		{"operator-policies", `{"vhost":"/","name":"app","pattern":".*","applyTo":"queues","priority":0,"definition":{"unknown-definition":"submitted-secret"}}`, "definition"},
	} {
		t.Run(tc.resource, func(t *testing.T) {
			w := call(s, "PUT", "/api/v1/clusters/brokers/rabbitmq/"+tc.resource+"/%2F/app", tc.body, token)
			var body api.Error
			if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
				t.Fatal(err)
			}
			if w.Code != 400 || body.Details == nil || (*body.Details)["field"] != tc.field {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
			if strings.Contains(w.Body.String(), "submitted-secret") || strings.Contains(w.Body.String(), "unknown-definition") || !strings.Contains(body.Message, "RabbitMQ") {
				t.Fatal("unsafe or unhelpful native error", w.Body.String())
			}
		})
	}
	if writes != 4 {
		t.Fatalf("native writes=%d", writes)
	}
}

func TestRabbitMQProtectedUserHTTPDetails(t *testing.T) {
	s, token := nativeRabbitMQServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("protected mutation contacted broker: %s", r.URL.Path)
		w.WriteHeader(500)
	}))
	for _, user := range []string{"karakoram", "admin", "guest", "monitor"} {
		explanation := "managed by Karakoram; rotate with system-users rotate"
		if user == "guest" || user == "monitor" {
			explanation = "reserved identity; cannot be created or modified"
		}
		for _, resource := range []string{"users", "permissions", "topic-permissions"} {
			for _, method := range []string{"POST", "PUT", "DELETE"} {
				path := "/api/v1/clusters/brokers/rabbitmq/" + resource
				body := fmt.Sprintf(`{"name":%q,"tags":[],"password":"submitted-secret"}`, user)
				if resource == "permissions" {
					body = fmt.Sprintf(`{"vhost":"/","user":%q,"configure":"","write":"","read":""}`, user)
				}
				if resource == "topic-permissions" {
					body = fmt.Sprintf(`{"vhost":"/","user":%q,"exchange":"events","write":"","read":""}`, user)
				}
				if method != "POST" {
					if resource != "users" {
						path += "/%2F"
					}
					path += "/" + user
				}
				if method == "DELETE" {
					body = ""
				}
				w := call(s, method, path, body, token)
				var response struct {
					Message string
					Details struct{ Resource, Name, Reason string }
				}
				if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
					t.Fatal(err)
				}
				if w.Code != 409 || response.Details.Name != user || response.Details.Resource != "rabbitmq-user" || response.Details.Reason != "protected" || !strings.Contains(response.Message, explanation) || strings.Contains(w.Body.String(), "submitted-secret") {
					t.Fatalf("%s %s: %d %s", method, path, w.Code, w.Body.String())
				}
				if (user == "guest" || user == "monitor") && strings.Contains(response.Message, "rotate") {
					t.Fatal("reserved name incorrectly offered rotation", response.Message)
				}
			}
		}
	}
}

func TestRabbitMQUserPasswordSchema(t *testing.T) {
	s, _, token := testServer(t)
	f := &fakeRabbitMQAccess{}
	s.Registry, _ = engine.NewRegistry(f)
	for _, credentials := range []string{"", `,"password":"secret"`, `,"passwordHash":"private-hash"`, `,"password":"secret","passwordHash":"private-hash"`} {
		f.last = ""
		w := call(s, "POST", "/api/v1/clusters/prod/rabbitmq/users", `{"name":"app","tags":[]`+credentials+`}`, token)
		if strings.Contains(credentials, "passwordHash") && strings.Contains(credentials, `"password"`) {
			if w.Code != 400 || !strings.Contains(w.Body.String(), `"field":"password"`) || f.last != "" || strings.Contains(w.Body.String(), "private-hash") {
				t.Fatal(w.Code, w.Body.String(), f.last)
			}
		} else if w.Code != 201 || f.last != "createUser" {
			t.Fatal(w.Code, w.Body.String(), f.last)
		}
	}
}

func TestRabbitMQBackupLimitErrors(t *testing.T) {
	s, token := nativeRabbitMQServer(t, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("unsupported backup contacted broker")
		w.WriteHeader(500)
	}))
	for _, kind := range []string{"logical", "physical"} {
		w := call(s, "POST", "/api/v1/clusters/brokers/backups", fmt.Sprintf(`{"name":"snapshot","storage":"archive","type":%q}`, kind), token)
		if w.Code != 400 || !strings.Contains(w.Body.String(), `"field":"type"`) || !strings.Contains(w.Body.String(), "definitions") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w := call(s, "GET", "/api/v1/clusters/brokers/pitr", "", token)
	if w.Code != 400 || !strings.Contains(w.Body.String(), `"field":"source.pitrTime"`) || !strings.Contains(w.Body.String(), "RabbitMQ does not support PITR") {
		t.Fatal(w.Code, w.Body.String())
	}
	backups, err := s.Store.ListBackups(t.Context(), domain.ClusterRef{Name: "brokers"})
	if err != nil || len(backups) != 0 {
		t.Fatal(backups, err)
	}
}

func TestRabbitMQMissingNamespaceAuthorization(t *testing.T) {
	s, _, admin := testServer(t)
	s.Registry, _ = engine.NewRegistry(&fakeRabbitMQAccess{})
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "denied"}, "test-password-long"); err != nil {
		t.Fatal(err)
	}
	denied, _, err := s.Auth.CreateToken(t.Context(), "denied", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, route := range []string{"vhosts", "vhosts/%2F", "permissions/%2F/app", "topic-permissions/%2F/app", "policies/%2F/app", "operator-policies/%2F/app"} {
		path := "/api/v1/clusters/prod/rabbitmq/" + route + "?namespace=missing"
		w := call(s, "GET", path, "", admin)
		if w.Code != 404 || !strings.Contains(w.Body.String(), `"resource":"namespace"`) || !strings.Contains(w.Body.String(), `"name":"missing"`) {
			t.Fatal(w.Code, w.Body.String())
		}
		w = call(s, "GET", path, "", denied)
		if w.Code != 403 || strings.Contains(w.Body.String(), "not found") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestRabbitMQRejectNullCreateOptions(t *testing.T) {
	s, _, token := testServer(t)
	before, err := s.Store.ListClusters(t.Context(), "")
	if err != nil {
		t.Fatal(err)
	}
	operations, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "brokers"})
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters", `{"name":"brokers","spec":{"provider":"rabbitmq","version":"4.3.6","topology":{"type":"cluster"},"components":{"engine":{"replicas":1,"resources":{"cpuMode":"SHARED","minCpu":1,"maxCpu":2,"minRam":1,"maxRam":2,"minDisk":5,"maxDisk":10}}},"engineOptions":null}}`, token)
	if w.Code != 400 || !strings.Contains(w.Body.String(), `"field":"spec.engineOptions"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	after, err := s.Store.ListClusters(t.Context(), "")
	if err != nil || len(before) != len(after) {
		t.Fatal(after, err)
	}
	afterOps, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "brokers"})
	if err != nil || len(operations) != len(afterOps) {
		t.Fatal(afterOps, err)
	}
}

func TestRabbitMQDefinitionsBackupPagination(t *testing.T) {
	s, token := nativeRabbitMQServer(t, http.NotFoundHandler())
	for i, phase := range []domain.Phase{domain.PhaseSucceeded, domain.PhaseFailed, domain.PhasePending} {
		completed := time.Date(2026, 1, i+1, 0, 0, 0, 0, time.UTC)
		b := domain.Backup{Name: fmt.Sprintf("backup%d", i), Cluster: "brokers", Provider: "rabbitmq", Storage: "archive", Type: "definitions", Trigger: "manual", Phase: phase, CompletedAt: &completed}
		if err := s.Store.CreateBackup(t.Context(), &b); err != nil {
			t.Fatal(err)
		}
	}
	var items []api.Backup
	cursor := ""
	for {
		path := "/api/v1/clusters/brokers/backups?limit=1"
		if cursor != "" {
			path += "&cursor=" + cursor
		}
		w := call(s, "GET", path, "", token)
		var page api.BackupList
		if err := json.Unmarshal(w.Body.Bytes(), &page); err != nil || w.Code != 200 {
			t.Fatal(w.Code, w.Body.String(), err)
		}
		items = append(items, page.Items...)
		if page.NextCursor == nil {
			break
		}
		cursor = *page.NextCursor
	}
	if len(items) != 3 {
		t.Fatal(items)
	}
	succeeded := 0
	for _, item := range items {
		if item.Type != "definitions" {
			t.Fatal(item)
		}
		if item.Phase == "Succeeded" {
			succeeded++
			if item.CompletedAt == nil {
				t.Fatal(item)
			}
		}
	}
	if succeeded != 1 {
		t.Fatal(items)
	}
}

func TestRabbitMQRetainedDefinitionsCloneRemainsPatchableAfterSourceDeletion(t *testing.T) {
	s, _, token := testServer(t)
	provider := rabbitmq.New(rabbitmq.Options{Store: s.Store})
	var err error
	s.Registry, err = engine.NewRegistry(provider)
	if err != nil {
		t.Fatal(err)
	}
	source := domain.Cluster{Name: "source"}
	provider.ApplyDefaults(&source.Spec)
	if err := s.Store.CreateCluster(t.Context(), &source); err != nil {
		t.Fatal(err)
	}
	backup := domain.Backup{Name: "saved", Cluster: source.Name, Provider: "rabbitmq", Type: "definitions", Phase: domain.PhaseSucceeded, Location: "source/saved.json.gz", Storage: "archive"}
	if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DeleteCluster(t.Context(), source.Ref()); err != nil {
		t.Fatal(err)
	}
	spec := specAPI(source.Spec)
	spec.DataSource = &api.DataSource{Cluster: source.Name, Backup: &backup.Name}
	body, err := json.Marshal(map[string]any{"name": "copy", "spec": spec})
	if err != nil {
		t.Fatal(err)
	}
	created := call(s, "POST", "/api/v1/clusters", string(body), token)
	if created.Code != http.StatusCreated {
		t.Fatal("retained definitions backup rejected after source cluster deletion", created.Code, created.Body.String())
	}
	completed, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "copy"})
	if err != nil {
		t.Fatal(err)
	}
	completed.Internal = []byte(`{"cloneImported":true}`)
	if err := s.Store.UpdateClusterState(t.Context(), completed); err != nil {
		t.Fatal(err)
	}
	if err := s.Store.DeleteBackup(t.Context(), source.Ref(), backup.Name); err != nil {
		t.Fatal(err)
	}
	patched := call(s, "PATCH", "/api/v1/clusters/copy", `{"spec":{"engineConfig":"consumer_timeout = 600000"}}`, token)
	if patched.Code != http.StatusOK {
		t.Fatal("clone update re-resolved the deleted source", patched.Code, patched.Body.String())
	}
	persisted, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "copy"})
	if err != nil || persisted.Spec.EngineConfig != "consumer_timeout = 600000" || persisted.Spec.DataSource.Cluster != source.Name || persisted.Spec.DataSource.Backup != backup.Name {
		t.Fatal(persisted, err)
	}
	changed := call(s, "PATCH", "/api/v1/clusters/copy", `{"spec":{"dataSource":{"backup":"different"}}}`, token)
	if changed.Code != http.StatusBadRequest || !strings.Contains(changed.Body.String(), "dataSource is immutable") {
		t.Fatal("clone source became mutable", changed.Code, changed.Body.String())
	}
}
