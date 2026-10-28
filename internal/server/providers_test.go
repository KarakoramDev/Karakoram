// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/internal/engine/rabbitmq"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/infra/zerops"
	"github.com/KarakoramDev/Karakoram/internal/pgbinding"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestBuiltInProviderCatalogConforms(t *testing.T) {
	s, _, token := testServer(t)
	var err error
	s.Registry, err = engine.NewRegistry(engineProviders(engineOptions{Store: s.Store, Bindings: s.postgresBindings})...)
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "GET", "/api/v1/providers", "", token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var catalog struct {
		Items []api.Provider `json:"items"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &catalog); err != nil {
		t.Fatal(err)
	}
	got := make(map[string]bool, len(catalog.Items))
	for _, provider := range catalog.Items {
		got[provider.Name] = true
		w := call(s, "GET", "/api/v1/providers/"+provider.Name, "", token)
		if w.Code != 200 {
			t.Fatalf("%s: %d %s", provider.Name, w.Code, w.Body.String())
		}
	}
	want := map[string]bool{"psmdb": true, "rabbitmq": true, "openbao": true, "elasticsearch": true, "postgres": true, "supabase": true, "keycloak": true, "temporal": true, "mysql": true, "pxc": true, "neo4j": true, "weaviate": true}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("built-in providers = %v, want %v", got, want)
	}
}

func TestBuiltInProviderPartialResourceDefaults(t *testing.T) {
	s, _, _ := testServer(t)
	bindings := pgbinding.New(pgbinding.Options{Store: s.Store})
	for _, provider := range engineProviders(engineOptions{Store: s.Store, Bindings: bindings}) {
		for _, topology := range provider.Describe().Topologies {
			for name, info := range topology.Components {
				for _, field := range []string{"cpuMode", "minCpu", "maxCpu", "minRam", "maxRam", "minDisk", "maxDisk"} {
					t.Run(provider.Name()+"/"+topology.Type+"/"+name+"/"+field, func(t *testing.T) {
						defaults := resourcesAPI(info.Defaults.Resources)
						encoded, err := json.Marshal(defaults)
						if err != nil {
							t.Fatal(err)
						}
						var fields map[string]json.RawMessage
						if err := json.Unmarshal(encoded, &fields); err != nil {
							t.Fatal(err)
						}
						// Supply just one catalog value: all other bounds must
						// still be filled, including for optional components.
						partial, err := json.Marshal(map[string]json.RawMessage{field: fields[field]})
						if err != nil {
							t.Fatal(err)
						}
						var resources api.ResourcesInput
						if err := json.Unmarshal(partial, &resources); err != nil {
							t.Fatal(err)
						}
						spec := domain.ClusterSpec{Provider: provider.Name(), Topology: domain.Topology{Type: topology.Type}}
						component := domain.ComponentSpec{Replicas: info.Defaults.Replicas, Resources: resourcesDomain(&resources)}
						switch name {
						case "engine":
							spec.Components.Engine = component
						case "configServer":
							spec.Components.ConfigServer = component
						case "router":
							spec.Components.Router = component
						default:
							spec.Components.Extra = map[string]domain.ComponentSpec{name: component}
						}
						provider.ApplyDefaults(&spec)
						var got domain.Resources
						switch name {
						case "engine":
							got = spec.Components.Engine.Resources
						case "configServer":
							got = spec.Components.ConfigServer.Resources
						case "router":
							got = spec.Components.Router.Resources
						default:
							got = spec.Components.Extra[name].Resources
						}
						if !reflect.DeepEqual(got, info.Defaults.Resources) {
							t.Fatalf("partial %s: got %+v, want catalog defaults %+v", field, got, info.Defaults.Resources)
						}
					})
				}
			}
		}
	}
}

func TestKeycloakCatalogWithBindingService(t *testing.T) {
	s, _, token := testServer(t)
	bindings := pgbinding.New(pgbinding.Options{Store: s.Store})
	var err error
	s.Registry, err = engine.NewRegistry(engineProviders(engineOptions{Store: s.Store, Bindings: bindings})...)
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "GET", "/api/v1/providers/keycloak", "", token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var provider api.Provider
	if err := json.Unmarshal(w.Body.Bytes(), &provider); err != nil {
		t.Fatal(err)
	}
	if provider.Name != "keycloak" || len(provider.Topologies) != 1 {
		t.Fatal(provider)
	}
}

func TestBuiltInProvidersSharePostgresBindingService(t *testing.T) {
	s, _, _ := testServer(t)
	var typedNil *pgbinding.Manager
	for _, tc := range []struct {
		name      string
		bindings  pgbinding.Service
		available bool
	}{
		{"manager", pgbinding.New(pgbinding.Options{Store: s.Store}), true},
		{"nil", nil, false},
		{"typed-nil-manager", typedNil, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			found := map[string]bool{}
			for _, provider := range engineProviders(engineOptions{Store: s.Store, Bindings: tc.bindings}) {
				if provider.Name() != "openbao" && provider.Name() != "temporal" {
					continue
				}
				found[provider.Name()] = true
				t.Run(provider.Name(), func(t *testing.T) {
					c := domain.Cluster{Name: "consumer", Spec: domain.ClusterSpec{Provider: provider.Name()}}
					if provider.Name() == "openbao" {
						c.Spec.EngineOptions = json.RawMessage(`{"storage":{"type":"postgresql","postgresql":{"cluster":"pg"}}}`)
						var advertised bool
						for _, capability := range provider.Describe().Capabilities {
							advertised = advertised || capability == "postgresql-storage"
						}
						if advertised != tc.available {
							t.Fatalf("postgresql-storage advertised = %t, want %t", advertised, tc.available)
						}
					} else {
						c.Spec.EngineOptions = json.RawMessage(`{"persistence":{"postgres":{"cluster":"pg"}}}`)
					}
					provider.ApplyDefaults(&c.Spec)
					if provider.Name() == "openbao" {
						c.Spec.Components.Engine.Replicas = 1
					}
					err := provider.Validate(t.Context(), nil, &c)
					if tc.available {
						if err != nil {
							t.Fatalf("concrete binding manager rejected: %v", err)
						}
					} else if err == nil || !strings.Contains(strings.ToLower(err.Error()), "binding") {
						t.Fatalf("missing binding manager must reject admission: %v", err)
					}
				})
			}
			if !found["openbao"] || !found["temporal"] {
				t.Fatalf("binding-backed providers missing from catalog: %v", found)
			}
		})
	}
}

func TestServerRetainsSuppliedPostgresBindingManager(t *testing.T) {
	s, _, _ := testServer(t)
	bindings := pgbinding.New(pgbinding.Options{Store: s.Store})
	options := s.Options
	options.Bindings = bindings
	withBindings, err := New(options)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { withBindings.Close() })
	if withBindings.postgresBindings != bindings {
		t.Fatal("server replaced the supplied PostgreSQL binding manager")
	}
}

func TestProviderImmutable(t *testing.T) {
	for _, method := range []string{"PATCH", "PUT"} {
		t.Run(method, func(t *testing.T) {
			s, _, token := testServer(t)
			before, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			body := `{"spec":{"provider":"psmdb"}}`
			if method == "PUT" {
				spec := specAPI(before.Spec)
				spec.Provider = "psmdb"
				data, err := json.Marshal(map[string]any{"spec": spec})
				if err != nil {
					t.Fatal(err)
				}
				body = string(data)
			}
			w := call(s, method, "/api/v1/clusters/prod", body, token)
			if w.Code != 400 || !strings.Contains(w.Body.String(), "provider is immutable") || !strings.Contains(w.Body.String(), `"field":"spec.provider"`) {
				t.Fatal(w.Code, w.Body.String())
			}
			after, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil || after.Spec.Provider != before.Spec.Provider || after.Generation != before.Generation {
				t.Fatal(after, err)
			}
		})
	}
}

func TestOperationPreflight(t *testing.T) {
	s, _, token := testServer(t)
	info := (&fakeProvider{}).Describe()
	info.Operations = nil
	var err error
	s.Registry, err = engine.NewRegistry(&fakeProvider{info: &info})
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ method, path, body string }{
		{"POST", "/restart", ""},
		{"POST", "/rotate-tls", ""},
		{"POST", "/raise-fcv", `{"version":"8.0"}`},
		{"POST", "/members/prodm0/replace", ""},
		{"POST", "/system-users/clusterAdmin/rotate", ""},
		{"POST", "/backups", `{"name":"saved","type":"logical","storage":"archive"}`},
		{"DELETE", "/backups/saved", ""},
		{"POST", "/restores", `{"name":"recovery","source":{"backup":"saved","sourceCluster":"prod"}}`},
	} {
		w := call(s, tc.method, "/api/v1/clusters/prod"+tc.path, tc.body, token)
		if w.Code != 409 {
			t.Errorf("%s %s: %d %s", tc.method, tc.path, w.Code, w.Body.String())
		}
	}
	ops, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || len(ops) != 0 {
		t.Fatal(ops, err)
	}
	backups, err := s.Store.ListBackups(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || len(backups) != 0 {
		t.Fatal(backups, err)
	}
	restores, err := s.Store.ListRestores(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || len(restores) != 0 {
		t.Fatal(restores, err)
	}
}

func TestProviderBackupTypes(t *testing.T) {
	s, _, token := testServer(t)
	info := (&fakeProvider{}).Describe()
	info.BackupTypes = []string{"definitions", "snapshot"}
	var err error
	s.Registry, err = engine.NewRegistry(&fakeProvider{info: &info})
	if err != nil {
		t.Fatal(err)
	}
	storage := domain.BackupStorage{Name: "archive", Type: "s3", Bucket: "bucket", EndpointURL: "https://s3.example.com", Region: "us-east-1"}
	if err := s.Store.CreateBackupStorage(t.Context(), &storage); err != nil {
		t.Fatal(err)
	}
	for _, typ := range []string{"logical", "physical", "definitions", "snapshot"} {
		w := call(s, "POST", "/api/v1/clusters/prod/backups", `{"name":"saved-`+typ+`","type":"`+typ+`","storage":"archive"}`, token)
		want := 400
		if typ == "definitions" || typ == "snapshot" {
			want = 202
		}
		if w.Code != want {
			t.Fatalf("%s: %d %s", typ, w.Code, w.Body.String())
		}
	}
	for _, method := range []string{"PATCH", "POST"} {
		for _, typ := range []string{"physical", "definitions"} {
			body := `{"spec":{"backup":{"schedules":[{"name":"daily","cron":"0 0 * * *","type":"` + typ + `","storage":"archive","retainCount":2,"enabled":true}]}}}`
			path := "/api/v1/clusters/prod"
			want := 200
			if method == "POST" {
				path = "/api/v1/clusters"
				body = `{"name":"new` + typ[:4] + `","spec":{"provider":"fake","version":"8.0","topology":{"type":"cluster"},"components":{"engine":{"replicas":1,"resources":{}}},"backup":{"schedules":[{"name":"daily","cron":"0 0 * * *","type":"` + typ + `","storage":"archive","retainCount":2,"enabled":true}]}}}`
				want = 201
			}
			if typ == "physical" {
				want = 400
			}
			w := call(s, method, path, body, token)
			if w.Code != want {
				t.Fatalf("%s %s: %d %s", method, typ, w.Code, w.Body.String())
			}
		}
	}
}

func TestProviderMetadataAndModelRoundtrip(t *testing.T) {
	info := (&fakeProvider{}).Describe()
	info.Capabilities = []string{"monitoring", "engineConfig"}
	info.Topologies = []domain.TopologyInfo{{Type: "dedicatedMasters", Components: map[string]domain.ComponentInfo{"engine": {Defaults: domain.ComponentSpec{Replicas: 3}}, "master": {Defaults: domain.ComponentSpec{Replicas: 3}}, "dashboard": {Defaults: domain.ComponentSpec{Replicas: 0}}}}}
	p := providerAPI(&fakeProvider{info: &info})
	if p.Capabilities == nil || p.Operations == nil || p.Processes == nil || p.Topologies[0].Defaults.Components.Extra == nil {
		t.Fatal(p)
	}
	extra := *p.Topologies[0].Defaults.Components.Extra
	if len(extra) != 2 || extra["master"].Replicas != 3 || extra["dashboard"].Replicas != 0 {
		t.Fatal(extra)
	}
	spec := specDomain(providerInputSpec(t, p.Topologies[0].Defaults))
	spec.EngineOptions = json.RawMessage(`{"nested":{"value":2},"enabled":true}`)
	back := specDomain(providerInputSpec(t, specAPI(spec)))
	if string(back.EngineOptions) == "" || back.Components.Extra["master"].Replicas != 3 || back.Topology.Type != "dedicatedMasters" {
		t.Fatal(back)
	}
	var options map[string]any
	if err := json.Unmarshal(back.EngineOptions, &options); err != nil || options["enabled"] != true {
		t.Fatal(options, err)
	}
	connection := connectionAPI(domain.Connection{Endpoints: []domain.Endpoint{{Name: "management", URL: "https://example.com", Public: true}}})
	if connection.Endpoints == nil || (*connection.Endpoints)[0].Url != "https://example.com" {
		t.Fatal(connection)
	}
	backup := backupAPI(domain.Backup{Provider: "fake", Location: "snapshot-id"})
	if value(backup.Provider) != "fake" || value(backup.Location) != "snapshot-id" {
		t.Fatal(backup)
	}
}

func providerInputSpec(t *testing.T, spec api.ClusterSpec) api.ClusterInputSpec {
	t.Helper()
	raw, err := json.Marshal(spec)
	if err != nil {
		t.Fatal(err)
	}
	var input api.ClusterInputSpec
	if err := json.Unmarshal(raw, &input); err != nil {
		t.Fatal(err)
	}
	return input
}

func TestProviderOptionalInputs(t *testing.T) {
	s, _, token := testServer(t)
	for _, tc := range []struct {
		name, spec string
		want       int
	}{
		{"extras", `{"components":{"extra":{"dashboard":{"replicas":0},"master":{"replicas":3}}},"engineOptions":{"nested":{"enabled":true}}}`, 201},
		{"unknown", `{"components":{"extra":{"dashboard":{"unknown":true}}}}`, 400},
		{"negative", `{"components":{"extra":{"dashboard":{"replicas":-1}}}}`, 400},
		{"badoptions", `{"engineOptions":[]}`, 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var spec map[string]any
			if err := json.Unmarshal([]byte(tc.spec), &spec); err != nil {
				t.Fatal(err)
			}
			spec["provider"] = "fake"
			body, err := json.Marshal(map[string]any{"name": tc.name, "spec": spec})
			if err != nil {
				t.Fatal(err)
			}
			w := call(s, "POST", "/api/v1/clusters", string(body), token)
			if w.Code != tc.want {
				t.Fatal(w.Code, w.Body.String())
			}
			if tc.want == 201 {
				c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: tc.name})
				if err != nil || c.Spec.Components.Extra["master"].Replicas != 3 || len(c.Spec.Components.Extra) != 2 || len(c.Spec.EngineOptions) == 0 {
					t.Fatal(c, err)
				}
				w = call(s, "PATCH", "/api/v1/clusters/"+tc.name, `{"spec":{"components":{"extra":{"master":{"replicas":5},"dashboard":null}},"engineOptions":{"nested":{"enabled":false}}}}`, token)
				if w.Code != 200 {
					t.Fatal(w.Code, w.Body.String())
				}
				c, err = s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: tc.name})
				if err != nil || c.Spec.Components.Extra["master"].Replicas != 5 || len(c.Spec.Components.Extra) != 1 {
					t.Fatal(c, err)
				}
			}
		})
	}
}

func TestProviderProcessLogs(t *testing.T) {
	s, _, token := testServer(t)
	info := (&fakeProvider{}).Describe()
	info.Processes = []string{"worker"}
	var err error
	s.Registry, err = engine.NewRegistry(&fakeProvider{info: &info})
	if err != nil {
		t.Fatal(err)
	}
	for _, process := range []string{"", "worker", "agent", "mongod", "unknown", "empty"} {
		path := "/api/v1/clusters/prod/members/prodm0/logs"
		if process != "" {
			value := process
			if process == "empty" {
				value = ""
			}
			path += "?process=" + value
		}
		w := call(s, "GET", path, "", token)
		want := 200
		if process == "mongod" || process == "unknown" || process == "empty" {
			want = 400
		}
		if w.Code != want {
			t.Fatal(process, w.Code, w.Body.String())
		}
	}
}

type memberLogsBackend struct {
	fakeBackend
	supported bool
	calls     int
	member    string
	query     infra.LogQuery
	err       error
	logs      []infra.LogLine
}

func (b *memberLogsBackend) Capabilities() infra.Capabilities {
	return infra.Capabilities{Logs: b.supported}
}

func (b *memberLogsBackend) Logs(_ context.Context, member string, q infra.LogQuery) ([]infra.LogLine, error) {
	b.calls++
	b.member, b.query = member, q
	if b.logs != nil {
		return b.logs, b.err
	}
	return []infra.LogLine{{Message: "configuration applied", Stream: "stderr"}}, b.err
}

type childLogsProvider struct {
	fakeProvider
	calls int
}

func (p *childLogsProvider) MemberLogs(ctx context.Context, rc *engine.Context, member, process string, lines int) ([]domain.MemberLogLine, error) {
	p.calls++
	if process == "agent" {
		return nil, engine.ErrInvalid
	}
	return p.fakeProvider.MemberLogs(ctx, rc, member, process, lines)
}

func TestMemberAgentLogsUseRuntimeBackend(t *testing.T) {
	for _, tc := range []struct {
		name, member, query string
		supported           bool
		err                 error
		want, calls         int
	}{
		{"agent", "prodm0", "?process=agent&lines=37", true, nil, 200, 1},
		{"other-cluster-member", "devm0", "?process=agent", true, nil, 404, 0},
		{"unknown-member", "missing", "?process=agent", true, nil, 404, 0},
		{"invalid-lines", "prodm0", "?process=agent&lines=10001", true, nil, 400, 0},
		{"unsupported", "prodm0", "?process=agent", false, nil, 409, 0},
		{"backend-missing", "prodm0", "?process=agent", true, infra.ErrNotFound, 404, 1},
		{"backend-unavailable", "prodm0", "?process=agent", true, infra.ErrUnavailable, 503, 1},
		{"child", "prodm0", "?process=mongod", true, nil, 200, 0},
		{"default-child", "prodm0", "", true, nil, 200, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, _, token := testServer(t)
			b := &memberLogsBackend{supported: tc.supported, err: tc.err}
			s.namespaces = engine.StaticNamespaces{DefaultBackend: b}
			p := &childLogsProvider{}
			var err error
			s.Registry, err = engine.NewRegistry(p)
			if err != nil {
				t.Fatal(err)
			}
			w := call(s, "GET", "/api/v1/clusters/prod/members/"+tc.member+"/logs"+tc.query, "", token)
			if w.Code != tc.want || b.calls != tc.calls {
				t.Fatalf("status=%d backend calls=%d: %s", w.Code, b.calls, w.Body.String())
			}
			if tc.name == "agent" {
				var logs api.MemberLogs
				if err := json.Unmarshal(w.Body.Bytes(), &logs); err != nil {
					t.Fatal(err)
				}
				if p.calls != 0 || b.member != "prodm0" || b.query != (infra.LogQuery{Limit: 37}) {
					t.Fatalf("provider calls=%d member=%q query=%+v", p.calls, b.member, b.query)
				}
				if len(logs.Lines) != 1 || logs.Lines[0].Process != "agent" || logs.Lines[0].Message != "configuration applied" {
					t.Fatalf("runtime logs not returned: %+v", logs)
				}
				if !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
					t.Fatal("logs must not be cached")
				}
			}
			if (tc.name == "child" || tc.name == "default-child") && p.calls != 1 {
				t.Fatalf("child logs bypassed provider: %d calls", p.calls)
			}
		})
	}
}

func TestMemberAgentLogsRedactRuntimeOutput(t *testing.T) {
	s, _, token := testServer(t)
	b := &memberLogsBackend{supported: true, logs: []infra.LogLine{
		{Container: "first", Stream: "stdout", Message: `password: "`},
		{Container: "first", Stream: "stderr", Message: "stderr diagnostic retained"},
		{Container: "second", Stream: "stdout", Message: "second container retained"},
		{Container: "first", Stream: "stdout", Message: "multiline-value"},
		{Container: "first", Stream: "stdout", Message: `closing-value"`},
		{Container: "first", Stream: "stdout", Message: `token=inline-value progress`},
		{Container: "first", Stream: "stdout", Message: `{"password":"json-value","message":"ready"}`},
		{Container: "first", Stream: "stdout", Message: "secret: |\n  yaml-value\n2026-10-27T13:00:00Z recovered"},
		{Container: "first", Stream: "stdout", Message: "mongodb://user:uri-value@example.invalid/db"},
		{Container: "first", Stream: "stdout", Message: "password: \"\ntrailing-value"},
	}}
	s.namespaces = engine.StaticNamespaces{DefaultBackend: b}
	w := call(s, "GET", "/api/v1/clusters/prod/members/prodm0/logs?process=agent&lines=100", "", token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var logs api.MemberLogs
	if err := json.Unmarshal(w.Body.Bytes(), &logs); err != nil {
		t.Fatal(err)
	}
	var messages strings.Builder
	for _, line := range logs.Lines {
		if line.Process != "agent" {
			t.Fatalf("unexpected process %q", line.Process)
		}
		messages.WriteString(line.Message)
		messages.WriteByte('\n')
	}
	got := messages.String()
	for _, secret := range []string{"multiline-value", "closing-value", "inline-value", "json-value", "yaml-value", "uri-value", "trailing-value"} {
		if strings.Contains(got, secret) {
			t.Fatalf("runtime credential %q escaped redaction", secret)
		}
	}
	for _, diagnostic := range []string{"stderr diagnostic retained", "second container retained", "ready", "recovered", "[redacted]", "lines omitted: possible credential material"} {
		if !strings.Contains(got, diagnostic) {
			t.Fatalf("missing safe diagnostic %q in %q", diagnostic, got)
		}
	}
	w = call(s, "GET", "/api/v1/clusters/prod/members/prodm0/logs?process=agent&lines=2", "", token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if err := json.Unmarshal(w.Body.Bytes(), &logs); err != nil {
		t.Fatal(err)
	}
	if len(logs.Lines) > 2 || strings.Contains(w.Body.String(), "trailing-value") {
		t.Fatal("redaction expanded past the requested limit or leaked a continuation")
	}
}

func TestMemberAgentLogsRedactDescendingZeropsTail(t *testing.T) {
	s, _, token := testServer(t)
	at := time.Date(2026, 10, 27, 13, 0, 0, 0, time.UTC)
	records := []zerops.RuntimeLog{
		{Timestamp: at.Add(4 * time.Second), Content: "safe first"},
		{Timestamp: at.Add(4 * time.Second), Content: "safe second"},
		{Timestamp: at.Add(3 * time.Second), Content: `closing-value"`},
		{Timestamp: at.Add(2 * time.Second), Content: "private-value"},
		{Timestamp: at.Add(time.Second), Content: `password: "`},
		{Timestamp: at, Content: "older unselected diagnostic"},
	}
	platform := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/project/project/service-stack":
			_, _ = w.Write([]byte(`{"list":[{"id":"member-id","name":"prodm0","projectId":"project"}],"total":1}`))
		case "/service-stack/member-id":
			_, _ = w.Write([]byte(`{"id":"member-id","name":"prodm0","projectId":"project"}`))
		case "/project/project/log":
			_ = json.NewEncoder(w).Encode(map[string]string{"url": "http://" + r.Host + "/signed"})
		case "/signed":
			n, err := strconv.Atoi(r.URL.Query().Get("limit"))
			if err != nil || n < 1 || n > len(records) || r.URL.Query().Get("desc") != "1" {
				t.Error("incorrect newest-tail query")
				w.WriteHeader(http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"items": records[:n]})
		default:
			t.Errorf("unexpected platform path %q", r.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	defer platform.Close()
	b, err := zerops.New(zerops.Options{BaseURL: platform.URL, ProjectID: "project", Token: "synthetic-token", MaxRetries: -1})
	if err != nil {
		t.Fatal(err)
	}
	s.namespaces = engine.StaticNamespaces{DefaultBackend: b}
	for _, limit := range []int{5, 2} {
		w := call(s, "GET", "/api/v1/clusters/prod/members/prodm0/logs?process=agent&lines="+strconv.Itoa(limit), "", token)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
		for _, secret := range []string{"private-value", "closing-value", "older unselected diagnostic"} {
			if strings.Contains(w.Body.String(), secret) {
				t.Fatalf("descending tail leaked %q", secret)
			}
		}
		var logs api.MemberLogs
		if err := json.Unmarshal(w.Body.Bytes(), &logs); err != nil {
			t.Fatal(err)
		}
		n := len(logs.Lines)
		if n < 2 || n > limit || logs.Lines[n-2].Message != "safe first" || logs.Lines[n-1].Message != "safe second" {
			t.Fatalf("newest selection or equal-timestamp source order lost: %+v", logs)
		}
	}
}

func TestBuiltInProviderAgentLogs(t *testing.T) {
	s, _, token := testServer(t)
	var err error
	s.Registry, err = engine.NewRegistry(engineProviders(engineOptions{Store: s.Store, Bindings: s.postgresBindings})...)
	if err != nil {
		t.Fatal(err)
	}
	b := &memberLogsBackend{supported: true}
	s.namespaces = engine.StaticNamespaces{DefaultBackend: b}
	for _, info := range s.Registry.List() {
		t.Run(info.Name, func(t *testing.T) {
			p, err := s.Registry.Get(info.Name)
			if err != nil {
				t.Fatal(err)
			}
			name := "log" + info.Name[:min(len(info.Name), 9)]
			member := name + "m0"
			c := domain.Cluster{Name: name, Spec: domain.ClusterSpec{Provider: info.Name}, Status: domain.ClusterStatus{Members: []domain.MemberStatus{{Name: member}}}}
			p.ApplyDefaults(&c.Spec)
			if err := s.Store.CreateCluster(t.Context(), &c); err != nil {
				t.Fatal(err)
			}
			before := b.calls
			w := call(s, "GET", "/api/v1/clusters/"+c.Name+"/members/"+member+"/logs?process=agent", "", token)
			if w.Code != 200 || b.calls != before+1 || b.member != member || b.query.Limit != 100 {
				t.Fatalf("status=%d backend calls=%d member=%q: %s", w.Code, b.calls-before, b.member, w.Body.String())
			}
		})
	}
}

type retainedProvider struct {
	fakeProvider
	calls  int
	err    error
	backup domain.Backup
}

func (p *retainedProvider) DeleteRetainedBackup(_ context.Context, _ domain.BackupStorage, b domain.Backup) error {
	p.calls++
	p.backup = b
	return p.err
}

func TestRetainedBackupProviderDispatch(t *testing.T) {
	s, _, token := testServer(t)
	provider := &retainedProvider{err: errors.New("storage unavailable")}
	var err error
	s.Registry, err = engine.NewRegistry(provider)
	if err != nil {
		t.Fatal(err)
	}
	storage := domain.BackupStorage{Name: "archive", Type: "s3", Bucket: "bucket", EndpointURL: "https://s3.example.com", Region: "us-east-1"}
	if err := s.Store.CreateBackupStorage(t.Context(), &storage); err != nil {
		t.Fatal(err)
	}
	backup := domain.Backup{Name: "saved", Cluster: "deleted", Storage: "archive", Provider: "fake", Location: "snapshot-id", Type: "snapshot", Phase: domain.PhaseSucceeded}
	if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
		t.Fatal(err)
	}
	if failure := s.retainedRestoreSource(t.Context(), domain.ClusterRef{Name: "deleted"}, "saved"); failure != nil {
		t.Fatal(failure)
	}
	path := "/api/v1/clusters/deleted/backups/saved"
	w := call(s, "DELETE", path, "", token)
	if w.Code != 500 || provider.calls != 1 || provider.backup.Location != "snapshot-id" {
		t.Fatal(w.Code, w.Body.String(), provider)
	}
	persisted, err := s.Store.GetBackup(t.Context(), domain.ClusterRef{Name: "deleted"}, "saved")
	if err != nil || persisted.Phase != domain.PhaseDeleting {
		t.Fatal(persisted, err)
	}
	provider.err = nil
	w = call(s, "DELETE", path, "", token)
	if w.Code != 204 || provider.calls != 2 {
		t.Fatal(w.Code, w.Body.String(), provider.calls)
	}
	if _, err := s.Store.GetBackup(t.Context(), domain.ClusterRef{Name: "deleted"}, "saved"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal(err)
	}
}

func TestRestoreSourceAdmissionChecksProviderAndRabbitMQFormat(t *testing.T) {
	for _, provider := range []string{"rabbitmq", "psmdb"} {
		for _, source := range []string{"valid", "PITR", "foreign provider", "incompatible type", "pending backup", "missing location"} {
			t.Run(provider+"/"+source, func(t *testing.T) {
				s, _, token := testServer(t)
				rmq := rabbitmq.New(rabbitmq.Options{Store: s.Store})
				mongo := psmdb.New(psmdb.Options{Store: s.Store})
				var err error
				s.Registry, err = engine.NewRegistry(&fakeProvider{}, rmq, mongo)
				if err != nil {
					t.Fatal(err)
				}
				p := engine.Provider(rmq)
				if provider == "psmdb" {
					p = mongo
				}
				c := domain.Cluster{Name: "broker"}
				p.ApplyDefaults(&c.Spec)
				if err := s.Store.CreateCluster(t.Context(), &c); err != nil {
					t.Fatal(err)
				}
				backup := domain.Backup{Namespace: c.Namespace, Name: "saved", Cluster: c.Name, Provider: provider, Type: "definitions", Phase: domain.PhaseSucceeded, Location: "saved.json.gz"}
				if provider == "psmdb" {
					backup.Type = "physical"
				}
				switch source {
				case "foreign provider":
					backup.Provider = "other"
				case "incompatible type":
					backup.Type = "unsupported"
				case "pending backup":
					backup.Phase = domain.PhasePending
				case "missing location":
					backup.Location = ""
				}
				if err := s.Store.CreateBackup(t.Context(), &backup); err != nil {
					t.Fatal(err)
				}
				body := `{"name":"recover","source":{"sourceCluster":"broker","backup":"saved"}}`
				if source == "PITR" {
					body = `{"name":"recover","source":{"sourceCluster":"broker","pitrTime":"2026-10-12T12:00:00Z"}}`
				}
				want := 202
				if source == "foreign provider" || provider == "rabbitmq" && source != "valid" {
					want = 400
				}
				if provider == "psmdb" && source == "PITR" {
					want = 409
				}
				w := call(s, "POST", "/api/v1/clusters/broker/restores", body, token)
				if w.Code != want {
					t.Fatalf("status=%d want=%d body=%s", w.Code, want, w.Body.String())
				}
				if provider == "rabbitmq" && source == "PITR" && !strings.Contains(w.Body.String(), "RabbitMQ does not support PITR") {
					t.Fatal(w.Body.String())
				}
				restores, err := s.Store.ListRestores(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				operations, err := s.Store.ListOperations(t.Context(), c.Ref())
				if err != nil {
					t.Fatal(err)
				}
				count := 0
				if want == 202 {
					count = 1
				}
				if len(restores) != count || len(operations) != count {
					t.Fatalf("restore admission persisted unexpected work: restores=%v operations=%v", restores, operations)
				}
			})
		}
	}
}

func TestRabbitMQProviderCatalogConfigurationMetadata(t *testing.T) {
	s, _, token := testServer(t)
	provider := rabbitmq.New(rabbitmq.Options{})
	var err error
	s.Registry, err = engine.NewRegistry(provider)
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "GET", "/api/v1/providers/rabbitmq", "", token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var got api.Provider
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	if got.EngineOptions == nil || got.EngineConfig == nil {
		t.Fatalf("missing configuration metadata: %s", w.Body.String())
	}
	info := provider.Describe()
	for name, pair := range map[string][2]any{
		"engineOptions": {*got.EngineOptions, info.EngineOptions},
		"engineConfig":  {*got.EngineConfig, info.EngineConfig},
	} {
		raw, err := json.Marshal(pair[1])
		if err != nil {
			t.Fatal(err)
		}
		var expected map[string]any
		if err := json.Unmarshal(raw, &expected); err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(pair[0], expected) {
			t.Errorf("%s metadata lost during HTTP mapping: got %#v, want %#v", name, pair[0], expected)
		}
	}
	if providerAPI(psmdb.New(psmdb.Options{})).EngineOptions != nil {
		t.Fatal("provider-specific metadata leaked to another engine")
	}
}
