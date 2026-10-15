// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

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
