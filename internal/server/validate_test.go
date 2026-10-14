// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/apispec"
	"github.com/KarakoramDev/Karakoram/internal/cli"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/psmdb"
	"github.com/KarakoramDev/Karakoram/pkg/client"
	"github.com/getkin/kin-openapi/openapi3"
)

func TestRequestValidation(t *testing.T) {
	s, _, token := testServer(t)
	for _, tc := range []struct {
		name, method, path, body, kind, field, reason string
		status                                        int
	}{
		{"unknown envelope", "POST", "/api/v1/clusters", `{"name":"sample","spec":{"provider":"fake"},"unexpected":true}`, "application/json", "unexpected", "unknown_field", 400},
		{"unknown spec", "POST", "/api/v1/clusters", `{"name":"sample","spec":{"provider":"fake","mystery":true}}`, "application/json", "spec.mystery", "unknown_field", 400},
		{"nested typo", "POST", "/api/v1/clusters", `{"name":"sample","spec":{"provider":"fake","components":{"engine":{"replcias":3}}}}`, "application/json", "spec.components.engine.replcias", "unknown_field", 400},
		{"wrong replica type", "POST", "/api/v1/clusters", `{"name":"sample","spec":{"provider":"fake","components":{"engine":{"replicas":"3"}}}}`, "application/json", "spec.components.engine.replicas", "invalid", 400},
		{"wrong config type", "POST", "/api/v1/clusters", `{"name":"sample","spec":{"provider":"fake","engineConfig":{"a":1}}}`, "application/json", "spec.engineConfig", "invalid", 400},
		{"missing provider", "POST", "/api/v1/clusters", `{"name":"sample","spec":{}}`, "application/json", "spec.provider", "required", 400},
		{"missing body", "POST", "/api/v1/clusters", "", "application/json", "body", "required", 400},
		{"missing body and media", "POST", "/api/v1/clusters", "", "", "body", "required", 400},
		{"malformed", "POST", "/api/v1/clusters", `{"name":`, "application/json", "body", "invalid", 400},
		{"trailing garbage", "POST", "/api/v1/clusters", `{"name":"sample","spec":{"provider":"fake"}} garbage`, "application/json", "body", "invalid", 400},
		{"second value", "POST", "/api/v1/clusters", `{"name":"sample","spec":{"provider":"fake"}} {}`, "application/json", "body", "invalid", 400},
		{"plain media", "POST", "/api/v1/clusters", `{}`, "text/plain", "Content-Type", "unsupported", 415},
		{"missing media", "POST", "/api/v1/api-tokens", `{"name":"example"}`, "", "Content-Type", "unsupported", 415},
		{"replicas zero", "POST", "/api/v1/clusters", `{"name":"sample","spec":{"provider":"fake","components":{"engine":{"replicas":0}}}}`, "application/json", "spec.components.engine.replicas", "out_of_range", 400},
		{"patch replicas zero", "PATCH", "/api/v1/clusters/prod", `{"spec":{"components":{"engine":{"replicas":0}}}}`, "application/merge-patch+json", "spec.components.engine.replicas", "out_of_range", 400},
		{"patch CPU zero", "PATCH", "/api/v1/clusters/prod", `{"spec":{"components":{"engine":{"resources":{"maxCpu":0}}}}}`, "application/json", "spec.components.engine.resources.maxCpu", "out_of_range", 400},
		{"config server count", "PATCH", "/api/v1/clusters/prod", `{"spec":{"components":{"configServer":{"replicas":2}}}}`, "application/json", "spec.components.configServer.replicas", "out_of_range", 400},
		{"retain zero", "PATCH", "/api/v1/clusters/prod", `{"spec":{"backup":{"schedules":[{"name":"daily","cron":"0 0 * * *","type":"logical","retainCount":0,"enabled":true}]}}}`, "application/json", "spec.backup.schedules.0.retainCount", "out_of_range", 400},
		{"retain negative", "PATCH", "/api/v1/clusters/prod", `{"spec":{"backup":{"schedules":[{"name":"daily","cron":"0 0 * * *","type":"logical","retainCount":-1,"enabled":true}]}}}`, "application/json", "spec.backup.schedules.0.retainCount", "out_of_range", 400},
		{"shards zero", "PATCH", "/api/v1/clusters/prod", `{"spec":{"topology":{"shards":0}}}`, "application/json", "spec.topology.shards", "out_of_range", 400},
		{"RAM minimum", "PATCH", "/api/v1/clusters/prod", `{"spec":{"components":{"engine":{"resources":{"minRam":0.25}}}}}`, "application/json", "spec.components.engine.resources.minRam", "out_of_range", 400},
		{"disk minimum", "PATCH", "/api/v1/clusters/prod", `{"spec":{"components":{"engine":{"resources":{"minDisk":1}}}}}`, "application/json", "spec.components.engine.resources.minDisk", "out_of_range", 400},
		{"patch unknown", "PATCH", "/api/v1/clusters/prod", `{"spec":{"components":{"engine":{"replcias":3}}}}`, "application/json", "spec.components.engine.replcias", "unknown_field", 400},
		{"patch server owned", "PATCH", "/api/v1/clusters/prod", `{"spec":{},"status":{}}`, "application/json", "status", "unknown_field", 400},
		{"bad enum", "PATCH", "/api/v1/clusters/prod", `{"spec":{"tls":{"mode":"something"}}}`, "application/json", "spec.tls.mode", "unsupported", 400},
		{"bad name", "POST", "/api/v1/clusters", `{"name":"Bad_Name","spec":{"provider":"fake"}}`, "application/json", "name", "invalid", 400},
		{"limit zero", "GET", "/api/v1/clusters?limit=0", "", "application/json", "limit", "out_of_range", 400},
		{"limit too high", "GET", "/api/v1/events?limit=501", "", "application/json", "limit", "out_of_range", 400},
		{"limit type", "GET", "/api/v1/operations?limit=nope", "", "application/json", "limit", "invalid", 400},
		{"cursor", "GET", "/api/v1/events?cursor=not-a-cursor", "", "application/json", "cursor", "invalid", 400},
		{"cluster cursor", "GET", "/api/v1/clusters?cursor=garbage", "", "application/json", "cursor", "invalid", 400},
		{"operation cursor", "GET", "/api/v1/operations?cursor=garbage", "", "application/json", "cursor", "invalid", 400},
		{"order", "GET", "/api/v1/events?order=sideways", "", "application/json", "order", "unsupported", 400},
		{"range", "GET", "/api/v1/clusters/prod/metrics?range=7d", "", "application/json", "range", "out_of_range", 400},
		{"zero range", "GET", "/api/v1/clusters/prod/metrics?range=0s", "", "application/json", "range", "out_of_range", 400},
		{"step", "GET", "/api/v1/clusters/prod/metrics?range=1h&step=2h", "", "application/json", "step", "out_of_range", 400},
		{"expiry format", "POST", "/api/v1/api-tokens", `{"name":"example","expiresAt":"nope"}`, "application/json", "expiresAt", "invalid", 400},
		{"all IPv4", "PATCH", "/api/v1/clusters/prod", `{"spec":{"exposure":{"public":true,"ipType":"IPV4","allowlist":["0.0.0.0/0"]}}}`, "application/json", "spec.exposure.allowlist.0", "required", 400},
		{"family", "PATCH", "/api/v1/clusters/prod", `{"spec":{"exposure":{"public":true,"ipType":"IPV6","allowlist":["0.0.0.0/0"],"allowAllAddresses":true}}}`, "application/json", "spec.exposure.allowlist.0", "invalid", 400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, strings.NewReader(tc.body))
			if tc.kind != "" {
				r.Header.Set("Content-Type", tc.kind)
			}
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			testHandler(s).ServeHTTP(w, r)
			var e api.Error
			if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
				t.Fatal(err, w.Body.String())
			}
			if w.Code != tc.status || e.Details == nil || (*e.Details)["field"] != tc.field || (*e.Details)["reason"] != tc.reason || !strings.Contains(e.Message, tc.field) {
				t.Fatalf("got %d %s", w.Code, w.Body.String())
			}
			if tc.field == "spec.provider" && !strings.Contains(e.Message, "GET /api/v1/providers") {
				t.Fatal(e.Message)
			}
		})
	}
}

func TestMethodsAndRequestIDs(t *testing.T) {
	s, _, token := testServer(t)
	for _, tc := range []struct {
		method, path string
		status       int
		allow        string
	}{
		{"PUT", "/api/v1/clusters", 405, "GET, HEAD, POST"}, {"POST", "/api/v1/providers", 405, "GET, HEAD"},
		{"PUT", "/api/v1/clusters/prod/status", 405, "GET, HEAD"}, {"POST", "/healthz", 405, "GET, HEAD"},
		{"POST", "/metrics", 405, "GET, HEAD"}, {"GET", "/api/v1/unknown", 404, ""}, {"GET", "/api/v1/version", 200, ""},
		{"GET", "/api/v1/clusters/%70rod", 200, ""},
		{"GET", "/api/v1/events?actor=&resource=", 200, ""},
	} {
		t.Run(tc.method+tc.path, func(t *testing.T) {
			r := httptest.NewRequest(tc.method, tc.path, nil)
			r.Header.Set("X-Request-ID", "example-001:a.b_c")
			r.Header.Set("Authorization", "Bearer "+token)
			w := httptest.NewRecorder()
			testHandler(s).ServeHTTP(w, r)
			if w.Code != tc.status || w.Header().Get("Allow") != tc.allow || w.Header().Get("X-Request-ID") != "example-001:a.b_c" {
				t.Fatal(w.Code, w.Header(), w.Body.String())
			}
			if tc.status == 405 && !strings.Contains(w.Body.String(), `"code":"method_not_allowed"`) {
				t.Fatal(w.Body.String())
			}
		})
	}
	for _, id := range []string{"", strings.Repeat("x", 129), "contains spaces", "a/b"} {
		r := httptest.NewRequest("GET", "/api/v1/version", nil)
		r.Header.Set("X-Request-ID", id)
		w := httptest.NewRecorder()
		testHandler(s).ServeHTTP(w, r)
		got := w.Header().Get("X-Request-ID")
		if !saneRequestID.MatchString(got) || got == id {
			t.Fatal(got)
		}
	}
}

func TestBodySizeLimit(t *testing.T) {
	s, _, token := testServer(t)
	for _, chunked := range []bool{false, true} {
		r := httptest.NewRequest("POST", "/api/v1/api-tokens", strings.NewReader(`{"name":"`+strings.Repeat("x", 2<<20)+`"}`))
		r.Header.Set("Authorization", "Bearer "+token)
		r.Header.Set("Content-Type", "application/json")
		if chunked {
			r.ContentLength = -1
		}
		w := httptest.NewRecorder()
		testHandler(s).ServeHTTP(w, r)
		if w.Code != 413 || !strings.Contains(w.Body.String(), `"code":"payload_too_large"`) {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestDefaultsRoundTripAndExposureAcknowledgement(t *testing.T) {
	s, _, token := testServer(t)
	w := call(s, "POST", "/api/v1/clusters", `{"name":"sample","spec":{"provider":"fake"}}`, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	var c api.Cluster
	if err := json.Unmarshal(w.Body.Bytes(), &c); err != nil {
		t.Fatal(err)
	}
	if c.Spec.Components.Engine.Replicas != 3 {
		t.Fatal(c.Spec)
	}
	for _, kind := range []string{"application/json", "application/merge-patch+json"} {
		r := httptest.NewRequest("PATCH", "/api/v1/clusters/sample", strings.NewReader(`{"spec":{"exposure":{"public":true,"ipType":"IPV4","allowlist":["0.0.0.0/0"],"allowAllAddresses":true}}}`))
		r.Header.Set("Content-Type", kind)
		r.Header.Set("Authorization", "Bearer "+token)
		w = httptest.NewRecorder()
		testHandler(s).ServeHTTP(w, r)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	w = call(s, "GET", "/api/v1/clusters/sample", "", token)
	body := w.Body.String()
	w = call(s, "PUT", "/api/v1/clusters/sample", body, token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	var envelope map[string]any
	json.Unmarshal([]byte(body), &envelope)
	specBody, _ := json.Marshal(map[string]any{"spec": envelope["spec"]})
	w = call(s, "PUT", "/api/v1/clusters/sample", string(specBody), token)
	if w.Code != 200 {
		t.Fatal("CLI spec edit", w.Code, w.Body.String())
	}
	stored, err := s.Store.GetCluster(context.Background(), domain.ClusterRef{Namespace: "default", Name: "sample"})
	if err != nil || !stored.Spec.Exposure.AllowAllAddresses {
		t.Fatal(stored, err)
	}
}

func TestLegacyExposureUnrelatedUpdates(t *testing.T) {
	for _, exposure := range []domain.Exposure{
		{Public: true, IPType: "IPV4", Allowlist: []string{"0.0.0.0/0"}},
		{Public: true, IPType: "IPV6", Allowlist: []string{"::/0"}},
		{Public: true, IPType: "IPV6", Allowlist: []string{"192.0.2.0/24"}},
	} {
		t.Run(exposure.IPType+exposure.Allowlist[0], func(t *testing.T) {
			s, _, token := testServer(t)
			ref := domain.ClusterRef{Namespace: "default", Name: "prod"}
			c, err := s.Store.GetCluster(t.Context(), ref)
			if err != nil {
				t.Fatal(err)
			}
			c.Spec.Exposure = exposure
			if err := s.Store.UpdateClusterSpec(t.Context(), ref, c.Spec); err != nil {
				t.Fatal(err)
			}
			w := call(s, "PATCH", "/api/v1/clusters/prod", `{"spec":{"version":"8.0.1"}}`, token)
			if w.Code != 200 {
				t.Fatalf("version-only update: %d %s", w.Code, w.Body.String())
			}
			c, err = s.Store.GetCluster(t.Context(), ref)
			if err != nil || c.Spec.Version != "8.0.1" || c.Spec.Exposure.Allowlist[0] != exposure.Allowlist[0] {
				t.Fatal(c, err)
			}
			c.Spec.Components.Engine.Replicas = 5
			body, err := json.Marshal(map[string]any{"spec": specAPI(c.Spec)})
			if err != nil {
				t.Fatal(err)
			}
			w = call(s, "PUT", "/api/v1/clusters/prod", string(body), token)
			if w.Code != 200 {
				t.Fatalf("scaling replacement: %d %s", w.Code, w.Body.String())
			}
			w = call(s, "PATCH", "/api/v1/clusters/prod", `{"spec":{"exposure":{"public":true,"ipType":"IPV4","allowlist":["0.0.0.0/0","192.0.2.0/24"]}}}`, token)
			if w.Code != 400 || !strings.Contains(w.Body.String(), "allowAllAddresses") {
				t.Fatalf("changed allowlist needs acknowledgement: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

func TestSplitAllowlistRequiresAcknowledgement(t *testing.T) {
	for _, allowlist := range []string{`["0.0.0.0/1","128.0.0.0/1"]`, `["::/1","8000::/1"]`} {
		s, _, token := testServer(t)
		ipType := "IPV4"
		if strings.Contains(allowlist, ":") {
			ipType = "IPV6"
		}
		body := `{"spec":{"exposure":{"public":true,"ipType":"` + ipType + `","allowlist":` + allowlist + `}}}`
		w := call(s, "PATCH", "/api/v1/clusters/prod", body, token)
		if w.Code != 400 || !strings.Contains(w.Body.String(), "allowAllAddresses") {
			t.Fatal(w.Code, w.Body.String())
		}
		body = `{"spec":{"exposure":{"public":true,"ipType":"` + ipType + `","allowlist":` + allowlist + `,"allowAllAddresses":true}}}`
		w = call(s, "PATCH", "/api/v1/clusters/prod", body, token)
		if w.Code != 200 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestClosedSchemasAndCompositions(t *testing.T) {
	doc, err := apispec.Load()
	if err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"CreateClusterRequest", "ReplaceClusterRequest", "ClusterMergePatch", "ClusterInputSpec", "ClusterPatchSpec", "ResourcesInput", "ResourcesPatch", "ComponentsInput", "ComponentsPatch"} {
		s := doc.Components.Schemas[name].Value
		if s.AdditionalProperties.Has == nil || *s.AdditionalProperties.Has {
			t.Fatalf("%s is not closed", name)
		}
	}
	// allOf references used by namespace properties continue to validate.
	schema := doc.Components.Schemas["DataSource"].Value
	err = schema.VisitJSON(map[string]any{"namespace": "Bad_Name", "cluster": "sample", "backup": "base"})
	if err == nil {
		t.Fatal("allOf pattern ignored")
	}
	// Closing oneOf alternatives must retain field paths through composition errors.
	branch := openapi3.NewObjectSchema().WithProperty("name", openapi3.NewStringSchema()).WithRequired([]string{"name"})
	no := false
	branch.AdditionalProperties.Has = &no
	composed := &openapi3.Schema{OneOf: openapi3.SchemaRefs{{Value: branch}}}
	err = composed.VisitJSON(map[string]any{"name": "example", "typo": true})
	if err == nil {
		t.Fatal("oneOf unknown field accepted")
	}
	rejected := schemaFailure(err).body.(api.Error)
	if rejected.Details == nil || (*rejected.Details)["field"] != "typo" || (*rejected.Details)["reason"] != "unknown_field" {
		t.Fatal(rejected)
	}
}

func TestEveryExplicitZeroResourceRejected(t *testing.T) {
	s, _, token := testServer(t)
	for _, field := range []string{"minCpu", "maxCpu", "minRam", "maxRam", "minDisk", "maxDisk", "startCpu"} {
		t.Run(field, func(t *testing.T) {
			body := `{"spec":{"components":{"engine":{"resources":{"` + field + `":0}}}}}`
			w := call(s, "PATCH", "/api/v1/clusters/prod", body, token)
			var e api.Error
			if err := json.Unmarshal(w.Body.Bytes(), &e); err != nil {
				t.Fatal(err)
			}
			if w.Code != 400 || e.Details == nil || (*e.Details)["field"] != "spec.components.engine.resources."+field || (*e.Details)["reason"] != "out_of_range" {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
}

func TestCLIYAMLUnknownFieldsReachValidation(t *testing.T) {
	s, _, token := testServer(t)
	server := httptest.NewServer(testHandler(s))
	defer server.Close()
	for _, yaml := range []string{
		"name: sample\nspec:\n  provider: fake\nunexpectedField: misspelled-option\n",
		"name: sample\nspec:\n  provider: fake\n  components:\n    engine:\n      replcias: 3\n",
	} {
		cmd := cli.NewCommand(cli.Options{ConfigPath: filepath.Join(t.TempDir(), "config.yaml"), In: strings.NewReader(yaml), Out: io.Discard, Err: io.Discard})
		cmd.SetArgs([]string{"--server", server.URL, "--token", token, "clusters", "create", "-f", "-"})
		err := cmd.ExecuteContext(t.Context())
		if err == nil || (!strings.Contains(err.Error(), "unexpectedField") && !strings.Contains(err.Error(), "spec.components.engine.replcias")) {
			t.Fatal(err)
		}
	}
}

func TestGeneratedClientPaginationDiagnostics(t *testing.T) {
	s, _, token := testServer(t)
	server := httptest.NewServer(testHandler(s))
	defer server.Close()
	c, err := client.NewClientWithResponses(server.URL+"/api/v1", client.WithRequestEditorFn(func(ctx context.Context, r *http.Request) error {
		r.Header.Set("Authorization", "Bearer "+token)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		params client.ListEventsParams
		field  string
	}{{client.ListEventsParams{Limit: ptr(0)}, "limit"}, {client.ListEventsParams{Limit: ptr(501)}, "limit"}, {client.ListEventsParams{Cursor: ptr("invalid-cursor")}, "cursor"}} {
		res, err := c.ListEventsWithResponse(t.Context(), &tc.params)
		if err != nil || res.JSON400 == nil || res.JSON400.Details == nil || (*res.JSON400.Details)["field"] != tc.field || !strings.Contains(res.JSON400.Message, tc.field) {
			t.Fatal(res, err)
		}
		if tc.field == "limit" && !strings.Contains(res.JSON400.Message, "1 to 500") {
			t.Fatal(res.JSON400.Message)
		}
	}
}

func TestRequestIdentifierLoggedAndRepeatedHeaderReplaced(t *testing.T) {
	s, _, _ := testServer(t)
	var logs bytes.Buffer
	s.Logger = slog.New(slog.NewJSONHandler(&logs, nil))
	r := httptest.NewRequest("GET", "/api/v1/version", nil)
	r.Header.Set("X-Request-ID", "example-req-001")
	w := httptest.NewRecorder()
	testHandler(s).ServeHTTP(w, r)
	if !strings.Contains(logs.String(), `"request_id":"example-req-001"`) {
		t.Fatal(logs.String())
	}
	r = httptest.NewRequest("GET", "/api/v1/version", nil)
	r.Header.Add("X-Request-ID", "first")
	r.Header.Add("X-Request-ID", "second")
	w = httptest.NewRecorder()
	testHandler(s).ServeHTTP(w, r)
	id := w.Header().Get("X-Request-ID")
	if id == "first" || id == "second" || !saneRequestID.MatchString(id) {
		t.Fatal(id)
	}
}

func TestTypedClientOmittedFieldsAndExplicitZero(t *testing.T) {
	s, _, token := testServer(t)
	var err error
	s.Registry, err = engine.NewRegistry(&fakeProvider{}, psmdb.New(psmdb.Options{}))
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(testHandler(s))
	defer server.Close()
	c, err := client.NewClientWithResponses(server.URL+"/api/v1", client.WithRequestEditorFn(func(ctx context.Context, r *http.Request) error {
		r.Header.Set("Authorization", "Bearer "+token)
		return nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	created, err := c.CreateClusterWithResponse(t.Context(), nil, client.CreateClusterRequest{Name: "typeddef", Spec: client.ClusterInputSpec{Provider: "psmdb"}})
	if err != nil || created.JSON201 == nil {
		t.Fatal(created, err)
	}
	spec := created.JSON201.Spec
	if spec.Version != "8.0.32-14" || spec.Topology.Type != "replicaSet" || spec.Components.Engine.Replicas != 3 || spec.Components.Engine.Resources.MaxCpu != 1 || spec.Components.Engine.Resources.MinRam != 1 || spec.Components.Engine.Resources.MinDisk != 5 {
		t.Fatal(spec)
	}
	for _, tc := range []struct {
		name, field string
		component   client.ComponentInput
	}{
		{"typedzero", "spec.components.engine.replicas", client.ComponentInput{Replicas: ptr(0)}},
		{"typedcpu", "spec.components.engine.resources.maxCpu", client.ComponentInput{Resources: &client.ResourcesInput{MaxCpu: ptr(float32(0))}}},
	} {
		res, err := c.CreateClusterWithResponse(t.Context(), nil, client.CreateClusterRequest{Name: tc.name, Spec: client.ClusterInputSpec{Provider: "psmdb", Components: &client.ComponentsInput{Engine: &tc.component}}})
		if err != nil || res.JSON400 == nil || res.JSON400.Details == nil || (*res.JSON400.Details)["field"] != tc.field {
			t.Fatal(res, err)
		}
	}
}

func TestBackupConflictExplainsPendingGeneration(t *testing.T) {
	s, _, token := testServer(t)
	ref := domain.ClusterRef{Namespace: "default", Name: "prod"}
	c, err := s.Store.GetCluster(t.Context(), ref)
	if err != nil {
		t.Fatal(err)
	}
	c.Spec.Components.Engine.Replicas = 5
	if err := s.Store.UpdateClusterSpec(t.Context(), ref, c.Spec); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/clusters/prod/backups", `{"name":"beforechange","type":"logical","storage":"archive"}`, token)
	var problem api.Error
	if err := json.Unmarshal(w.Body.Bytes(), &problem); err != nil {
		t.Fatal(err)
	}
	if w.Code != 409 || !strings.Contains(problem.Message, "Ready") ||
		!strings.Contains(problem.Message, "generation 2 is pending (observed generation 1)") {
		t.Fatal(w.Code, w.Body.String())
	}
	if problem.Details == nil || (*problem.Details)["generation"] != float64(2) ||
		(*problem.Details)["observedGeneration"] != float64(1) {
		t.Fatal(problem.Details)
	}
	backups, err := s.Store.ListBackups(t.Context(), ref)
	if err != nil || len(backups) != 0 {
		t.Fatal(backups, err)
	}
}
