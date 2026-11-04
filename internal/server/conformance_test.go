// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/apispec"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine/postgres"
	"github.com/KarakoramDev/Karakoram/pkg/client"
	"github.com/getkin/kin-openapi/openapi3"
	"github.com/getkin/kin-openapi/openapi3filter"
	"github.com/getkin/kin-openapi/routers/gorillamux"
)

func init() {
	// SSE is text; the YAML download is an OpenAPI document object.
	openapi3filter.RegisterBodyDecoder("text/event-stream", openapi3filter.PlainBodyDecoder)
	openapi3filter.RegisterBodyDecoder("application/yaml", openapi3filter.YamlBodyDecoder)
}

// Every public handler used by this package's fixtures goes through the same
// response validator, including cookie requests and real streaming HTTP servers.
var conformanceTests sync.Map // *Server -> *testing.T

func registerConformance(t *testing.T, s *Server) {
	conformanceTests.Store(s, t)
	t.Cleanup(func() { conformanceTests.Delete(s) })
}

func testHandler(s *Server) http.Handler {
	return conformingHandler(s, s.Handler())
}

func testStream(s *Server, ctx context.Context, w http.ResponseWriter) error {
	var streamErr error
	conformingHandler(s, http.HandlerFunc(func(out http.ResponseWriter, _ *http.Request) {
		streamErr = s.stream(ctx).serve(out)
	})).ServeHTTP(w, request(ctx))
	return streamErr
}

func conformingHandler(s *Server, handler http.Handler) http.Handler {
	owner, ok := conformanceTests.Load(s)
	if !ok {
		panic("server fixture has no conformance test owner")
	}
	t := owner.(*testing.T)
	doc, err := apispec.Load()
	if err != nil {
		t.Fatal(err)
	}
	router, err := gorillamux.NewRouter(doc)
	if err != nil {
		t.Fatal(err)
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		recorded := &conformanceWriter{ResponseWriter: w}
		handler.ServeHTTP(recorded, r)
		route, params, err := router.FindRoute(r)
		if err != nil {
			// SPA/relay routes and intentionally nonexistent API paths are not OpenAPI
			// operations. Their behavior is asserted by the calling regression tests.
			return
		}
		status := recorded.status
		if status == 0 {
			status = http.StatusOK
		}
		input := &openapi3filter.ResponseValidationInput{
			RequestValidationInput: &openapi3filter.RequestValidationInput{Request: r, PathParams: params, Route: route},
			Status:                 status, Header: recorded.Header(), Body: http.NoBody,
			Options: &openapi3filter.Options{IncludeResponseStatus: true, MultiError: true},
		}
		input.SetBodyBytes(recorded.body.Bytes())
		if err := openapi3filter.ValidateResponse(context.Background(), input); err != nil {
			t.Errorf("OpenAPI response %s (%s %s), status %d: %v", route.Operation.OperationID, r.Method, r.URL.Path, status, err)
		}
		if route.Operation.OperationID == "streamEvents" && status == http.StatusOK {
			for _, line := range bytes.Split(recorded.body.Bytes(), []byte("\n")) {
				if !bytes.HasPrefix(line, []byte("data:")) {
					continue
				}
				var event any
				if err := json.Unmarshal(bytes.TrimSpace(bytes.TrimPrefix(line, []byte("data:"))), &event); err != nil {
					t.Errorf("OpenAPI response streamEvents /data: invalid JSON: %v", err)
					continue
				}
				if err := doc.Components.Schemas["Event"].Value.VisitJSON(event, openapi3.MultiErrors(), openapi3.VisitAsResponse()); err != nil {
					t.Errorf("OpenAPI response streamEvents /data: %v", err)
				}
			}
		}
	})
}

type conformanceWriter struct {
	http.ResponseWriter
	status int
	body   bytes.Buffer
}

func (w *conformanceWriter) WriteHeader(status int) {
	if w.status != 0 {
		return
	}
	w.status = status
	w.ResponseWriter.WriteHeader(status)
}
func (w *conformanceWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	n, err := w.ResponseWriter.Write(p)
	w.body.Write(p[:n])
	return n, err
}
func (w *conformanceWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }
func (w *conformanceWriter) Flush() {
	if w.status == 0 {
		w.WriteHeader(http.StatusOK)
	}
	_ = http.NewResponseController(w.ResponseWriter).Flush()
}

func TestConformanceObservedMemberResourcesAndScheduleStorage(t *testing.T) {
	s, _, token := testServer(t)
	ctx := t.Context()
	c, err := s.Store.GetCluster(ctx, domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	// The runtime root disk is independent of the desired persistent data volume.
	c.Status.Members[0].Autoscaling = &domain.Resources{
		CPUMode: "SHARED", MinCPU: 1, MaxCPU: 2, MinRAM: 0.25, MaxRAM: 1,
		MinDisk: 1, MaxDisk: 3,
	}
	c.Status.Members[0].Resources = &domain.ObservedResources{CPUCores: 1, MemoryGB: 0.25, DiskGB: 1}
	if err := s.Store.UpdateClusterState(ctx, c); err != nil {
		t.Fatal(err)
	}
	c.Spec.Backup = domain.BackupConfig{
		Storage: "archive",
		Schedules: []domain.BackupSchedule{
			{Name: "daily", Cron: "0 0 * * *", Type: "logical", Storage: "archive", RetainCount: 2, Enabled: true},
			{Name: "weekly", Cron: "0 0 * * 0", Type: "logical", RetainCount: 2, Enabled: true},
		},
	}
	if err := s.Store.UpdateClusterSpec(ctx, c.Ref(), c.Spec); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/v1/clusters/prod", "/api/v1/clusters", "/api/v1/clusters/prod/members"} {
		// call uses the shared OpenAPI response-conformance middleware.
		w := call(s, "GET", path, "", token)
		if w.Code != http.StatusOK {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
		if !bytes.Contains(w.Body.Bytes(), []byte(`"minDisk":1`)) || !bytes.Contains(w.Body.Bytes(), []byte(`"maxDisk":3`)) {
			t.Fatalf("%s: observed runtime disk bounds were not preserved: %s", path, w.Body.String())
		}
	}
	doc, err := apispec.Load()
	if err != nil {
		t.Fatal(err)
	}
	// A primitive wrapped in allOf is valid OpenAPI but some generators model
	// it as an object. Keep optional schedule storage a plain string.
	storage := doc.Components.Schemas["BackupSchedule"].Value.Properties["storage"].Value
	if !storage.Type.Is("string") || len(storage.AllOf) != 0 || storage.Nullable {
		t.Fatal("schedule storage must be an optional, non-nullable plain string")
	}
	// These schemas cover both runtime-only engines and persistent data engines.
	// The shared floor must allow 1 GiB; higher persistent floors belong to
	// provider validation, not to the runtime observation or shared input schema.
	for _, schema := range []string{"ResourcesInput", "ResourcesPatch"} {
		for _, field := range []string{"minDisk", "maxDisk"} {
			bound := doc.Components.Schemas[schema].Value.Properties[field].Value.Min
			if bound == nil || *bound != 1 {
				t.Fatalf("%s.%s must retain the shared 1-GiB request minimum", schema, field)
			}
		}
	}
	p := postgres.New(postgres.Options{})
	data := &domain.Cluster{Name: "pgfloor"}
	p.ApplyDefaults(&data.Spec)
	if err := p.Validate(ctx, nil, data); err != nil {
		t.Fatalf("default PostgreSQL data resources rejected: %v", err)
	}
	data.Spec.Components.Engine.Resources.MinDisk = 1
	data.Spec.Components.Engine.Resources.MaxDisk = 1
	if err := p.Validate(ctx, nil, data); err == nil {
		t.Fatal("PostgreSQL provider lost its 5-GiB persistent-volume minimum")
	}
}

func TestConformancePostRestoreBackupTrigger(t *testing.T) {
	s, _, token := testServer(t)
	backup := &domain.Backup{
		Name: "restore-base", Cluster: "prod", Storage: "archive",
		Type: "logical", Trigger: "restore", Phase: domain.PhaseSucceeded,
	}
	if err := s.Store.CreateBackup(t.Context(), backup); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{"/api/v1/clusters/prod/backups", "/api/v1/clusters/prod/backups/restore-base"} {
		w := call(s, "GET", path, "", token)
		if w.Code != http.StatusOK || !bytes.Contains(w.Body.Bytes(), []byte(`"trigger":"restore"`)) {
			t.Fatalf("%s: %d %s", path, w.Code, w.Body.String())
		}
	}
}

func TestGeneratedEnumIdentifiersRemainStable(t *testing.T) {
	// Refer to the exported names in both generated packages so a schema change
	// cannot silently rename existing constants through enum collision handling.
	for _, pair := range [][2]string{
		{string(api.Manual), "manual"},
		{string(api.Schedule), "schedule"},
		{string(api.BackupTriggerRestore), "restore"},
		{string(api.CanRequestActionAsterisk), "*"},
		{string(api.CanRequestActionCreate), "create"},
		{string(api.CanRequestActionRead), "read"},
		{string(api.CanRequestActionUpdate), "update"},
		{string(api.CanRequestActionDelete), "delete"},
		{string(api.CanRequestActionExport), "export"},
		{string(client.Manual), "manual"},
		{string(client.Schedule), "schedule"},
		{string(client.BackupTriggerRestore), "restore"},
		{string(client.CanRequestActionAsterisk), "*"},
		{string(client.CanRequestActionCreate), "create"},
		{string(client.CanRequestActionRead), "read"},
		{string(client.CanRequestActionUpdate), "update"},
		{string(client.CanRequestActionDelete), "delete"},
		{string(client.CanRequestActionExport), "export"},
	} {
		if pair[0] != pair[1] {
			t.Errorf("generated enum value = %q, want %q", pair[0], pair[1])
		}
	}
}
