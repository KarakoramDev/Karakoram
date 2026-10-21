// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/KarakoramDev/Karakoram/internal/engine/supabase"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

type fakeSupabaseAccess struct {
	fakeProvider
	names     []string
	files     map[string]string
	password  string
	mutations int
	err       error
}

func (*fakeSupabaseAccess) Name() string { return "supabase" }
func (f *fakeSupabaseAccess) Describe() domain.ProviderInfo {
	info := f.fakeProvider.Describe()
	info.Name = "supabase"
	if f.info == nil {
		info.Operations = append(info.Operations, "rotate-jwt-secret")
	}
	return info
}
func (f *fakeSupabaseAccess) Credentials(context.Context, *engine.Context) (supabase.Credentials, error) {
	return supabase.Credentials{APIURL: "http://appgw:8000", StudioURL: "http://appgw:8000", AnonKey: "test-anon", ServiceRoleKey: "test-service", PublishableKey: "test-publishable", SecretKey: "test-secret", DashboardUsername: "admin", DashboardPassword: "test-dashboard", JWTSecret: "test-jwt"}, f.err
}
func (f *fakeSupabaseAccess) SetSMTPPassword(_ context.Context, _ *engine.Context, value string) error {
	f.password = value
	f.mutations++
	return f.err
}
func (f *fakeSupabaseAccess) ListFunctions(context.Context, *engine.Context) ([]string, error) {
	return f.names, f.err
}
func (f *fakeSupabaseAccess) GetFunction(context.Context, *engine.Context, string) (map[string]string, error) {
	if f.files == nil {
		return map[string]string{"index.ts": "Deno.serve(() => new Response('hello'))"}, f.err
	}
	return f.files, f.err
}
func (f *fakeSupabaseAccess) PutFunction(_ context.Context, _ *engine.Context, _ string, files map[string]string) error {
	f.files = files
	f.mutations++
	return f.err
}
func (f *fakeSupabaseAccess) DeleteFunction(context.Context, *engine.Context, string) error {
	f.mutations++
	return f.err
}

func supabaseTestServer(t *testing.T) (*Server, *fakeSupabaseAccess, string) {
	t.Helper()
	s, _, token := testServer(t)
	f := &fakeSupabaseAccess{names: []string{"hello"}}
	registry, err := engine.NewRegistry(f, &fakeProvider{})
	if err != nil {
		t.Fatal(err)
	}
	s.Registry = registry
	cluster, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	cluster.Spec.Provider = "supabase"
	if err := s.Store.UpdateClusterSpec(t.Context(), cluster.Ref(), cluster.Spec); err != nil {
		t.Fatal(err)
	}
	return s, f, token
}

func TestSupabaseHandlersAndAudit(t *testing.T) {
	s, f, token := supabaseTestServer(t)
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"GET", "credentials", "", 200},
		{"PUT", "smtp-password", `{"password":"private-smtp"}`, 204},
		{"GET", "functions", "", 200},
		{"GET", "functions/hello", "", 200},
		{"PUT", "functions/hello", `{"index.ts":"private-source","lib/util.ts":"export {}"}`, 204},
		{"DELETE", "functions/hello", "", 204},
		{"POST", "rotate-jwt-secret", "", 202},
	} {
		w := call(s, tc.method, "/api/v1/clusters/prod/supabase/"+tc.path, tc.body, token)
		if w.Code != tc.status {
			t.Fatalf("%s %s: %d %s", tc.method, tc.path, w.Code, w.Body.String())
		}
		if tc.path == "credentials" || tc.method == "GET" && strings.HasPrefix(tc.path, "functions/") {
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("sensitive response cached")
			}
		}
		if tc.method != "GET" && (strings.Contains(w.Body.String(), "private-smtp") || strings.Contains(w.Body.String(), "private-source")) {
			t.Fatal("mutation echoed secret content")
		}
	}
	if f.password != "private-smtp" || f.mutations != 3 {
		t.Fatal("provider changes not forwarded")
	}
	events, err := s.Store.ListEvents(t.Context(), store.EventFilter{Cluster: "prod", Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	data, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{"private-smtp", "private-source", "test-jwt", "test-dashboard"} {
		if strings.Contains(string(data), secret) {
			t.Fatal("audit contains secret content")
		}
	}
	for _, reason := range []string{"SupabaseSMTPPasswordUpdated", "PutSupabaseFunctionSucceeded", "DeleteSupabaseFunctionSucceeded", "SupabaseCredentialsRevealed"} {
		if !strings.Contains(string(data), reason) {
			t.Fatal("missing audit event", reason)
		}
	}
	ops, err := s.Store.ListOperations(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, op := range ops {
		if op.Kind == "rotate-jwt-secret" {
			found = true
			if string(op.Payload) != "{}" {
				t.Fatal("rotation payload contains content")
			}
		}
	}
	if !found {
		t.Fatal("rotation was not enqueued")
	}
}

func TestSupabaseFunctionValidation(t *testing.T) {
	s, f, token := supabaseTestServer(t)
	for _, path := range []string{"../index.ts", "lib/../index.ts", "/index.ts", "lib\\index.ts", "./index.ts", "lib//index.ts", "C:/index.ts", "lib/"} {
		body, _ := json.Marshal(map[string]string{path: "export {}"})
		w := call(s, "PUT", "/api/v1/clusters/prod/supabase/functions/hello", string(body), token)
		if w.Code != 400 || f.mutations != 0 {
			t.Fatalf("unsafe path %q accepted: %d", path, w.Code)
		}
	}
	for _, name := range []string{"Bad", "_bad", strings.Repeat("a", 64)} {
		w := call(s, "PUT", "/api/v1/clusters/prod/supabase/functions/"+name, `{"index.ts":"ok"}`, token)
		if w.Code != 400 {
			t.Fatal("invalid function name accepted", w.Code)
		}
	}
	for _, size := range []int{1 << 20, (1 << 20) + 1} {
		body, _ := json.Marshal(map[string]string{"index.ts": strings.Repeat("x", size)})
		w := call(s, "PUT", "/api/v1/clusters/prod/supabase/functions/hello", string(body), token)
		want := 204
		if size > 1<<20 {
			want = 400
		}
		if w.Code != want {
			t.Fatal("source size boundary", size, w.Code)
		}
	}
	f.names = make([]string, 100)
	for i := range f.names {
		f.names[i] = fmt.Sprintf("f%d", i)
	}
	w := call(s, "PUT", "/api/v1/clusters/prod/supabase/functions/new", `{"index.ts":"ok"}`, token)
	if w.Code != 400 {
		t.Fatal("101st function accepted", w.Code)
	}
	w = call(s, "PUT", "/api/v1/clusters/prod/supabase/functions/f0", `{"index.ts":"ok"}`, token)
	if w.Code != 204 {
		t.Fatal("existing function cannot be replaced at limit", w.Code)
	}
}

func TestSupabaseRBACAndProviderAdmission(t *testing.T) {
	s, f, admin := supabaseTestServer(t)
	for _, role := range []string{"viewer", "operator"} {
		if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: role, Roles: []string{"role:" + role}}, "long-test-password"); err != nil {
			t.Fatal(err)
		}
		token, _, err := s.Auth.CreateToken(t.Context(), role, "test", nil, nil)
		if err != nil {
			t.Fatal(err)
		}
		w := call(s, "GET", "/api/v1/clusters/prod/supabase/credentials", "", token)
		if w.Code != 403 {
			t.Fatal("credentials grant bypassed", role, w.Code)
		}
		if role == "viewer" {
			for _, tc := range []struct{ method, path, body string }{
				{"PUT", "smtp-password", `{"password":"private"}`},
				{"PUT", "functions/hello", `{"index.ts":"private"}`},
				{"DELETE", "functions/hello", ""},
				{"POST", "rotate-jwt-secret", ""},
			} {
				w := call(s, tc.method, "/api/v1/clusters/prod/supabase/"+tc.path, tc.body, token)
				if w.Code != 403 {
					t.Fatal("viewer changed supabase", tc.path, w.Code)
				}
			}
		}
	}
	for _, path := range []string{"credentials", "functions"} {
		w := call(s, "GET", "/api/v1/clusters/dev/supabase/"+path, "", admin)
		if w.Code != 409 {
			t.Fatal("wrong provider accepted", w.Code)
		}
	}
	info := f.Describe()
	info.Operations = nil
	f.info = &info
	w := call(s, "POST", "/api/v1/clusters/prod/supabase/rotate-jwt-secret", "", admin)
	if w.Code != 409 {
		t.Fatal("unadvertised operation accepted", w.Code)
	}
}

func TestSupabaseMutationIdempotency(t *testing.T) {
	for _, tc := range []struct {
		method, path, body string
		status             int
	}{
		{"PUT", "smtp-password", `{"password":"private-smtp"}`, 204},
		{"PUT", "functions/hello", `{"index.ts":"export {}"}`, 204},
		{"POST", "rotate-jwt-secret", "", 202},
	} {
		s, f, token := supabaseTestServer(t)
		path := "/api/v1/clusters/prod/supabase/" + tc.path
		first := idempotentCall(s, tc.method, path, tc.body, token, "supabase-request")
		f.err = engine.ErrConflict
		second := idempotentCall(s, tc.method, path, tc.body, token, "supabase-request")
		assertReplay(t, first, second, tc.status)
	}
}

func TestSupabaseFunctionJSONExpansionBodyLimit(t *testing.T) {
	s, f, token := supabaseTestServer(t)
	source := strings.Repeat("<", 1<<20)
	body, err := json.Marshal(map[string]string{"index.ts": source})
	if err != nil {
		t.Fatal(err)
	}
	if len(body) < 6<<20 {
		t.Fatal("test source did not exercise sixfold JSON escaping")
	}
	for _, idempotency := range []bool{false, true} {
		path := "/api/v1/clusters/prod/supabase/functions/hello"
		w := call(s, "PUT", path, string(body), token)
		if idempotency {
			w = idempotentCall(s, "PUT", path, string(body), token, "escaped-function")
		}
		if w.Code != 204 || f.files["index.ts"] != source {
			t.Fatal("valid source rejected by transport limit", w.Code)
		}
	}
	for _, path := range []string{"functions/hello", "smtp-password"} {
		size := (2 << 20) + 1
		if path == "functions/hello" {
			size = (7 << 20) + 1
		}
		w := call(s, "PUT", "/api/v1/clusters/prod/supabase/"+path, strings.Repeat("x", size), token)
		if w.Code != 413 {
			t.Fatal("route body cap not enforced", path, w.Code)
		}
	}
}
