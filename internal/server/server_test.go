// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"bufio"
	"context"
	"encoding/json"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func call(s *Server, method, path, body, token string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, path, strings.NewReader(body))
	r.Header.Set("Content-Type", "application/json")
	if token != "" {
		r.Header.Set("Authorization", "Bearer "+token)
	}
	w := httptest.NewRecorder()
	testHandler(s).ServeHTTP(w, r)
	return w
}
func TestCookieCSRFAndAPITokenFlows(t *testing.T) {
	s, _, token := testServer(t)
	w := call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"admin-test-password"}`, "")
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w.Header().Get("Cache-Control") != "no-store" {
		t.Fatal("login may be cached")
	}
	cookies := w.Result().Cookies()
	var session api.Session
	if err := json.Unmarshal(w.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	if session.CsrfToken == nil || *session.CsrfToken == "" {
		t.Fatal("missing CSRF token")
	}
	send := func(path, csrf string) *httptest.ResponseRecorder {
		r := httptest.NewRequest("POST", path, strings.NewReader(`{"name":"cookie-token"}`))
		r.Header.Set("Content-Type", "application/json")
		for _, c := range cookies {
			r.AddCookie(c)
		}
		if csrf != "" {
			r.Header.Set("X-Karakoram-CSRF", csrf)
		}
		out := httptest.NewRecorder()
		testHandler(s).ServeHTTP(out, r)
		return out
	}
	if got := send("/api/v1/api-tokens", ""); got.Code != 403 {
		t.Fatal("CSRF bypass", got.Code, got.Body.String())
	}
	if got := send("/api/v1/api-tokens", *session.CsrfToken); got.Code != 201 {
		t.Fatal(got.Code, got.Body.String())
	}
	if got := call(s, "GET", "/api/v1/auth/me", "", token); got.Code != 200 {
		t.Fatal(got.Code, got.Body.String())
	}
	if got := call(s, "POST", "/api/v1/auth/logout", "", token); got.Code != 204 {
		t.Fatal(got.Code, got.Body.String())
	}
	if got := call(s, "GET", "/api/v1/clusters", "", ""); got.Code != 401 {
		t.Fatal("anonymous clusters", got.Code)
	}
}
func TestRBACCredentialSeparationAndFilteredCollections(t *testing.T) {
	s, _, admin := testServer(t)
	ctx := context.Background()
	for name, role := range map[string]string{"viewer": "viewer", "operator": "operator", "limited": "limited"} {
		if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: name, Roles: []string{"role:" + role}}, "long-test-password"); err != nil {
			t.Fatal(err)
		}
	}
	policy := s.RBAC.Policy() + "\np, role:limited, clusters, read, prod*, allow\np, role:limited, events, read, prod*, allow\np, role:limited, operations, read, prod*, allow\n"
	if err := s.RBAC.ReplacePolicy(ctx, policy); err != nil {
		t.Fatal(err)
	}
	viewer, _, err := s.Auth.CreateToken(ctx, "viewer", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	operator, _, err := s.Auth.CreateToken(ctx, "operator", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	limited, _, err := s.Auth.CreateToken(ctx, "limited", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, tok := range []string{viewer, operator} {
		w := call(s, "GET", "/api/v1/clusters/prod/connection", "", tok)
		if w.Code != 403 || strings.Contains(w.Body.String(), "private-test-password") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	if w := call(s, "GET", "/api/v1/accounts", "", operator); w.Code != 403 {
		t.Fatal("operator account access", w.Code)
	}
	if w := call(s, "GET", "/api/v1/clusters/dev", "", limited); w.Code != 403 {
		t.Fatal("limited object access", w.Code)
	}
	w := call(s, "GET", "/api/v1/clusters?limit=1", "", limited)
	var list api.ClusterList
	if err = json.Unmarshal(w.Body.Bytes(), &list); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(list.Items) != 1 || list.Items[0].Name != "prod" || list.NextCursor != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, name := range []string{"prod", "dev"} {
		if err = s.Context.Events.Record(ctx, domain.Event{Cluster: name, Type: "cluster.updated", Message: "changed"}); err != nil {
			t.Fatal(err)
		}
		if err = s.Store.EnqueueOperation(ctx, &domain.Operation{Cluster: name, Kind: "restart"}); err != nil {
			t.Fatal(err)
		}
	}
	if w = call(s, "GET", "/api/v1/events", "", limited); w.Code != 200 || strings.Contains(w.Body.String(), `"cluster":"dev"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = call(s, "GET", "/api/v1/operations", "", limited); w.Code != 200 || strings.Contains(w.Body.String(), `"cluster":"dev"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	if w = call(s, "GET", "/api/v1/clusters/prod/connection", "", admin); w.Code != 200 || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Body.String(), "private-test-password") {
		t.Fatal(w.Code, w.Body.String())
	}
	// Self-service identity endpoints do not require an account-management grant.
	for _, path := range []string{"/api/v1/auth/me", "/api/v1/permissions"} {
		if w = call(s, "GET", path, "", limited); w.Code != 200 {
			t.Fatal(path, w.Code)
		}
	}
}
func TestMustChangePasswordRestrictedToRecovery(t *testing.T) {
	s, _, _ := testServer(t)
	ctx := context.Background()
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "first", MustChangePassword: true}, "first-test-password"); err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/auth/login", `{"username":"first","password":"first-test-password"}`, "")
	if w.Code != 200 {
		t.Fatal(w.Code)
	}
	cookies := w.Result().Cookies()
	var si api.Session
	json.Unmarshal(w.Body.Bytes(), &si)
	for _, path := range []string{"/api/v1/clusters", "/api/v1/permissions"} {
		r := httptest.NewRequest("GET", path, nil)
		for _, c := range cookies {
			r.AddCookie(c)
		}
		out := httptest.NewRecorder()
		testHandler(s).ServeHTTP(out, r)
		if out.Code != 403 {
			t.Fatal(path, out.Code)
		}
	}
	r := httptest.NewRequest("POST", "/api/v1/auth/change-password", strings.NewReader(`{"currentPassword":"first-test-password","newPassword":"changed-test-password"}`))
	r.Header.Set("Content-Type", "application/json")
	for _, c := range cookies {
		r.AddCookie(c)
	}
	r.Header.Set("X-Karakoram-CSRF", *si.CsrfToken)
	out := httptest.NewRecorder()
	testHandler(s).ServeHTTP(out, r)
	if out.Code != 204 {
		t.Fatal(out.Code, out.Body.String())
	}
}
func TestClusterPatchOperationPayloadsAndDeleteIntent(t *testing.T) {
	s, _, token := testServer(t)
	w := call(s, "PATCH", "/api/v1/clusters/prod", `{"spec":{"engineConfig":"storage:\n  wiredTiger:\n    engineConfig:\n      cacheSizeGB: 0.25"}}`, token)
	if w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	c, err := s.Store.GetCluster(context.Background(), domain.ClusterRef{Name: "prod"})
	if err != nil || c.Generation != 2 || c.Spec.EngineConfig == "" {
		t.Fatal(c, err)
	}
	if w = call(s, "PATCH", "/api/v1/clusters/prod", `{"status":{"phase":"Ready"}}`, token); w.Code != 400 {
		t.Fatal("server-owned patch accepted", w.Code)
	}
	if w = call(s, "PATCH", "/api/v1/clusters/prod", `{"spec":{"version":"invalid"}}`, token); w.Code != 400 {
		t.Fatal("invalid spec accepted", w.Code)
	}
	if w = call(s, "POST", "/api/v1/clusters/prod/raise-fcv", `{"version":"8.0"}`, token); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	ops, err := s.Store.ListOperations(context.Background(), domain.ClusterRef{Name: "prod"})
	if err != nil || len(ops) != 2 || ops[1].Kind != "raise-fcv" || string(ops[1].Payload) != `{"version":"8.0"}` {
		t.Fatal(ops, err)
	}
	if w = call(s, "DELETE", "/api/v1/clusters/prod?keepVolumes=true&keepBackups=true", "", token); w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	c, err = s.Store.GetCluster(context.Background(), domain.ClusterRef{Name: "prod"})
	if err != nil || !c.Deleting || !c.DeleteOptions.KeepVolumes || !c.DeleteOptions.KeepBackups {
		t.Fatal(c, err)
	}
}
func TestInternalAgentConfigUsesPersistedIdentity(t *testing.T) {
	s, _, _ := testServer(t)
	ctx := context.Background()
	token := "agent-private-token"
	if err := s.Store.SetAgentToken(ctx, &domain.AgentToken{Cluster: "prod", Member: "prodm0", Token: token}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		token  string
		status int
	}{{"", 401}, {"wrong", 401}, {token, 200}} {
		r := httptest.NewRequest("GET", "/internal/agent/v1/config?cluster=dev&member=devm0", nil)
		if tc.token != "" {
			r.Header.Set("Authorization", "Bearer "+tc.token)
		}
		w := httptest.NewRecorder()
		s.InternalHandler().ServeHTTP(w, r)
		if w.Code != tc.status {
			t.Fatal(w.Code, w.Body.String())
		}
		if tc.status == 200 {
			if !strings.Contains(w.Body.String(), `"cluster":"prod"`) || !strings.Contains(w.Body.String(), `"member":"prodm0"`) {
				t.Fatal(w.Body.String())
			}
			if w.Header().Get("Cache-Control") != "no-store" {
				t.Fatal("agent credentials cached")
			}
		}
	}
}
func TestSSEReplayLiveFilteringAndCancellation(t *testing.T) {
	s, _, token := testServer(t)
	ctx := context.Background()
	if err := s.Context.Events.Record(ctx, domain.Event{Cluster: "prod", Type: "cluster.updated", Message: "replayed"}); err != nil {
		t.Fatal(err)
	}
	ts := httptest.NewServer(testHandler(s))
	defer ts.Close()
	reqCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	req, _ := http.NewRequestWithContext(reqCtx, "GET", ts.URL+"/api/v1/events/stream?cluster=prod", nil)
	req.Header.Set("Authorization", "Bearer "+token)
	res, err := ts.Client().Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer res.Body.Close()
	if res.StatusCode != 200 || !strings.Contains(res.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatal(res.StatusCode)
	}
	lines := make(chan string, 16)
	go func() {
		scanner := bufio.NewScanner(res.Body)
		for scanner.Scan() {
			select {
			case lines <- scanner.Text():
			case <-reqCtx.Done():
				return
			}
		}
		close(lines)
	}()
	await := func(phrase string) {
		t.Helper()
		timer := time.NewTimer(3 * time.Second)
		defer timer.Stop()
		for {
			select {
			case line, open := <-lines:
				if !open {
					t.Fatal("stream closed")
				}
				if strings.Contains(line, phrase) {
					return
				}
			case <-timer.C:
				t.Fatal("event not received", phrase)
			}
		}
	}
	await("replayed")
	if err = s.Context.Events.Record(ctx, domain.Event{Cluster: "dev", Type: "cluster.updated", Message: "hidden"}); err != nil {
		t.Fatal(err)
	}
	if err = s.Context.Events.Record(ctx, domain.Event{Cluster: "prod", Type: "backup.updated", Message: "live"}); err != nil {
		t.Fatal(err)
	}
	await("live")
	cancel()
}
func TestGeneratedDatabasePasswordReturnedOnce(t *testing.T) {
	s, _, token := testServer(t)
	w := call(s, "POST", "/api/v1/clusters/prod/database-users", `{"username":"app","database":"app","roles":[]}`, token)
	if w.Code != 201 || w.Header().Get("Cache-Control") != "no-store" || !strings.Contains(w.Body.String(), "generated-test-password") {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call(s, "GET", "/api/v1/clusters/prod/database-users/app/app", "", token)
	if w.Code != 200 || strings.Contains(w.Body.String(), "generatedPassword") {
		t.Fatal(w.Code, w.Body.String())
	}
}
func TestBackupStorageCredentialsStayWriteOnly(t *testing.T) {
	s, _, token := testServer(t)
	body := `{"name":"storage","type":"s3","endpointURL":"http://127.0.0.1:1","region":"us-east-1","bucket":"test","credentials":{"accessKeyId":"access-private","secretAccessKey":"secret-private"}}`
	w := call(s, "POST", "/api/v1/backup-storages", body, token)
	if w.Code != 201 || strings.Contains(w.Body.String(), "private") {
		t.Fatal(w.Code, w.Body.String())
	}
	st, err := s.Store.GetBackupStorage(context.Background(), domain.DefaultNamespace, "storage")
	if err != nil || st.Credentials.SecretAccessKey != "secret-private" {
		t.Fatal(st, err)
	}
	for _, path := range []string{"/api/v1/backup-storages", "/api/v1/backup-storages/storage"} {
		w = call(s, "GET", path, "", token)
		if w.Code != 200 || strings.Contains(w.Body.String(), "secret-private") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}

func TestEventPagesUseDurableAuthorizedCursors(t *testing.T) {
	s, _, _ := testServer(t)
	ctx := context.Background()
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "limited"}, "limited-test-password"); err != nil {
		t.Fatal(err)
	}
	if err := s.RBAC.ReplacePolicy(ctx, s.RBAC.Policy()+"\np, user:limited, events, read, prod, allow\n"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(ctx, "limited", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, cluster := range []string{"dev", "prod", "dev", "prod", "prod"} {
		if err := s.Context.Events.Record(ctx, domain.Event{Cluster: cluster, Type: "cluster.updated", Message: "changed"}); err != nil {
			t.Fatal(err)
		}
	}
	w := call(s, "GET", "/api/v1/events?limit=1", "", token)
	var first api.EventList
	if err := json.Unmarshal(w.Body.Bytes(), &first); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(first.Items) != 1 || first.Items[0].Id != 2 || first.NextCursor == nil {
		t.Fatal(w.Code, w.Body.String())
	}
	w = call(s, "GET", "/api/v1/events?limit=1&cursor="+*first.NextCursor, "", token)
	var second api.EventList
	if err := json.Unmarshal(w.Body.Bytes(), &second); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || len(second.Items) != 1 || second.Items[0].Id != 4 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestRootRoutesSecurityHeadersAndBodyLimit(t *testing.T) {
	s, _, token := testServer(t)
	w := call(s, "GET", "/healthz", "", "")
	if w.Code != 200 || w.Header().Get("Strict-Transport-Security") == "" || !strings.Contains(w.Header().Get("Content-Security-Policy"), "script-src 'self'") {
		t.Fatal(w.Code, w.Header())
	}
	for _, path := range []string{"/api/v1/healthz", "/api/v1/metrics", "/api/v1/unknown"} {
		w = call(s, "GET", path, "", token)
		if w.Code != 404 || !strings.Contains(w.Header().Get("Content-Type"), "application/json") {
			t.Fatal(path, w.Code, w.Body.String())
		}
	}
	w = call(s, "POST", "/api/v1/auth/login", strings.Repeat("x", (2<<20)+1), "")
	if w.Code != 413 {
		t.Fatal("oversized request accepted", w.Code)
	}
}

func TestUnkeyedClusterCreateConflictPreservesAcceptedRequest(t *testing.T) {
	s, _, token := testServer(t)
	ctx := t.Context()
	body := `{"name":"newcluster","spec":{"provider":"fake","version":"8.0","topology":{"type":"replicaSet"},"components":{"engine":{"replicas":3,"resources":{"maxCpu":2,"minCpu":1,"minRam":2,"maxRam":2,"minDisk":10,"maxDisk":10}}}}}`
	// call deliberately sends no Idempotency-Key: the second POST is a new
	// request, not a replay of the successful first create.
	first := call(s, "POST", "/api/v1/clusters?namespace=default", body, token)
	if first.Code != http.StatusCreated {
		t.Fatal(first.Code, first.Body.String())
	}
	opID := first.Header().Get("Karakoram-Operation-Id")
	if opID == "" {
		t.Fatal("accepted create has no operation ID")
	}
	ref := domain.ClusterRef{Namespace: "default", Name: "newcluster"}
	before, err := s.Store.GetCluster(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	second := call(s, "POST", "/api/v1/clusters?namespace=default", body, token)
	if second.Code != http.StatusConflict || !strings.Contains(second.Body.String(), "Cluster already exists") {
		t.Fatal(second.Code, second.Body.String())
	}
	after, err := s.Store.GetCluster(ctx, ref)
	if err != nil {
		t.Fatal(err)
	}
	beforeJSON, err := json.Marshal(before)
	if err != nil {
		t.Fatal(err)
	}
	afterJSON, err := json.Marshal(after)
	if err != nil {
		t.Fatal(err)
	}
	if string(beforeJSON) != string(afterJSON) {
		t.Fatal("duplicate create changed the accepted cluster")
	}
	ops, err := s.Store.ListOperations(ctx, ref)
	if err != nil || len(ops) != 1 || ops[0].ID != opID || ops[0].Kind != domain.OperationCreate {
		t.Fatal("duplicate create changed accepted operations", ops, err)
	}
}
