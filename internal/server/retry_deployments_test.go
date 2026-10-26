// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func exhaustDeployments(t *testing.T, s *Server) {
	t.Helper()
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	c.Status.DeploymentFailures = map[string]domain.DeploymentFailure{"prodm0/engine": {RetryEpoch: c.Spec.RetryEpoch, Attempts: 5}}
	if err := s.Store.UpdateClusterState(t.Context(), c); err != nil {
		t.Fatal(err)
	}
}

func TestRetryClusterDeployments(t *testing.T) {
	s, _, token := testServer(t)
	path := "/api/v1/clusters/prod/retry-deployments"
	if w := call(s, "POST", path, "", token); w.Code != 409 || !strings.Contains(w.Body.String(), "No platform deployments") {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "POST", "/api/v1/clusters/missing/retry-deployments", "", token); w.Code != 404 {
		t.Fatal(w.Code, w.Body.String())
	}
	exhaustDeployments(t, s)
	before, _ := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	first := idempotentCall(s, "POST", path, "", token, "rearm")
	second := idempotentCall(s, "POST", path, "", token, "rearm")
	assertReplay(t, first, second, 202)
	c, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
	if err != nil || c.Spec.RetryEpoch != before.Spec.RetryEpoch+1 || c.Generation != before.Generation+1 || c.RequestedBy == "" {
		t.Fatal(c, err)
	}
	events, err := s.Store.ListEvents(t.Context(), store.EventFilter{Cluster: "prod"})
	if err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, e := range events {
		if e.Reason == "DeploymentRetriesRearmed" {
			found++
			if e.Actor == "" || !strings.Contains(e.Message, "0 -> 1") || !strings.Contains(e.Message, "prodm0/engine") {
				t.Fatal(e)
			}
		}
	}
	if found != 1 {
		t.Fatal("rearm event count", found)
	}
	if w := call(s, "POST", path, "", token); w.Code != 409 {
		t.Fatal(w.Code, w.Body.String())
	}
	if w := call(s, "PATCH", "/api/v1/clusters/prod", `{"spec":{"engineConfig":"changed"}}`, token); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
	c, _ = s.Store.GetCluster(t.Context(), c.Ref())
	if c.Spec.RetryEpoch != 1 {
		t.Fatal("ordinary spec write reset epoch", c.Spec.RetryEpoch)
	}
}

func TestRetryClusterDeploymentsUnchangedSpecUpdates(t *testing.T) {
	for _, method := range []string{"PUT", "PATCH"} {
		t.Run(method, func(t *testing.T) {
			s, _, token := testServer(t)
			response := call(s, "GET", "/api/v1/clusters/prod", "", token)
			if response.Code != 200 {
				t.Fatal(response.Code, response.Body.String())
			}
			var cluster map[string]json.RawMessage
			if err := json.Unmarshal(response.Body.Bytes(), &cluster); err != nil {
				t.Fatal(err)
			}
			var spec map[string]json.RawMessage
			if err := json.Unmarshal(cluster["spec"], &spec); err != nil {
				t.Fatal(err)
			}
			delete(spec, "retryEpoch") // Read-only action metadata is not PATCH input.
			body, err := json.Marshal(map[string]any{"spec": spec})
			if err != nil {
				t.Fatal(err)
			}
			// Establish an API-normalized spec; the fixture was inserted directly.
			if w := call(s, method, "/api/v1/clusters/prod", string(body), token); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			exhaustDeployments(t, s)
			if w := call(s, "POST", "/api/v1/clusters/prod/retry-deployments", "", token); w.Code != 202 {
				t.Fatal(w.Code, w.Body.String())
			}
			armed, err := s.Store.GetCluster(t.Context(), domain.ClusterRef{Name: "prod"})
			if err != nil {
				t.Fatal(err)
			}
			if w := call(s, method, "/api/v1/clusters/prod", string(body), token); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
			got, err := s.Store.GetCluster(t.Context(), armed.Ref())
			if err != nil {
				t.Fatal(err)
			}
			if got.Generation != armed.Generation || got.RequestedBy != armed.RequestedBy || got.Spec.RetryEpoch != armed.Spec.RetryEpoch {
				t.Fatalf("unchanged %s changed generation/actor/epoch: got %d %q %d; want %d %q %d", method, got.Generation, got.RequestedBy, got.Spec.RetryEpoch, armed.Generation, armed.RequestedBy, armed.Spec.RetryEpoch)
			}
		})
	}
}

func TestRetryClusterDeploymentsAuthorizationAndCSRF(t *testing.T) {
	s, _, _ := testServer(t)
	exhaustDeployments(t, s)
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "viewer", Roles: []string{"role:viewer"}}, "long-test-password"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(t.Context(), "viewer", "test", nil)
	if err != nil {
		t.Fatal(err)
	}
	path := "/api/v1/clusters/prod/retry-deployments"
	if w := call(s, "POST", path, "", token); w.Code != 403 {
		t.Fatal(w.Code, w.Body.String())
	}
	login := call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"admin-test-password"}`, "")
	if login.Code != 200 {
		t.Fatal(login.Code, login.Body.String())
	}
	var session api.Session
	if err := json.Unmarshal(login.Body.Bytes(), &session); err != nil {
		t.Fatal(err)
	}
	for _, valid := range []bool{false, true} {
		r := httptest.NewRequest("POST", path, nil)
		for _, cookie := range login.Result().Cookies() {
			r.AddCookie(cookie)
		}
		if valid {
			r.Header.Set("X-Karakoram-CSRF", *session.CsrfToken)
		}
		w := httptest.NewRecorder()
		testHandler(s).ServeHTTP(w, r)
		want := 403
		if valid {
			want = 202
		}
		if w.Code != want {
			t.Fatal(w.Code, w.Body.String())
		}
	}
}
