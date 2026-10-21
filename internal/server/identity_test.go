// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func expectIdentityError(t *testing.T, w *httptest.ResponseRecorder, status int, field, reason string) {
	t.Helper()
	var body struct {
		Code, Message string
		Details       map[string]any
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != status || body.Code == "" || body.Details["field"] != field || body.Details["reason"] != reason || !strings.Contains(body.Message, field) {
		t.Fatalf("want %d %s/%s: %d %s", status, field, reason, w.Code, w.Body.String())
	}
}

func TestPasswordChangeHTTPExplainsRejectedField(t *testing.T) {
	for _, tc := range []struct{ old, next, field, reason string }{
		{"admin-test-password", "admin-test-password", "newPassword", "reused"},
		{"incorrect", "another-test-password", "currentPassword", "invalid"},
		{"admin-test-password", "short", "newPassword", "out_of_range"},
	} {
		t.Run(tc.reason, func(t *testing.T) {
			s, _, token := testServer(t)
			body, _ := json.Marshal(map[string]string{"currentPassword": tc.old, "newPassword": tc.next})
			expectIdentityError(t, call(s, "POST", "/api/v1/auth/change-password", string(body), token), 400, tc.field, tc.reason)
		})
	}
}

func TestPasswordResetHTTPAcceptsCurrentPassword(t *testing.T) {
	s, _, token := testServer(t)
	ctx := t.Context()
	if _, err := s.Auth.CreateAccount(ctx, domain.Account{Name: "reader"}, "reader-test-password"); err != nil {
		t.Fatal(err)
	}
	before, err := s.Store.GetAccount(ctx, "reader")
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/accounts/reader/reset-password", `{"password":"reader-test-password"}`, token)
	if w.Code != 204 || w.Body.Len() != 0 {
		t.Fatalf("reset returned %d: %s", w.Code, w.Body.String())
	}
	after, err := s.Store.GetAccount(ctx, "reader")
	if err != nil {
		t.Fatal(err)
	}
	if !after.MustChangePassword || !after.PasswordChangedAt.After(before.PasswordChangedAt) ||
		!after.UpdatedAt.Equal(after.PasswordChangedAt) || after.PasswordHash == before.PasswordHash {
		t.Fatalf("reset did not rotate password state: %+v", after)
	}
}

func TestAccountHTTPValidationAndUpdatedAt(t *testing.T) {
	s, _, token := testServer(t)
	expectIdentityError(t, call(s, "POST", "/api/v1/accounts", `{"name":"bad name","password":"long-test-password"}`, token), 400, "name", "invalid")
	expectIdentityError(t, call(s, "POST", "/api/v1/accounts", `{"name":"reader","password":"short"}`, token), 400, "password", "out_of_range")
	w := call(s, "POST", "/api/v1/accounts", `{"name":"reader","password":"long-test-password"}`, token)
	if w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	var created api.Account
	if err := json.Unmarshal(w.Body.Bytes(), &created); err != nil {
		t.Fatal(err)
	}
	w = call(s, "POST", "/api/v1/accounts", `{"name":"reader","password":"long-test-password"}`, token)
	if w.Code != 409 || !strings.Contains(w.Body.String(), `"reason":"already_exists"`) || !strings.Contains(w.Body.String(), `"name":"reader"`) {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, enabled := range []bool{false, true} {
		w = call(s, "PUT", "/api/v1/accounts/reader", `{"enabled":`+strconv.FormatBool(enabled)+`,"mustChangePassword":true}`, token)
		var updated api.Account
		if err := json.Unmarshal(w.Body.Bytes(), &updated); err != nil {
			t.Fatal(err)
		}
		if w.Code != 200 || !updated.UpdatedAt.After(created.UpdatedAt) || updated.Enabled != enabled {
			t.Fatal(w.Code, w.Body.String())
		}
		created = updated
	}
}

func TestLoginHTTPThrottleIncludesRetryTimingWithoutOracle(t *testing.T) {
	s, _, _ := testServer(t)
	for range 5 {
		w := call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"wrong"}`, "")
		if w.Code != 401 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	for _, secret := range []string{"wrong", "admin-test-password"} {
		w := call(s, "POST", "/api/v1/auth/login", `{"username":"admin","password":"`+secret+`"}`, "")
		seconds, err := strconv.Atoi(w.Header().Get("Retry-After"))
		var body struct {
			Code    string
			Details struct {
				RetryAfterSeconds int `json:"retryAfterSeconds"`
			}
		}
		if err != nil {
			t.Fatal(err)
		}
		if err = json.Unmarshal(w.Body.Bytes(), &body); err != nil {
			t.Fatal(err)
		}
		if w.Code != 429 || body.Code != "rate_limited" || seconds < 1 || seconds != body.Details.RetryAfterSeconds {
			t.Fatal(w.Code, w.Header(), w.Body.String())
		}
	}
}

func TestTokenHTTPValidationOnCreateAndUpdate(t *testing.T) {
	s, _, token := testServer(t)
	w := call(s, "POST", "/api/v1/api-tokens", `{"name":"automation"}`, token)
	var created api.CreatedAPIToken
	if w.Code != 201 || json.Unmarshal(w.Body.Bytes(), &created) != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	for _, path := range []string{"/api/v1/api-tokens", "/api/v1/api-tokens/" + created.Token.Id} {
		method := "POST"
		if path != "/api/v1/api-tokens" {
			method = "PUT"
		}
		for _, tc := range []struct{ body, field, reason string }{
			{`{}`, "name", "required"},
			{`{"name":" "}`, "name", "required"},
			{`{"name":"` + strings.Repeat("x", 129) + `"}`, "name", "out_of_range"},
			{`{"name":"automation","expiresAt":"2020-01-01T00:00:00Z"}`, "expiresAt", "out_of_range"},
			{`{"name":"automation","expiresAt":"tomorrow"}`, "expiresAt", "invalid"},
		} {
			expectIdentityError(t, call(s, method, path, tc.body, token), 400, tc.field, tc.reason)
		}
	}
	w = call(s, "GET", "/api/v1/api-tokens/"+created.Token.Id, "", token)
	if w.Code != 200 || strings.Contains(w.Body.String(), "2020-") {
		t.Fatal("failed update mutated token", w.Body.String())
	}
}

func TestPolicyHTTPNamesLineAndRejectsAdminLockout(t *testing.T) {
	s, _, token := testServer(t)
	previous := s.RBAC.Policy()
	malformed, _ := json.Marshal(map[string]string{"policy": "# comment\np, user:reader, imaginary, read, *, allow"})
	w := call(s, "PUT", "/api/v1/rbac/policy", string(malformed), token)
	expectIdentityError(t, w, 400, "policy", "invalid")
	if !strings.Contains(w.Body.String(), "line 2") {
		t.Fatal(w.Body.String())
	}
	lockout, _ := json.Marshal(map[string]string{"policy": "g, user:reader, role:viewer"})
	expectIdentityError(t, call(s, "PUT", "/api/v1/rbac/policy", string(lockout), token), 400, "policy", "protected")
	if s.RBAC.Policy() != previous {
		t.Fatal("rejection replaced policy")
	}
}

func TestLegacyAccountTimestampFallback(t *testing.T) {
	at := time.Now().UTC()
	// Account metadata created before updatedAt was persisted remains readable.
	s, _, _ := testServer(t)
	a, err := s.Auth.GetAccount(t.Context(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	a.UpdatedAt = time.Time{}
	a.PasswordChangedAt = at
	if got := accountAPI(*a); !got.UpdatedAt.Equal(at) {
		t.Fatal(got)
	}
}

func TestAccountAuthorizationDenialExplainsRequiredGrant(t *testing.T) {
	s, _, _ := testServer(t)
	if _, err := s.Auth.CreateAccount(t.Context(), domain.Account{Name: "reader"}, "reader-test-password"); err != nil {
		t.Fatal(err)
	}
	token, _, err := s.Auth.CreateToken(t.Context(), "reader", "automation", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	w := call(s, "POST", "/api/v1/accounts", `{"name":"new-reader","password":"new-reader-password"}`, token)
	var body struct {
		Code, Message string
		Details       map[string]any
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if w.Code != 403 || body.Code != "forbidden" || body.Details["resource"] != "accounts" || body.Details["action"] != "create" || !strings.Contains(body.Message, "administrator") {
		t.Fatal(w.Code, w.Body.String())
	}
}
