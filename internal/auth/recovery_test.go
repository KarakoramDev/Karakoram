// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"bytes"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const recoveryPassword = "recovery-test-password"

func openRecoveryService(t *testing.T, path string, logs *bytes.Buffer) *Service {
	t.Helper()
	sealer, err := secrets.New(strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s, err := New(Options{Store: st, Sealer: sealer, Logger: slog.New(slog.NewJSONHandler(logs, nil))})
	if err != nil {
		t.Fatal(err)
	}
	return s
}

func TestAdminRecoveryAppliesOnceAcrossRestarts(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "karakoram.db")
	var logs bytes.Buffer
	s := openRecoveryService(t, path, &logs)
	if applied, err := s.RecoverAdmin(ctx, "", recoveryPassword, strings.Repeat("k", 32)); err != nil || !applied {
		t.Fatal("first recovery", applied, err)
	}
	if err := s.ChangePassword(ctx, "admin", recoveryPassword, password); err != nil {
		t.Fatal(err)
	}
	cookies, _ := login(t, s)
	secret, token, err := s.CreateToken(ctx, "admin", "after-recovery", nil)
	if err != nil {
		t.Fatal(err)
	}
	before, err := s.o.Store.GetAccount(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	rules, err := s.o.Store.ListPolicyRules(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err = s.o.Store.Close(); err != nil {
		t.Fatal(err)
	}
	s = openRecoveryService(t, path, &logs)
	if applied, err := s.RecoverAdmin(ctx, "admin", recoveryPassword, strings.Repeat("k", 32)); err != nil || applied {
		t.Fatal("same recovery must be a no-op", applied, err)
	}
	after, err := s.o.Store.GetAccount(ctx, "admin")
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("restart reset changed password/flags", err)
	}
	if _, err = s.Authenticate(request("GET", cookies, "")); err != nil {
		t.Fatal("restart revoked new session", err)
	}
	r := request("GET", nil, "")
	r.Header.Set("Authorization", "Bearer "+secret)
	if _, err = s.Authenticate(r); err != nil {
		t.Fatal("restart revoked new token", err)
	}
	if applied, err := s.RecoverAdmin(ctx, "admin", "a-new-recovery-password", strings.Repeat("k", 32)); err != nil || !applied {
		t.Fatal("changed recovery must apply", applied, err)
	}
	after, err = s.o.Store.GetAccount(ctx, "admin")
	if err != nil || !after.MustChangePassword || !VerifyPassword(after.PasswordHash, "a-new-recovery-password") {
		t.Fatal("new recovery did not reset", err)
	}
	if _, err = s.o.Store.GetApiToken(ctx, token.ID); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("new recovery did not revoke token", err)
	}
	afterRules, err := s.o.Store.ListPolicyRules(ctx)
	if err != nil || !reflect.DeepEqual(rules, afterRules) {
		t.Fatal("recovery policy accumulates duplicate grants", err)
	}
	events, err := s.o.Store.ListEvents(ctx, store.EventFilter{})
	if err != nil || len(events) != 2 {
		t.Fatal("expected one durable audit event per applied value", len(events), err)
	}
	if strings.Count(logs.String(), "Local administrator access recovered") != 2 {
		t.Fatal("expected one log per applied value")
	}
	encoded, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{recoveryPassword, "a-new-recovery-password", strings.Repeat("k", 32)} {
		if strings.Contains(logs.String(), secret) || strings.Contains(string(encoded), secret) {
			t.Fatal("recovery logged a secret")
		}
	}
	raw, err := s.o.Store.GetSetting(ctx, "auth/admin-recovery/admin")
	var fingerprint string
	if err != nil || json.Unmarshal(raw, &fingerprint) != nil {
		t.Fatal("missing recovery receipt", err)
	}
	if decoded, err := hex.DecodeString(fingerprint); err != nil || len(decoded) != 32 {
		t.Fatal("receipt is not a SHA-256 fingerprint")
	}
}

func TestAdminRecoveryCreatesAdministratorAndRepairsPolicy(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	// Persist a valid customized policy with no built-in admin allow and
	// conflicting direct/inherited denies for the recovery identity.
	unrelated := domain.PolicyRule{PType: "p", Values: []string{"role:viewer", "clusters", "read", "*", "allow"}}
	if err := s.o.Store.ReplacePolicyRules(ctx, []domain.PolicyRule{
		unrelated,
		{PType: "p", Values: []string{"role:old-admin", "*", "*", "*", "allow"}},
		{PType: "g", Values: []string{"user:admin", "role:old-admin"}},
		{PType: "p", Values: []string{"user:rescuer", "accounts", "read", "*", "deny"}},
		{PType: "g", Values: []string{"user:rescuer", "role:restricted"}},
		{PType: "g", Values: []string{"role:admin", "role:restricted"}},
		{PType: "p", Values: []string{"role:restricted", "*", "*", "*", "deny"}},
		{PType: "p", Values: []string{"role:admin", "clusters", "read", "*", "deny"}},
	}); err != nil {
		t.Fatal(err)
	}
	if applied, err := s.RecoverAdmin(ctx, "rescuer", recoveryPassword, strings.Repeat("k", 32)); err != nil || !applied {
		t.Fatal(applied, err)
	}
	a, err := s.o.Store.GetAccount(ctx, "rescuer")
	if err != nil || a.OIDC || a.Disabled || !a.MustChangePassword || !reflect.DeepEqual(a.Roles, []string{"role:admin"}) {
		t.Fatal("recovery identity not a forced-change local administrator", err)
	}
	policy, err := rbac.New(s.o.Store)
	if err != nil {
		t.Fatal(err)
	}
	for _, subjects := range [][]string{{"user:rescuer"}, {"user:rescuer", "role:admin"}} {
		if !policy.Can(subjects, "accounts", "write", "*") || !policy.Can(subjects, "clusters", "read", "prod") || !policy.Can(subjects, "*", "*", "*") {
			t.Fatal("recovery did not restore unrestricted administrator authorization")
		}
	}
	if !policy.Can([]string{"role:viewer"}, "clusters", "read", "prod") || policy.Can([]string{"role:restricted"}, "clusters", "read", "prod") {
		t.Fatal("recovery changed unrelated policy")
	}
	w := httptest.NewRecorder()
	info, err := s.Login(ctx, w, "rescuer", recoveryPassword)
	if err != nil || !info.Account.MustChangePassword {
		t.Fatal("recovered administrator cannot log in", err)
	}
}

func TestAdminRecoveryReenablesAndRevokesCredentials(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	cookies, info := login(t, s)
	rotated := httptest.NewRecorder()
	if _, err := s.Refresh(ctx, rotated, request("POST", cookies, info.CSRFToken)); err != nil {
		t.Fatal(err)
	}
	secret, token, err := s.CreateToken(ctx, "admin", "old", nil)
	if err != nil {
		t.Fatal(err)
	}
	// Future-dated credentials must be removed too, not merely hidden behind
	// PasswordChangedAt until the recovery password is changed.
	stored, err := s.o.Store.GetApiToken(ctx, token.ID)
	if err != nil {
		t.Fatal(err)
	}
	stored.CreatedAt = time.Now().Add(time.Hour)
	if err = s.o.Store.UpdateApiToken(ctx, stored); err != nil {
		t.Fatal(err)
	}
	if err = s.o.Store.CreateSession(ctx, &domain.Session{ID: "future", Owner: "admin", RefreshHash: "future-hash", CreatedAt: time.Now().Add(time.Hour), ExpiresAt: time.Now().Add(2 * time.Hour)}); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateAccount(ctx, domain.Account{Name: "other"}, password); err != nil {
		t.Fatal(err)
	}
	otherSecret, _, err := s.CreateToken(ctx, "other", "untouched", nil)
	if err != nil {
		t.Fatal(err)
	}
	otherLogin := httptest.NewRecorder()
	if _, err = s.Login(ctx, otherLogin, "other", password); err != nil {
		t.Fatal(err)
	}
	a, err := s.GetAccount(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	a.Disabled = true
	a.Roles = []string{"role:viewer"}
	if _, err = s.UpdateAccount(ctx, *a); err != nil {
		t.Fatal(err)
	}
	if applied, err := s.RecoverAdmin(ctx, "admin", recoveryPassword, strings.Repeat("k", 32)); err != nil || !applied {
		t.Fatal(applied, err)
	}
	a, err = s.GetAccount(ctx, "admin")
	if err != nil || a.Disabled || !a.MustChangePassword {
		t.Fatal("disabled account not recovered", err)
	}
	for _, oldCookies := range [][]*http.Cookie{cookies, rotated.Result().Cookies()} {
		if _, err = s.Authenticate(request("GET", oldCookies, "")); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal("access session survived recovery", err)
		}
		if _, err = s.Refresh(ctx, httptest.NewRecorder(), request("POST", oldCookies, info.CSRFToken)); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal("refresh token survived recovery", err)
		}
	}
	if err = s.ChangePassword(ctx, "admin", recoveryPassword, password); err != nil {
		t.Fatal(err)
	}
	r := request("GET", nil, "")
	r.Header.Set("Authorization", "Bearer "+secret)
	if _, err = s.Authenticate(r); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("old API token revived after mandatory change", err)
	}
	sessions, err := s.o.Store.ListSessions(ctx)
	if err != nil || len(sessions) != 1 || sessions[0].Owner != "other" {
		t.Fatal("recovery did not remove only the target sessions", err)
	}
	tokens, err := s.o.Store.ListApiTokens(ctx)
	if err != nil || len(tokens) != 1 || tokens[0].Owner != "other" {
		t.Fatal("recovery did not remove only the target API tokens", err)
	}
	r.Header.Set("Authorization", "Bearer "+otherSecret)
	if _, err = s.Authenticate(r); err != nil {
		t.Fatal("unrelated token revoked", err)
	}
	if _, err = s.Authenticate(request("GET", otherLogin.Result().Cookies(), "")); err != nil {
		t.Fatal("unrelated session revoked", err)
	}
}

func TestAdminRecoveryInvalidInputDoesNotChangeState(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	login(t, s)
	if _, _, err := s.CreateToken(ctx, "admin", "unchanged", nil); err != nil {
		t.Fatal(err)
	}
	if err := s.o.Store.CreateAccount(ctx, &domain.Account{Name: "oidc", OIDC: true}); err != nil {
		t.Fatal(err)
	}
	snapshot := func() string {
		t.Helper()
		accounts, err := s.o.Store.ListAccounts(ctx)
		if err != nil {
			t.Fatal(err)
		}
		sessions, err := s.o.Store.ListSessions(ctx)
		if err != nil {
			t.Fatal(err)
		}
		tokens, err := s.o.Store.ListApiTokens(ctx)
		if err != nil {
			t.Fatal(err)
		}
		settings, err := s.o.Store.ListSettings(ctx)
		if err != nil {
			t.Fatal(err)
		}
		rules, err := s.o.Store.ListPolicyRules(ctx)
		if err != nil {
			t.Fatal(err)
		}
		events, err := s.o.Store.ListEvents(ctx, store.EventFilter{})
		if err != nil {
			t.Fatal(err)
		}
		raw, err := json.Marshal([]any{accounts, sessions, tokens, settings, rules, events})
		if err != nil {
			t.Fatal(err)
		}
		return string(raw)
	}
	before := snapshot()
	for _, tc := range []struct{ name, account, password, key string }{
		{"short", "admin", "short", strings.Repeat("k", 32)},
		{"too-long", "admin", strings.Repeat("p", 1025), strings.Repeat("k", 32)},
		{"invalid-account", "bad/account", recoveryPassword, strings.Repeat("k", 32)},
		{"oidc-account", "oidc", recoveryPassword, strings.Repeat("k", 32)},
		{"short-key", "admin", recoveryPassword, "short"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if applied, err := s.RecoverAdmin(ctx, tc.account, tc.password, tc.key); err == nil || applied {
				t.Fatal("invalid recovery applied", applied, err)
			}
			if snapshot() != before {
				t.Fatal("invalid recovery changed persisted state")
			}
		})
	}
	if applied, err := s.RecoverAdmin(ctx, "bad/account", "", ""); err != nil || applied || snapshot() != before {
		t.Fatal("unset recovery must be a no-op", applied, err)
	}
}

func TestAdminRecoveryRetriesInterruptedReceipt(t *testing.T) {
	ctx := t.Context()
	path := filepath.Join(t.TempDir(), "karakoram.db")
	var logs bytes.Buffer
	s := openRecoveryService(t, path, &logs)
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	if _, err = db.Exec(`CREATE TRIGGER fail_recovery_receipt BEFORE INSERT ON records
		WHEN NEW.kind = 'setting' AND NEW.key = 'auth/admin-recovery/admin'
		BEGIN SELECT RAISE(FAIL, 'injected receipt failure'); END`); err != nil {
		t.Fatal(err)
	}
	if applied, err := s.RecoverAdmin(ctx, "admin", recoveryPassword, strings.Repeat("k", 32)); err == nil || applied {
		t.Fatal("receipt failure must abort startup", applied, err)
	}
	if _, err = s.o.Store.GetSetting(ctx, "auth/admin-recovery/admin"); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("failed application recorded as complete", err)
	}
	if _, err = db.Exec("DROP TRIGGER fail_recovery_receipt"); err != nil {
		t.Fatal(err)
	}
	if err = s.o.Store.Close(); err != nil {
		t.Fatal(err)
	}
	s = openRecoveryService(t, path, &logs)
	if applied, err := s.RecoverAdmin(ctx, "admin", recoveryPassword, strings.Repeat("k", 32)); err != nil || !applied {
		t.Fatal("interrupted application did not resume", applied, err)
	}
	if applied, err := s.RecoverAdmin(ctx, "admin", recoveryPassword, strings.Repeat("k", 32)); err != nil || applied {
		t.Fatal("completed retry was not idempotent", applied, err)
	}
}
