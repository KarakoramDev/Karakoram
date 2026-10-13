// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

const password = "a-long-test-password"

func setup(t *testing.T) *Service {
	t.Helper()
	seal, err := secrets.New(strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(":memory:", seal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	s, err := New(Options{Store: st, Sealer: seal, SecureCookies: true, PublicURL: "https://karakoram.example"})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.Bootstrap(context.Background(), password); err != nil {
		t.Fatal(err)
	}
	a, err := s.GetAccount(t.Context(), "admin")
	if err != nil || !a.MustChangePassword {
		t.Fatal("bootstrap must require password change", err)
	}
	if err = s.ChangePassword(t.Context(), "admin", password, password); err != nil {
		t.Fatal(err)
	}
	return s
}
func request(method string, cookies []*http.Cookie, token string) *http.Request {
	r := httptest.NewRequest(method, "https://karakoram.example/api/v1/auth/refresh", nil)
	for _, c := range cookies {
		r.AddCookie(c)
	}
	if token != "" {
		r.Header.Set(CSRFHeader, token)
	}
	return r
}
func login(t *testing.T, s *Service) ([]*http.Cookie, *SessionInfo) {
	t.Helper()
	w := httptest.NewRecorder()
	info, err := s.Login(context.Background(), w, "admin", password)
	if err != nil {
		t.Fatal(err)
	}
	return w.Result().Cookies(), info
}
func TestPasswordHash(t *testing.T) {
	hash, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	second, err := HashPassword(password)
	if err != nil {
		t.Fatal(err)
	}
	if hash == second || !VerifyPassword(hash, password) || VerifyPassword(hash, "wrong") {
		t.Fatal("password verifier")
	}
	if _, err = HashPassword("short"); err == nil {
		t.Fatal("short accepted")
	}
	if VerifyPassword(strings.Replace(hash, "m=65536", "m=999999999", 1), password) {
		t.Fatal("unbounded parameters accepted")
	}
	if VerifyPassword("invalid", password) {
		t.Fatal("invalid encoding")
	}
}
func TestBootstrapAndKeyPersistence(t *testing.T) {
	s := setup(t)
	if generated, err := s.Bootstrap(context.Background(), "short"); err != nil || generated != "" {
		t.Fatalf("repeat bootstrap %q %v", generated, err)
	}
	again, err := New(s.o)
	if err != nil {
		t.Fatal(err)
	}
	if !s.key.Equal(again.key) {
		t.Fatal("signing key not stable")
	}
	cookies, _ := login(t, s)
	if _, err = again.Authenticate(request("GET", cookies, "")); err != nil {
		t.Fatal(err)
	}
	raw, err := s.o.Store.GetSetting(context.Background(), "auth/signing-key")
	if err != nil || !strings.Contains(string(raw), "kk:v1:") {
		t.Fatal("key not sealed")
	}
	if err = s.DeleteAccount(context.Background(), "admin"); err != nil {
		t.Fatal(err)
	}
	generated, err := s.Bootstrap(context.Background(), "")
	if err != nil || len(generated) < 12 {
		t.Fatal(err)
	}
	a, err := s.GetAccount(context.Background(), "admin")
	if err != nil || !a.MustChangePassword {
		t.Fatal("generated password not forced")
	}
}
func TestLoginCSRFAndCookieFlags(t *testing.T) {
	s := setup(t)
	cookies, info := login(t, s)
	for _, c := range cookies {
		if !c.Secure || c.SameSite != http.SameSiteStrictMode {
			t.Fatal("cookie flags", c)
		}
		if c.Name != CSRFCookie && !c.HttpOnly {
			t.Fatal("credential cookie not HttpOnly")
		}
		if c.Name == CSRFCookie && c.HttpOnly {
			t.Fatal("CSRF unreadable")
		}
	}
	p, err := s.Authenticate(request("GET", cookies, ""))
	if err != nil || p.Account != "admin" || p.CSRFToken != info.CSRFToken {
		t.Fatal(p, err)
	}
	if _, err = s.Authenticate(request("POST", cookies, "")); !errors.Is(err, ErrCSRF) {
		t.Fatal(err)
	}
	if _, err = s.Authenticate(request("DELETE", cookies, info.CSRFToken)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Authenticate(request("PUT", cookies, "wrong")); !errors.Is(err, ErrCSRF) {
		t.Fatal(err)
	}
	if _, err = s.Login(context.Background(), httptest.NewRecorder(), "admin", "bad"); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal(err)
	}
}
func TestRefreshRotationReuseAndLogout(t *testing.T) {
	s := setup(t)
	cookies, info := login(t, s)
	w := httptest.NewRecorder()
	next, err := s.Refresh(context.Background(), w, request("POST", cookies, info.CSRFToken))
	if err != nil {
		t.Fatal(err)
	}
	rotated := w.Result().Cookies()
	if next.CSRFToken != info.CSRFToken {
		t.Fatal("CSRF changed unexpectedly")
	}
	if _, err = s.Authenticate(request("GET", rotated, "")); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Refresh(context.Background(), httptest.NewRecorder(), request("POST", cookies, info.CSRFToken)); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("reuse accepted", err)
	}
	if _, err = s.Authenticate(request("GET", rotated, "")); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("family not revoked", err)
	}
	if _, err = s.Refresh(context.Background(), httptest.NewRecorder(), request("POST", rotated, info.CSRFToken)); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal(err)
	}
	cookies, info = login(t, s)
	w = httptest.NewRecorder()
	if err = s.Logout(context.Background(), w, request("POST", cookies, info.CSRFToken)); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Authenticate(request("GET", cookies, "")); err == nil {
		t.Fatal("logout not revoked")
	}
	for _, c := range w.Result().Cookies() {
		if c.MaxAge != -1 {
			t.Fatal("cookie not cleared")
		}
	}
}
func TestTokensPasswordInvalidationAndAccounts(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	secret, tok, err := s.CreateToken(ctx, "admin", "cli", nil)
	if err != nil || !strings.HasPrefix(secret, "kk_") || tok.Hash != "" {
		t.Fatal(err)
	}
	r := request("POST", nil, "")
	r.Header.Set("Authorization", "Bearer "+secret)
	p, err := s.Authenticate(r)
	if err != nil || p.TokenID != tok.ID {
		t.Fatal(p, err)
	}
	meta, err := s.GetToken(ctx, tok.ID)
	if err != nil || meta.LastUsedAt == nil || meta.Hash != "" {
		t.Fatal(meta, err)
	}
	meta.Name = "updated"
	if _, err = s.UpdateToken(ctx, *meta); err != nil {
		t.Fatal(err)
	}
	if tokens, err := s.ListTokens(ctx, "admin"); err != nil || len(tokens) != 1 {
		t.Fatal(tokens, err)
	}
	cookies, info := login(t, s)
	if err = s.ChangePassword(ctx, "admin", password, "another-long-password"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Authenticate(r); err == nil {
		t.Fatal("old API token accepted")
	}
	if _, err = s.Authenticate(request("GET", cookies, "")); err == nil {
		t.Fatal("old access accepted")
	}
	if _, err = s.Refresh(ctx, httptest.NewRecorder(), request("POST", cookies, info.CSRFToken)); err == nil {
		t.Fatal("old refresh accepted")
	}
	if err = s.DeleteToken(ctx, tok.ID); err != nil {
		t.Fatal(err)
	}
	a, err := s.CreateAccount(ctx, domain.Account{Name: "viewer", Roles: []string{"viewer"}}, password)
	if err != nil || a.PasswordHash != "" {
		t.Fatal(a, err)
	}
	a.Disabled = true
	if _, err = s.UpdateAccount(ctx, *a); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Login(ctx, httptest.NewRecorder(), "viewer", password); err == nil {
		t.Fatal("disabled login")
	}
	if _, _, err = s.CreateToken(ctx, "viewer", "disabled", nil); err == nil {
		t.Fatal("disabled token")
	}
	if err = s.ResetPassword(ctx, "viewer", "reset-long-password"); err != nil {
		t.Fatal(err)
	}
	if err = s.DeleteAccount(ctx, "viewer"); err != nil {
		t.Fatal(err)
	}
}
func TestTokenExpiryAndDisabledOwner(t *testing.T) {
	s := setup(t)
	ctx := context.Background()
	expiry := time.Now().Add(time.Hour)
	secret, tok, err := s.CreateToken(ctx, "admin", "expiring", &expiry)
	if err != nil {
		t.Fatal(err)
	}
	r := request("GET", nil, "")
	r.Header.Set("Authorization", "Bearer "+secret)
	past := time.Now().Add(-time.Hour)
	tok.ExpiresAt = &past
	if _, err = s.UpdateToken(ctx, tok); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Authenticate(r); err == nil {
		t.Fatal("expired token")
	}
	tok.ExpiresAt = nil
	if _, err = s.UpdateToken(ctx, tok); err != nil {
		t.Fatal(err)
	}
	a, err := s.GetAccount(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	a.Disabled = true
	if _, err = s.UpdateAccount(ctx, *a); err != nil {
		t.Fatal(err)
	}
	if _, err = s.Authenticate(r); err == nil {
		t.Fatal("disabled owner")
	}
}
func TestLoginBackoff(t *testing.T) {
	s := setup(t)
	ctx := WithClientIP(context.Background(), "127.0.0.1")
	for range 5 {
		_, err := s.Login(ctx, httptest.NewRecorder(), "admin", "wrong")
		if !errors.Is(err, ErrUnauthenticated) {
			t.Fatal(err)
		}
	}
	if _, err := s.Login(ctx, httptest.NewRecorder(), "admin", password); !errors.Is(err, ErrRateLimited) {
		t.Fatal(err)
	}
}

func TestConcurrentRefreshRevokesFamily(t *testing.T) {
	s := setup(t)
	cookies, info := login(t, s)
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_, err := s.Refresh(t.Context(), httptest.NewRecorder(), request("POST", cookies, info.CSRFToken))
			results <- err
		}()
	}
	wg.Wait()
	close(results)
	success, replay := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrUnauthenticated) {
			replay++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || replay != 1 {
		t.Fatal("refresh was not consumed exactly once", success, replay)
	}
	if _, err := s.Authenticate(request("GET", cookies, "")); err == nil {
		t.Fatal("concurrent replay did not revoke family")
	}
}

func TestRefreshCSRFExpiryAndMandatoryChange(t *testing.T) {
	s := setup(t)
	cookies, info := login(t, s)
	if _, err := s.Refresh(t.Context(), httptest.NewRecorder(), request("POST", cookies, "")); !errors.Is(err, ErrCSRF) {
		t.Fatal(err)
	}
	a, err := s.GetAccount(t.Context(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	a.MustChangePassword = true
	if _, err = s.UpdateAccount(t.Context(), *a); err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	if _, err = s.Refresh(t.Context(), w, request("POST", cookies, info.CSRFToken)); !errors.Is(err, ErrPasswordChangeRequired) {
		t.Fatal(err)
	}
	if len(w.Result().Cookies()) != 0 {
		t.Fatal("restricted refresh emitted cookies")
	}
	a.MustChangePassword = false
	if _, err = s.UpdateAccount(t.Context(), *a); err != nil {
		t.Fatal(err)
	}
	s.now = func() time.Time { return time.Now().UTC().Add(31 * 24 * time.Hour) }
	if _, err = s.Authenticate(request("GET", cookies, "")); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("expired access accepted", err)
	}
	if _, err = s.Refresh(t.Context(), httptest.NewRecorder(), request("POST", cookies, info.CSRFToken)); !errors.Is(err, ErrUnauthenticated) {
		t.Fatal("expired refresh accepted", err)
	}
}

func TestDeletedAccountCredentialsCannotBeAdopted(t *testing.T) {
	s := setup(t)
	secret, _, err := s.CreateToken(t.Context(), "admin", "cli", nil)
	if err != nil {
		t.Fatal(err)
	}
	cookies, _ := login(t, s)
	if err = s.DeleteAccount(t.Context(), "admin"); err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateAccount(t.Context(), domain.Account{Name: "admin"}, password); err != nil {
		t.Fatal(err)
	}
	r := request("GET", nil, "")
	r.Header.Set("Authorization", "Bearer "+secret)
	if _, err = s.Authenticate(r); err == nil {
		t.Fatal("deleted owner's token adopted")
	}
	if _, err = s.Authenticate(request("GET", cookies, "")); err == nil {
		t.Fatal("deleted owner's session adopted")
	}
}

func TestLoginHashingDoesNotBlockAuthenticationAndRechecksAccount(t *testing.T) {
	s := setup(t)
	cookies, _ := login(t, s)
	started, release := make(chan struct{}), make(chan struct{})
	s.verifyPassword = func(string, string) bool {
		close(started)
		<-release
		return true
	}
	result := make(chan error, 1)
	go func() {
		_, err := s.Login(t.Context(), httptest.NewRecorder(), "admin", password)
		result <- err
	}()
	<-started
	if _, err := s.Authenticate(request("GET", cookies, "")); err != nil {
		t.Fatal(err)
	}
	account, err := s.GetAccount(t.Context(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	account.Disabled = true
	if _, err := s.UpdateAccount(t.Context(), *account); err != nil {
		t.Fatal(err)
	}
	close(release)
	if err := <-result; !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("disabled account issued a session: %v", err)
	}
}

func TestLoginAdmissionIsBoundedAcrossUsernames(t *testing.T) {
	s := setup(t)
	s.loginWork <- struct{}{}
	s.loginWork <- struct{}{}
	// Admission must reject before touching the identity lock.
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, user := range []string{"admin", "new-username", "another-username"} {
		if _, err := s.Login(t.Context(), httptest.NewRecorder(), user, password); !errors.Is(err, ErrRateLimited) {
			t.Fatal(user, err)
		}
	}
}
