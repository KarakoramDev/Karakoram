// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"context"
	"crypto/subtle"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/golang-jwt/jwt/v5"
)

// Cookie names and CSRFHeader are shared with HTTP clients.
const (
	AccessCookie  = "karakoram_access"
	RefreshCookie = "karakoram_refresh"
	CSRFCookie    = "karakoram_csrf"
	CSRFHeader    = "X-Karakoram-CSRF"
)

type accessClaims struct {
	jwt.RegisteredClaims
	SessionID       string `json:"sid"`
	PasswordVersion int64  `json:"pv"`
}

func equal(a, b string) bool {
	return a != "" && b != "" && subtle.ConstantTimeCompare([]byte(a), []byte(b)) == 1
}
func unsafe(method string) bool { return method != "GET" && method != "HEAD" && method != "OPTIONS" }
func csrf(r *http.Request, want string) error {
	c, err := r.Cookie(CSRFCookie)
	if err != nil || !equal(c.Value, want) || !equal(r.Header.Get(CSRFHeader), want) {
		return ErrCSRF
	}
	return nil
}
func (s *Service) cookie(w http.ResponseWriter, name, value, path string, httpOnly bool, expiry time.Time) {
	http.SetCookie(w, &http.Cookie{Name: name, Value: value, Path: path, HttpOnly: httpOnly, Secure: s.o.SecureCookies, SameSite: http.SameSiteStrictMode, Expires: expiry, MaxAge: int(expiry.Sub(s.now()).Seconds())})
}
func (s *Service) clear(w http.ResponseWriter) {
	for _, v := range []struct {
		name, path string
		only       bool
	}{{AccessCookie, "/", true}, {RefreshCookie, "/api/v1/auth", true}, {CSRFCookie, "/", false}} {
		http.SetCookie(w, &http.Cookie{Name: v.name, Path: v.path, Value: "", MaxAge: -1, Expires: time.Unix(1, 0), HttpOnly: v.only, Secure: s.o.SecureCookies, SameSite: http.SameSiteStrictMode})
	}
}
func (s *Service) issue(ctx context.Context, w http.ResponseWriter, a *domain.Account, extra []string, via string) (*SessionInfo, error) {
	id, err := random()
	if err != nil {
		return nil, err
	}
	secret, err := random()
	if err != nil {
		return nil, err
	}
	cs, err := random()
	if err != nil {
		return nil, err
	}
	token := id + "." + secret
	sess := domain.Session{ID: id, Owner: a.Name, CreatedAt: s.now(), ExpiresAt: s.now().Add(s.o.RefreshTTL), RefreshHash: secrets.Hash(token), CSRF: cs, Subjects: extra, Via: via}
	if err = s.o.Store.CreateSession(ctx, &sess); err != nil {
		return nil, err
	}
	message := "Local login succeeded"
	if via == "oidc" {
		message = "OIDC login succeeded"
	}
	if err = s.auditAuthentication(ctx, Principal{Account: a.Name, Via: via, OIDCIssuer: a.OIDCIssuer}, "LoginSucceeded", message); err != nil {
		return nil, err
	}
	return s.cookies(w, a, &sess, token)
}
func (s *Service) cookies(w http.ResponseWriter, a *domain.Account, se *domain.Session, refresh string) (*SessionInfo, error) {
	now := s.now()
	expiry := now.Add(s.o.AccessTTL)
	if expiry.After(se.ExpiresAt) {
		expiry = se.ExpiresAt
	}
	claims := accessClaims{RegisteredClaims: jwt.RegisteredClaims{Issuer: s.o.Issuer, Subject: a.Name, Audience: jwt.ClaimStrings{"karakoram"}, IssuedAt: jwt.NewNumericDate(now), NotBefore: jwt.NewNumericDate(now), ExpiresAt: jwt.NewNumericDate(expiry)}, SessionID: se.ID, PasswordVersion: a.PasswordChangedAt.UnixNano()}
	signed, err := jwt.NewWithClaims(jwt.SigningMethodEdDSA, claims).SignedString(s.key)
	if err != nil {
		return nil, err
	}
	s.cookie(w, AccessCookie, signed, "/", true, expiry)
	s.cookie(w, RefreshCookie, refresh, "/api/v1/auth", true, se.ExpiresAt)
	s.cookie(w, CSRFCookie, se.CSRF, "/", false, se.ExpiresAt)
	return &SessionInfo{Account: clean(*a), CSRFToken: se.CSRF, ExpiresAt: expiry}, nil
}

// Login applies account+IP exponential backoff and issues the local session.
// After five failures delay starts at one second and doubles to fifteen minutes.
func (s *Service) Login(ctx context.Context, w http.ResponseWriter, user, password string) (*SessionInfo, error) {
	// Only bounded modeled account names may reach hashing, backoff or auditing.
	// Malformed input is rejected without persisting untrusted identity text.
	if len(user) > 64 || domain.ValidateAccountName(user) != nil {
		return nil, ErrUnauthenticated
	}
	// Bound memory-intensive hashing across all usernames without blocking the
	// shared identity lock or queuing unbounded unauthenticated work.
	select {
	case s.loginWork <- struct{}{}:
		defer func() { <-s.loginWork }()
	default:
		return nil, &RateLimitError{RetryAfterSeconds: 1}
	}
	s.mu.Lock()
	ip, _ := ctx.Value(clientIPKey{}).(string)
	key := user + "\x00" + ip
	now := s.now()
	for k, v := range s.attempts {
		if now.Sub(v.Last) > 24*time.Hour {
			delete(s.attempts, k)
		}
	}
	v := s.attempts[key]
	if v.Until.After(now) {
		s.mu.Unlock()
		return nil, &RateLimitError{RetryAfterSeconds: int((v.Until.Sub(now) + time.Second - 1) / time.Second)}
	}
	a, err := s.o.Store.GetAccount(ctx, user)
	hash := s.dummy
	if err == nil && !a.OIDC {
		hash = a.PasswordHash
	}
	s.mu.Unlock()
	valid := s.verifyPassword(hash, password)
	s.mu.Lock()
	defer s.mu.Unlock()
	// Account lifecycle changes may have occurred while hashing was in flight.
	current, currentErr := s.o.Store.GetAccount(ctx, user)
	v = s.attempts[key]
	now = s.now()
	if err != nil || currentErr != nil || !valid || current.Disabled || current.OIDC || current.PasswordHash != hash || !current.CreatedAt.Equal(a.CreatedAt) {
		v.Failures++
		v.Last = now
		if v.Failures >= 5 {
			shift := v.Failures - 5
			if shift > 10 {
				shift = 10
			}
			delay := time.Second * time.Duration(1<<shift)
			if delay > 15*time.Minute {
				delay = 15 * time.Minute
			}
			v.Until = now.Add(delay)
		}
		s.attempts[key] = v
		if auditErr := s.auditAuthentication(ctx, Principal{Account: user}, "LoginFailed", "Local login failed"); auditErr != nil {
			return nil, errors.Join(ErrUnauthenticated, auditErr)
		}
		return nil, ErrUnauthenticated
	}
	delete(s.attempts, key)
	return s.issue(ctx, w, current, nil, "session")
}

// Authenticate accepts a bearer API token, otherwise validates access cookies,
// persisted session state, account lifecycle and CSRF on unsafe methods.
func (s *Service) Authenticate(r *http.Request) (*Principal, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	ctx := r.Context()
	auth := r.Header.Get("Authorization")
	if auth != "" {
		parts := strings.Fields(auth)
		if len(parts) != 2 || !strings.EqualFold(parts[0], "Bearer") || !strings.HasPrefix(parts[1], "kk_") {
			return nil, ErrUnauthenticated
		}
		t, err := s.o.Store.FindApiTokenByHash(ctx, secrets.Hash(parts[1]))
		if err != nil || t.ExpiresAt != nil && !t.ExpiresAt.After(s.now()) {
			return nil, ErrUnauthenticated
		}
		a, err := s.o.Store.GetAccount(ctx, t.Owner)
		if err != nil || a.Disabled || a.MustChangePassword || t.CreatedAt.Before(a.PasswordChangedAt) || t.CreatedAt.Before(a.CreatedAt) {
			return nil, ErrUnauthenticated
		}
		if err = s.o.Store.TouchApiToken(ctx, t.ID, s.now()); err != nil {
			return nil, err
		}
		return &Principal{Account: a.Name, Subjects: subjects(a, nil), Via: "token", TokenID: t.ID, TokenName: t.Name, OIDCIssuer: a.OIDCIssuer, MustChangePassword: a.MustChangePassword}, nil
	}
	c, err := r.Cookie(AccessCookie)
	if err != nil {
		return nil, ErrUnauthenticated
	}
	claims := &accessClaims{}
	_, err = jwt.ParseWithClaims(c.Value, claims, func(t *jwt.Token) (interface{}, error) { return s.key.Public(), nil }, jwt.WithValidMethods([]string{"EdDSA"}), jwt.WithIssuer(s.o.Issuer), jwt.WithAudience("karakoram"), jwt.WithExpirationRequired(), jwt.WithIssuedAt(), jwt.WithTimeFunc(s.now))
	if err != nil {
		return nil, ErrUnauthenticated
	}
	a, err := s.o.Store.GetAccount(ctx, claims.Subject)
	if err != nil || a.Disabled || claims.PasswordVersion != a.PasswordChangedAt.UnixNano() {
		return nil, ErrUnauthenticated
	}
	se, err := s.o.Store.GetSession(ctx, claims.SessionID)
	if err != nil || se.Owner != a.Name || se.Revoked || !se.ExpiresAt.After(s.now()) || se.CreatedAt.Before(a.PasswordChangedAt) || se.CreatedAt.Before(a.CreatedAt) {
		return nil, ErrUnauthenticated
	}
	if unsafe(r.Method) {
		if err = csrf(r, se.CSRF); err != nil {
			return nil, err
		}
	}
	return &Principal{Account: a.Name, Subjects: subjects(a, se.Subjects), Via: se.Via, OIDCIssuer: a.OIDCIssuer, MustChangePassword: a.MustChangePassword, CSRFToken: se.CSRF}, nil
}
func (s *Service) refreshSession(ctx context.Context, r *http.Request) (*domain.Session, string, error) {
	c, err := r.Cookie(RefreshCookie)
	if err != nil {
		return nil, "", ErrUnauthenticated
	}
	id, _, ok := strings.Cut(c.Value, ".")
	if !ok {
		return nil, "", ErrUnauthenticated
	}
	se, err := s.o.Store.GetSession(ctx, id)
	if err != nil || se.Revoked || !se.ExpiresAt.After(s.now()) {
		return nil, "", ErrUnauthenticated
	}
	if err = csrf(r, se.CSRF); err != nil {
		return nil, "", err
	}
	hash := secrets.Hash(c.Value)
	if !equal(hash, se.RefreshHash) {
		for _, old := range se.UsedRefreshHashes {
			if equal(old, hash) {
				se.Revoked = true
				if err = s.o.Store.UpdateSession(ctx, se); err != nil {
					return nil, "", err
				}
				break
			}
		}
		return nil, "", ErrUnauthenticated
	}
	return se, hash, nil
}

// Refresh consumes a token once, keeping its hash as a durable replay receipt.
// Family expiry is absolute rather than extended indefinitely by rotation.
func (s *Service) Refresh(ctx context.Context, w http.ResponseWriter, r *http.Request) (*SessionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	se, old, err := s.refreshSession(ctx, r)
	if err != nil {
		return nil, err
	}
	a, err := s.o.Store.GetAccount(ctx, se.Owner)
	if err != nil || a.Disabled || se.CreatedAt.Before(a.PasswordChangedAt) || se.CreatedAt.Before(a.CreatedAt) {
		return nil, ErrUnauthenticated
	}
	if a.MustChangePassword {
		return nil, ErrPasswordChangeRequired
	}
	secret, err := random()
	if err != nil {
		return nil, err
	}
	token := se.ID + "." + secret
	se.UsedRefreshHashes = append(se.UsedRefreshHashes, old)
	se.RefreshHash = secrets.Hash(token)
	// Both consumption history and the replacement hash share one SQLite write.
	if err = s.o.Store.UpdateSession(ctx, se); err != nil {
		return nil, err
	}
	if err = s.auditAuthentication(ctx, Principal{Account: a.Name, Via: se.Via, OIDCIssuer: a.OIDCIssuer}, "RefreshSessionSucceeded", "Session refreshed"); err != nil {
		return nil, err
	}
	return s.cookies(w, a, se, token)
}

// Logout revokes the refresh family and clears all three local cookies.
func (s *Service) Logout(ctx context.Context, w http.ResponseWriter, r *http.Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	se, _, err := s.refreshSession(ctx, r)
	if err != nil {
		if !errors.Is(err, ErrUnauthenticated) {
			return err
		}
		s.clear(w)
		return nil
	}
	principal := Principal{Account: se.Owner, Via: se.Via}
	if se.Via == "oidc" {
		account, err := s.o.Store.GetAccount(ctx, se.Owner)
		if err != nil {
			return err
		}
		principal.OIDCIssuer = account.OIDCIssuer
	}
	se.Revoked = true
	if err = s.o.Store.UpdateSession(ctx, se); err != nil {
		return err
	}
	s.clear(w)
	return s.auditAuthentication(ctx, principal, "LogoutSucceeded", "Logout succeeded")
}
