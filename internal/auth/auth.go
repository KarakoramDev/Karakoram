// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

// Package auth manages local and OIDC accounts, sealed Ed25519 signing keys,
// five-minute access cookies, thirty-day rotating refresh families and CSRF.
// Cookie authentication checks the persisted session on every request; password
// changes and disabled/deleted accounts invalidate access immediately. Refresh
// secrets and API tokens are persisted only as SHA-256 hashes. Access/refresh
// lifetimes may be configured through Options. A replay revokes the whole family.
// Login, refresh and authenticated logout append durable auth-resource events.
// Failed local logins record only validated attempted account names, never
// passwords or addresses; backoff/admission denials do not produce audit floods.
// OIDC failures are attributed only to verified, matching persisted identities.
// Actor identities include the persisted OIDC issuer and public token metadata,
// so provider identities and separate API tokens remain distinguishable.
package auth

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

// ErrUnauthenticated deliberately does not disclose account existence.
var ErrUnauthenticated = errors.New("invalid credentials or expired authentication")

// ErrCSRF indicates a missing or mismatched double-submit token.
var ErrCSRF = errors.New("invalid CSRF token")

// ErrRateLimited indicates exponential login backoff is active.
var ErrRateLimited = errors.New("login temporarily locked")

// RateLimitError carries a secret-independent retry delay for login admission.
type RateLimitError struct {
	RetryAfterSeconds int
}

func (e *RateLimitError) Error() string { return ErrRateLimited.Error() }
func (e *RateLimitError) Unwrap() error { return ErrRateLimited }

// ErrInvalid identifies malformed account, password, token or OIDC input.
var ErrInvalid = errors.New("invalid authentication input")

// ErrPasswordChangeRequired restricts sessions pending a mandatory change.
var ErrPasswordChangeRequired = errors.New("password change required")

// Options configures persisted authentication and externally visible URLs.
type Options struct {
	Store                 *store.Store
	Sealer                *secrets.Sealer
	Issuer                string
	AccessTTL, RefreshTTL time.Duration
	SecureCookies         bool
	PublicURL             string
	Logger                *slog.Logger
}

// Principal is the authenticated identity used by policy enforcement.
type Principal struct {
	Account            string
	AccountCreatedAt   time.Time
	Subjects           []string
	Via                string
	MustChangePassword bool
	TokenID            string
	TokenName          string
	OIDCIssuer         string
	CSRFToken          string
	Restrictions       *domain.TokenRestrictions
}

// Actor returns the persisted account identity scoped to its OIDC issuer,
// distinguishing API tokens by public name and ID without credential material.
func (p Principal) Actor() string {
	if p.Via == "oauth" {
		return p.Account + " (mcp: " + p.TokenName + ")"
	}
	if p.Via == "token" {
		if p.OIDCIssuer != "" {
			if p.TokenName == "" {
				return p.Account + " (oidc: " + p.OIDCIssuer + ") (token: [" + p.TokenID + "])"
			}
			return p.Account + " (oidc: " + p.OIDCIssuer + ") (token: " + p.TokenName + " [" + p.TokenID + "])"
		}
		if p.TokenName == "" {
			return p.Account + " (token: [" + p.TokenID + "])"
		}
		return p.Account + " (token: " + p.TokenName + " [" + p.TokenID + "])"
	}
	if p.OIDCIssuer != "" {
		return p.Account + " (oidc: " + p.OIDCIssuer + ")"
	}
	return p.Account
}

func (s *Service) auditAuthentication(ctx context.Context, principal Principal, reason, message string) error {
	kind := "Normal"
	if reason == "LoginFailed" {
		kind = "Warning"
	}
	event := domain.Event{Resource: "auth", Target: principal.Account, Type: kind, Reason: reason, Actor: principal.Actor(), Message: message, CreatedAt: s.now()}
	return s.o.Store.AppendEvent(ctx, &event)
}

// SessionInfo is returned with newly issued cookies and includes the CSRF token.
type SessionInfo struct {
	Account   domain.Account `json:"account"`
	CSRFToken string         `json:"csrfToken"`
	ExpiresAt time.Time      `json:"expiresAt"`
}

// Service owns signing keys and serializes identity/session lifecycle changes.
type Service struct {
	o              Options
	mu             sync.Mutex
	key            ed25519.PrivateKey
	dummy          string
	attempts       map[string]attempt
	loginWork      chan struct{}
	tokenUsage     chan struct{}
	verifyPassword func(string, string) bool
	now            func() time.Time
}
type attempt struct {
	Failures    int
	Until, Last time.Time
}
type clientIPKey struct{}

// WithClientIP attaches the direct peer address to a login context. Forwarded
// headers must only be used by callers with an explicit trusted-proxy policy.
func WithClientIP(ctx context.Context, ip string) context.Context {
	return context.WithValue(ctx, clientIPKey{}, ip)
}

// LoginHTTP derives the rate-limit address from RemoteAddr, never proxy headers.
func (s *Service) LoginHTTP(w http.ResponseWriter, r *http.Request, user, password string) (*SessionInfo, error) {
	ip, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		ip = r.RemoteAddr
	}
	return s.Login(WithClientIP(r.Context(), ip), w, user, password)
}

// New loads the sealed persistent signing key or generates one on first start.
func New(o Options) (*Service, error) {
	if o.Store == nil || o.Sealer == nil {
		return nil, errors.New("store and sealer are required")
	}
	if o.Issuer == "" {
		o.Issuer = "karakoram"
	}
	if o.AccessTTL == 0 {
		o.AccessTTL = 5 * time.Minute
	}
	if o.RefreshTTL == 0 {
		o.RefreshTTL = 30 * 24 * time.Hour
	}
	if o.AccessTTL < time.Second || o.RefreshTTL <= o.AccessTTL {
		return nil, errors.New("invalid session lifetimes")
	}
	s := &Service{o: o, attempts: map[string]attempt{}, loginWork: make(chan struct{}, 2), tokenUsage: make(chan struct{}, 1), verifyPassword: VerifyPassword, now: func() time.Time { return time.Now().UTC() }}
	raw, err := o.Store.GetSetting(context.Background(), "auth/signing-key")
	if errors.Is(err, store.ErrNotFound) {
		_, s.key, err = ed25519.GenerateKey(rand.Reader)
		if err != nil {
			return nil, err
		}
		sealed, err := o.Sealer.SealFor("auth/signing-key", s.key)
		if err != nil {
			return nil, err
		}
		raw, _ = json.Marshal(sealed)
		if err = o.Store.SetSetting(context.Background(), "auth/signing-key", raw); err != nil {
			return nil, err
		}
	} else if err != nil {
		return nil, err
	} else {
		var sealed string
		if err = json.Unmarshal(raw, &sealed); err != nil {
			return nil, err
		}
		key, err := o.Sealer.OpenFor("auth/signing-key", sealed)
		if err != nil {
			return nil, err
		}
		if len(key) != ed25519.PrivateKeySize {
			return nil, errors.New("invalid signing key")
		}
		s.key = ed25519.PrivateKey(key)
	}
	s.dummy, err = HashPassword("dummy-password-never-valid")
	return s, err
}
func random() (string, error) {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(b), nil
}

func clean(a domain.Account) domain.Account {
	a.PasswordHash = ""
	a.OIDCIssuer = ""
	a.OIDCSubject = ""
	return a
}
func subjects(a *domain.Account, extra []string) []string {
	out := []string{"user:" + a.Name}
	for _, r := range a.Roles {
		if !strings.HasPrefix(r, "role:") {
			r = "role:" + r
		}
		out = append(out, r)
	}
	return append(out, extra...)
}

// Bootstrap creates the initial administrator once and returns only a generated
// password. Supplied passwords are never returned; every initial admin must change.
func (s *Service) Bootstrap(ctx context.Context, password string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	accounts, err := s.o.Store.ListAccounts(ctx)
	if err != nil || len(accounts) > 0 {
		return "", err
	}
	generated := ""
	if password == "" {
		password, err = secrets.Password()
		if err != nil {
			return "", err
		}
		generated = password
	}
	hash, err := HashPassword(password)
	if err != nil {
		return "", err
	}
	now := s.now()
	a := domain.Account{Name: "admin", DisplayName: "Administrator", Roles: []string{"role:admin"}, PasswordHash: hash, MustChangePassword: true, CreatedAt: now, PasswordChangedAt: now, UpdatedAt: now}
	return generated, s.o.Store.CreateAccount(ctx, &a)
}

// CreateAccount creates a local-password identity; OIDC identities are only
// provisioned by verified callbacks.
func (s *Service) CreateAccount(ctx context.Context, a domain.Account, password string) (*domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if domain.ValidateAccountName(a.Name) != nil {
		return nil, domain.InvalidField("name", domain.ReasonInvalid, "name must be 1–64 characters, start with a letter, and contain only letters, digits, _, ., @ or -")
	}
	if a.OIDC {
		return nil, domain.InvalidField("name", domain.ReasonInvalid, "name must identify a local account")
	}
	if err := validateAccountPassword("password", password); err != nil {
		return nil, err
	}
	hash, err := HashPassword(password)
	if err != nil {
		return nil, err
	}
	a.PasswordHash = hash
	a.CreatedAt = s.now()
	a.PasswordChangedAt = a.CreatedAt
	a.UpdatedAt = a.CreatedAt
	a.OIDCIssuer = ""
	a.OIDCSubject = ""
	if err = s.o.Store.CreateAccount(ctx, &a); err != nil {
		if errors.Is(err, store.ErrConflict) {
			return nil, domain.Conflict("account", a.Name, domain.ReasonAlreadyExists, err, "account %q already exists", a.Name)
		}
		return nil, err
	}
	a = clean(a)
	return &a, nil
}

// ListAccounts returns accounts without credentials or provider identity keys.
func (s *Service) ListAccounts(ctx context.Context) ([]domain.Account, error) {
	v, err := s.o.Store.ListAccounts(ctx)
	for i := range v {
		v[i] = clean(v[i])
	}
	return v, err
}

// GetAccount returns a credential-free account.
func (s *Service) GetAccount(ctx context.Context, name string) (*domain.Account, error) {
	a, err := s.o.Store.GetAccount(ctx, name)
	if err != nil {
		return nil, err
	}
	*a = clean(*a)
	return a, nil
}

// UpdateAccount updates display name, roles, disabled and must-change flags.
func (s *Service) UpdateAccount(ctx context.Context, a domain.Account) (*domain.Account, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	old, err := s.o.Store.GetAccount(ctx, a.Name)
	if err != nil {
		return nil, err
	}
	old.DisplayName = a.DisplayName
	old.Roles = append([]string(nil), a.Roles...)
	old.Disabled = a.Disabled
	old.MustChangePassword = a.MustChangePassword
	old.UpdatedAt = s.now()
	if err = s.o.Store.UpdateAccount(ctx, old); err != nil {
		return nil, err
	}
	*old = clean(*old)
	return old, nil
}

// DeleteAccount removes the owner and all of its persisted sessions and tokens.
// The account is removed first so partial cleanup still fails authentication closed.
func (s *Service) DeleteAccount(ctx context.Context, name string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	sessions, err := s.o.Store.ListSessions(ctx)
	if err != nil {
		return err
	}
	tokens, err := s.o.Store.ListApiTokens(ctx)
	if err != nil {
		return err
	}
	if err = s.o.Store.DeleteAccount(ctx, name); err != nil {
		return err
	}
	var cleanup error
	for _, se := range sessions {
		if se.Owner == name {
			cleanup = errors.Join(cleanup, s.o.Store.DeleteSession(ctx, se.ID))
		}
	}
	for _, token := range tokens {
		if token.Owner == name {
			cleanup = errors.Join(cleanup, s.o.Store.DeleteApiToken(ctx, token.ID))
		}
	}
	return cleanup
}

// ChangePassword requires the current local password and invalidates old tokens.
func (s *Service) ChangePassword(ctx context.Context, name, old, password string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.o.Store.GetAccount(ctx, name)
	if err != nil {
		return err
	}
	if a.Disabled || a.OIDC || !VerifyPassword(a.PasswordHash, old) {
		return domain.InvalidField("currentPassword", domain.ReasonInvalid, "currentPassword is invalid for this local account")
	}
	if old == password {
		return domain.InvalidField("newPassword", domain.ReasonReused, "newPassword must differ from the current password")
	}
	return s.setPassword(ctx, a, password, false)
}

// ResetPassword supports offline administrative recovery and forces a change.
func (s *Service) ResetPassword(ctx context.Context, name, password string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.o.Store.GetAccount(ctx, name)
	if err != nil {
		return err
	}
	if a.OIDC {
		return fmt.Errorf("%w: OIDC account has no local password", ErrInvalid)
	}
	return s.setPassword(ctx, a, password, true)
}
func (s *Service) setPassword(ctx context.Context, a *domain.Account, password string, must bool) error {
	if err := validateAccountPassword("newPassword", password); err != nil {
		return err
	}
	hash, err := HashPassword(password)
	if err != nil {
		return err
	}
	a.PasswordHash = hash
	a.PasswordChangedAt = s.now()
	a.UpdatedAt = a.PasswordChangedAt
	a.MustChangePassword = must
	return s.o.Store.UpdateAccount(ctx, a)
}

func validateAccountPassword(field, password string) error {
	if ValidatePassword(password) != nil {
		return domain.InvalidField(field, domain.ReasonOutOfRange, "%s must contain at least 12 characters and at most 1024 bytes", field)
	}
	return nil
}

// CreateToken issues a kk_-prefixed secret exactly once.
func (s *Service) CreateToken(ctx context.Context, owner, name string, expires *time.Time, restrictions *domain.TokenRestrictions) (string, domain.ApiToken, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	a, err := s.o.Store.GetAccount(ctx, owner)
	if err != nil {
		return "", domain.ApiToken{}, err
	}
	if a.Disabled || a.MustChangePassword {
		return "", domain.ApiToken{}, ErrUnauthenticated
	}
	name, err = validateToken(name, expires, s.now())
	if err != nil {
		return "", domain.ApiToken{}, err
	}
	restrictions, err = NormalizeRestrictions(restrictions)
	if err != nil {
		return "", domain.ApiToken{}, err
	}
	secret, err := secrets.Token()
	if err != nil {
		return "", domain.ApiToken{}, err
	}
	id, err := random()
	if err != nil {
		return "", domain.ApiToken{}, err
	}
	t := domain.ApiToken{ID: id, Name: name, Owner: owner, Hash: secrets.Hash(secret), ExpiresAt: expires, CreatedAt: s.now(), Restrictions: restrictions}
	if err = s.o.Store.CreateApiToken(ctx, &t); err != nil {
		return "", domain.ApiToken{}, err
	}
	t.Hash = ""
	return secret, t, nil
}

// ListTokens returns metadata, optionally restricted to one owner.
func (s *Service) ListTokens(ctx context.Context, owner string) ([]domain.ApiToken, error) {
	v, err := s.o.Store.ListApiTokens(ctx)
	out := make([]domain.ApiToken, 0, len(v))
	for _, t := range v {
		if owner == "" || t.Owner == owner {
			t.Hash = ""
			out = append(out, t)
		}
	}
	return out, err
}

// GetToken returns token metadata without its hash.
func (s *Service) GetToken(ctx context.Context, id string) (*domain.ApiToken, error) {
	t, err := s.o.Store.GetApiToken(ctx, id)
	if err != nil {
		return nil, err
	}
	t.Hash = ""
	return t, nil
}

// UpdateToken changes display name and expiry, never ownership or secret.
func (s *Service) UpdateToken(ctx context.Context, t domain.ApiToken) (*domain.ApiToken, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var err error
	t.Name, err = validateToken(t.Name, t.ExpiresAt, s.now())
	if err != nil {
		return nil, err
	}
	old, err := s.o.Store.GetApiToken(ctx, t.ID)
	if err != nil {
		return nil, err
	}
	old.Name = t.Name
	old.ExpiresAt = t.ExpiresAt
	if err = s.o.Store.UpdateApiToken(ctx, old); err != nil {
		return nil, err
	}
	old.Hash = ""
	return old, nil
}

func validateToken(name string, expires *time.Time, now time.Time) (string, error) {
	name = strings.TrimSpace(name)
	if name == "" {
		return "", domain.InvalidField("name", domain.ReasonRequired, "name is required and must not be blank")
	}
	if utf8.RuneCountInString(name) > 128 {
		return "", domain.InvalidField("name", domain.ReasonOutOfRange, "name must contain 1–128 characters after trimming whitespace")
	}
	if expires != nil && !expires.After(now) {
		return "", domain.InvalidField("expiresAt", domain.ReasonOutOfRange, "expiresAt must be in the future")
	}
	return name, nil
}

// DeleteToken revokes an API token immediately.
func (s *Service) DeleteToken(ctx context.Context, id string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.o.Store.DeleteApiToken(ctx, id)
}
