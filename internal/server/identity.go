// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/api"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/version"
	"net/http"
	"strconv"
	"strings"
)

func accountAPI(a domain.Account) api.Account {
	updated := a.UpdatedAt
	if updated.IsZero() {
		updated = a.PasswordChangedAt
	}
	if updated.IsZero() {
		updated = a.CreatedAt
	}
	return api.Account{Name: a.Name, Enabled: !a.Disabled, MustChangePassword: a.MustChangePassword, CreatedAt: a.CreatedAt, UpdatedAt: updated}
}
func tokenAPI(t domain.ApiToken) api.APIToken {
	return api.APIToken{Id: t.ID, Name: t.Name, Owner: t.Owner, CreatedAt: t.CreatedAt, ExpiresAt: t.ExpiresAt, LastUsedAt: t.LastUsedAt, Prefix: "kk_"}
}
func currentUser(p *auth.Principal) api.CurrentUser {
	groups := []string{}
	for _, v := range p.Subjects {
		if strings.HasPrefix(v, "group:") {
			groups = append(groups, strings.TrimPrefix(v, "group:"))
		}
	}
	u := api.CurrentUser{Name: p.Account, Subject: "user:" + p.Account, Groups: groups, MustChangePassword: p.MustChangePassword}
	if p.CSRFToken != "" {
		u.CsrfToken = &p.CSRFToken
	}
	return u
}
func sessionAPI(si *auth.SessionInfo) api.Session {
	return api.Session{Account: currentUser(&auth.Principal{Account: si.Account.Name, MustChangePassword: si.Account.MustChangePassword, CSRFToken: si.CSRFToken}), CsrfToken: &si.CSRFToken, ExpiresAt: si.ExpiresAt}
}
func (s *Server) identity(ctx context.Context, name string, in input) response {
	p := principal(ctx)
	switch name {
	case "Login":
		b := in.Body.(*api.LoginRequest)
		return response{noStore: true, serve: func(w http.ResponseWriter) error {
			si, err := s.Auth.LoginHTTP(w, request(ctx), b.Username, value(b.Password))
			if err != nil {
				var limited *auth.RateLimitError
				if errors.As(err, &limited) {
					w.Header().Set("Retry-After", strconv.Itoa(limited.RetryAfterSeconds))
					return failDetails(429, "rate_limited", "Login temporarily throttled; wait before trying again", map[string]any{"retryAfterSeconds": limited.RetryAfterSeconds}).write(w)
				}
				return fail(401, "unauthorized", "Invalid credentials").write(w)
			}
			return ok(sessionAPI(si)).write(w)
		}}
	case "RefreshSession":
		return response{noStore: true, serve: func(w http.ResponseWriter) error {
			si, err := s.Auth.Refresh(ctx, w, request(ctx))
			if err != nil {
				return s.failure(err).write(w)
			}
			return ok(sessionAPI(si)).write(w)
		}}
	case "Logout":
		return response{noStore: true, serve: func(w http.ResponseWriter) error {
			if p != nil && p.Via == "token" {
				if err := s.recordAudit(ctx, domain.Event{Resource: "auth", Target: p.Account, Type: "Normal", Reason: "LogoutSucceeded", Message: "API token client logged out (token remains valid)", Actor: p.Actor()}); err != nil {
					return s.failure(err).write(w)
				}
				w.WriteHeader(204)
				return nil
			}
			if err := s.Auth.Logout(ctx, w, request(ctx)); err != nil {
				return s.failure(err).write(w)
			}
			w.WriteHeader(204)
			return nil
		}}
	case "ChangePassword":
		b := in.Body.(*api.ChangePasswordRequest)
		if err := s.Auth.ChangePassword(ctx, p.Account, value(b.CurrentPassword), value(b.NewPassword)); err != nil {
			return s.failure(err)
		}
		return response{status: 204, noStore: true}
	case "GetCurrentUser":
		return response{status: 200, body: currentUser(p), noStore: true}
	case "OidcLogin":
		return response{noStore: true, serve: func(w http.ResponseWriter) error { s.Auth.OIDCLoginHandler(w, request(ctx)); return nil }}
	case "OidcCallback":
		return response{noStore: true, serve: func(w http.ResponseWriter) error { s.Auth.OIDCCallbackHandler(w, request(ctx)); return nil }}
	case "ListAccounts":
		items, err := s.Auth.ListAccounts(ctx)
		out := make([]api.Account, 0, len(items))
		for _, a := range items {
			out = append(out, accountAPI(a))
		}
		return listResponse(s, ctx, out, err)
	case "GetAccount":
		a, err := s.Auth.GetAccount(ctx, in.Account)
		if err != nil {
			return s.failure(err)
		}
		return ok(accountAPI(*a))
	case "CreateAccount":
		b := in.Body.(*api.CreateAccountRequest)
		a, err := s.Auth.CreateAccount(ctx, domain.Account{Name: b.Name, Disabled: b.Enabled != nil && !*b.Enabled, MustChangePassword: b.MustChangePassword == nil || *b.MustChangePassword}, value(b.Password))
		if err != nil {
			return s.failure(err)
		}
		return response{status: 201, body: accountAPI(*a)}
	case "ReplaceAccount":
		b := in.Body.(*api.UpdateAccountRequest)
		a, err := s.Auth.GetAccount(ctx, in.Account)
		if err != nil {
			return s.failure(err)
		}
		a.Disabled = !b.Enabled
		a.MustChangePassword = b.MustChangePassword
		a, err = s.Auth.UpdateAccount(ctx, *a)
		if err != nil {
			return s.failure(err)
		}
		return ok(accountAPI(*a))
	case "DeleteAccount":
		if err := s.Auth.DeleteAccount(ctx, in.Account); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	case "ResetAccountPassword":
		b := in.Body.(*api.ResetPasswordRequest)
		if err := s.Auth.ResetPassword(ctx, in.Account, value(b.Password)); err != nil {
			return s.failure(err)
		}
		if b.MustChangePassword != nil {
			a, err := s.Auth.GetAccount(ctx, in.Account)
			if err != nil {
				return s.failure(err)
			}
			a.MustChangePassword = *b.MustChangePassword
			if _, err = s.Auth.UpdateAccount(ctx, *a); err != nil {
				return s.failure(err)
			}
		}
		return response{status: 204, noStore: true}
	case "ListAPITokens":
		owner := p.Account
		if s.allowed(ctx, "api-tokens", "read", "*") {
			owner = ""
		}
		items, err := s.Auth.ListTokens(ctx, owner)
		out := make([]api.APIToken, 0, len(items))
		for _, t := range items {
			out = append(out, tokenAPI(t))
		}
		return listResponse(s, ctx, out, err)
	case "CreateAPIToken":
		b := in.Body.(*api.CreateAPITokenRequest)
		secret, t, err := s.Auth.CreateToken(ctx, p.Account, b.Name, b.ExpiresAt)
		if err != nil {
			return s.failure(err)
		}
		return response{status: 201, body: api.CreatedAPIToken{Secret: &secret, Token: tokenAPI(t)}, noStore: true}
	case "GetAPIToken":
		t, err := s.Auth.GetToken(ctx, in.Token)
		if err != nil {
			return s.failure(err)
		}
		if t.Owner != p.Account && !s.allowed(ctx, "api-tokens", "read", "*") {
			return fail(403, "forbidden", "Permission denied")
		}
		return ok(tokenAPI(*t))
	case "ReplaceAPIToken":
		b := in.Body.(*api.UpdateAPITokenRequest)
		t, err := s.Auth.GetToken(ctx, in.Token)
		if err != nil {
			return s.failure(err)
		}
		t.Name = b.Name
		t.ExpiresAt = b.ExpiresAt
		if t.Owner != p.Account && !s.allowed(ctx, "api-tokens", "update", "*") {
			return fail(403, "forbidden", "Permission denied")
		}
		t, err = s.Auth.UpdateToken(ctx, *t)
		if err != nil {
			return s.failure(err)
		}
		return ok(tokenAPI(*t))
	case "DeleteAPIToken":
		t, err := s.Auth.GetToken(ctx, in.Token)
		if err != nil {
			return s.failure(err)
		}
		if t.Owner != p.Account && !s.allowed(ctx, "api-tokens", "delete", "*") {
			return fail(403, "forbidden", "Permission denied")
		}
		if err := s.Auth.DeleteToken(ctx, in.Token); err != nil {
			return s.failure(err)
		}
		return response{status: 204}
	case "GetPermissions":
		out := api.Permissions{Subject: "user:" + p.Account, Groups: currentUser(p).Groups, Permissions: []api.Permission{}}
		for _, rule := range s.RBAC.Permissions(p.Subjects) {
			v := rule.Values
			if rule.PType == "p" && len(v) >= 5 {
				out.Permissions = append(out.Permissions, api.Permission{Resource: v[1], Action: v[2], Object: v[3], Effect: api.PermissionEffect(v[4])})
			}
		}
		return ok(out)
	case "CheckPermission":
		b := in.Body.(*api.CanRequest)
		subjects := p.Subjects
		if b.Subject != nil && *b.Subject != "user:"+p.Account {
			if !s.allowed(ctx, "rbac", "read", "*") {
				return fail(403, "forbidden", "Permission denied")
			}
			subjects = []string{*b.Subject}
		}
		return ok(api.CanResponse{Allowed: s.RBAC.Can(subjects, b.Resource, string(b.Action), b.Object)})
	case "GetRBACPolicy":
		return ok(api.RBACPolicy{Policy: s.RBAC.Policy()})
	case "ReplaceRBACPolicy":
		b := in.Body.(*api.RBACPolicy)
		if err := s.RBAC.ReplacePolicy(ctx, b.Policy); err != nil {
			return s.failure(err)
		}
		return ok(api.RBACPolicy{Policy: s.RBAC.Policy()})
	case "GetSettings":
		cfg, err := s.Auth.Settings(ctx)
		if err != nil {
			return s.failure(err)
		}
		return ok(s.settingsAPI(*cfg))
	case "ReplaceSettings":
		b := in.Body.(*api.Settings)
		cfg, err := s.Auth.Settings(ctx)
		if err != nil {
			return s.failure(err)
		}
		o := b.Oidc
		cfg.IssuerURL = value(o.IssuerURL)
		cfg.ClientID = value(o.ClientId)
		cfg.AutoCreate = o.Enabled
		if o.ClientSecret != nil {
			cfg.ClientSecret = *o.ClientSecret
		}
		if o.Scopes != nil {
			cfg.Scopes = *o.Scopes
		}
		cfg.GroupsClaim = value(o.GroupsClaim)
		if !o.Enabled {
			cfg.IssuerURL = ""
		}
		if err = s.Auth.UpdateSettings(ctx, *cfg); err != nil {
			return s.failure(err)
		}
		return ok(s.settingsAPI(*cfg))
	case "GetVersion":
		cfg, err := s.Auth.Settings(ctx)
		if err != nil {
			return s.failure(err)
		}
		enabled := cfg.IssuerURL != ""
		return ok(api.Version{Version: version.VersionLabel(), Commit: ptr(version.CommitLabel()), BuildDate: &version.BuildDate, ApiVersion: "v1", OidcEnabled: &enabled})
	}
	return fail(500, "internal_error", "Unknown identity operation")
}
func (s *Server) settingsAPI(c auth.OIDCSettings) api.Settings {
	enabled := c.IssuerURL != ""
	redirect := strings.TrimRight(s.PublicURL, "/") + "/api/v1/auth/oidc/callback"
	return api.Settings{Oidc: api.OIDCConfig{Enabled: enabled, IssuerURL: &c.IssuerURL, ClientId: &c.ClientID, GroupsClaim: &c.GroupsClaim, Scopes: ptr(nonNil(c.Scopes)), RedirectURL: &redirect}}
}
