// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestRestrictedTokenCreationLifetime(t *testing.T) {
	s, _, admin := testServer(t)
	expiry := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	restrictions := &domain.TokenRestrictions{AllowCredentialReveal: true}
	parent, _, err := s.Auth.CreateToken(t.Context(), "admin", "parent", &expiry, restrictions)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name    string
		expires *time.Time
		status  int
	}{
		{"earlier", timePointer(expiry.Add(-time.Minute)), 201}, {"equal", &expiry, 201},
		{"later", timePointer(expiry.Add(time.Minute)), 403}, {"unlimited", nil, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			request := map[string]any{"name": tc.name, "restrictions": restrictions}
			if tc.expires != nil {
				request["expiresAt"] = tc.expires
			}
			body, _ := json.Marshal(request)
			w := call(s, "POST", "/api/v1/api-tokens", string(body), parent)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
	// Unrestricted callers are not bound by another token's lifetime.
	if w := call(s, "POST", "/api/v1/api-tokens", `{"name":"unlimited"}`, admin); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
	noExpiry, _, err := s.Auth.CreateToken(t.Context(), "admin", "unlimited-parent", nil, restrictions)
	if err != nil {
		t.Fatal(err)
	}
	if w := call(s, "POST", "/api/v1/api-tokens", `{"name":"child","restrictions":{}}`, noExpiry); w.Code != 201 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestRestrictedTokenUpdateAuthorityAndLifetime(t *testing.T) {
	s, _, admin := testServer(t)
	expiry := time.Now().UTC().Truncate(time.Second).Add(time.Hour)
	restriction := &domain.TokenRestrictions{Permissions: []string{"api-tokens:*"}, AllowCredentialReveal: true}
	parent, _, err := s.Auth.CreateToken(t.Context(), "admin", "parent", &expiry, restriction)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		name         string
		restrictions *domain.TokenRestrictions
		expires      *time.Time
		status       int
	}{
		{"equal", restriction, &expiry, 200},
		{"earlier", &domain.TokenRestrictions{ReadOnly: true, Permissions: []string{"api-tokens:read"}}, timePointer(expiry.Add(-time.Minute)), 200},
		{"extended", restriction, timePointer(expiry.Add(time.Minute)), 403},
		{"removed", restriction, nil, 403},
		{"unrestricted-sibling", nil, &expiry, 403},
		{"broader-sibling", &domain.TokenRestrictions{AllowCredentialReveal: true}, &expiry, 403},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, target, err := s.Auth.CreateToken(t.Context(), "admin", tc.name, &expiry, tc.restrictions)
			if err != nil {
				t.Fatal(err)
			}
			body, _ := json.Marshal(map[string]any{"name": "renamed", "expiresAt": tc.expires})
			path := "/api/v1/api-tokens/" + target.ID
			w := call(s, "PUT", path, string(body), parent)
			if w.Code != tc.status {
				t.Fatal(w.Code, w.Body.String())
			}
			if tc.status == 403 {
				stored, err := s.Auth.GetToken(t.Context(), target.ID)
				if err != nil || stored.Name != tc.name || stored.ExpiresAt == nil || !stored.ExpiresAt.Equal(expiry) {
					t.Fatal("denial mutated token", stored, err)
				}
			}
			// An unrestricted owner can still rename broader siblings and remove expiry.
			if w = call(s, "PUT", path, `{"name":"unrestricted-update","expiresAt":null}`, admin); w.Code != 200 {
				t.Fatal(w.Code, w.Body.String())
			}
		})
	}
}

func timePointer(t time.Time) *time.Time { return &t }
