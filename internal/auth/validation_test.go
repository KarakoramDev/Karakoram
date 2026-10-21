// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"errors"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func requireField(t *testing.T, err error, field, reason string) {
	t.Helper()
	var got *domain.FieldError
	if !errors.As(err, &got) || got.Field != field || got.Reason != reason || !strings.Contains(got.Message, field) {
		t.Fatalf("want %s/%s, got %v", field, reason, err)
	}
}

func TestPasswordChangesRejectReuseAndPreserveMandatoryChange(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	for _, forced := range []bool{false, true} {
		a, err := s.GetAccount(ctx, "admin")
		if err != nil {
			t.Fatal(err)
		}
		a.MustChangePassword = forced
		if _, err = s.UpdateAccount(ctx, *a); err != nil {
			t.Fatal(err)
		}
		for _, tc := range []struct{ current, next, field, reason string }{
			{password, password, "newPassword", domain.ReasonReused},
			{"incorrect", "a-different-password", "currentPassword", domain.ReasonInvalid},
			{password, "short", "newPassword", domain.ReasonOutOfRange},
		} {
			requireField(t, s.ChangePassword(ctx, "admin", tc.current, tc.next), tc.field, tc.reason)
			stored, err := s.GetAccount(ctx, "admin")
			if err != nil || stored.MustChangePassword != forced {
				t.Fatalf("rejection changed mandatory flag: %v %v", stored, err)
			}
		}
	}
	if err := s.ChangePassword(ctx, "admin", password, "a-different-password"); err != nil {
		t.Fatal(err)
	}
	a, _ := s.GetAccount(ctx, "admin")
	if a.MustChangePassword {
		t.Fatal("successful change did not clear flag")
	}
}

func TestPasswordResetAcceptsCurrentPasswordAndInvalidatesAuthority(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	before, err := s.o.Store.GetAccount(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	cookies, _ := login(t, s)
	secret, _, err := s.CreateToken(ctx, "admin", "before-reset", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	later := before.PasswordChangedAt.Add(time.Hour)
	s.now = func() time.Time { return later }
	if err := s.ResetPassword(ctx, "admin", password); err != nil {
		t.Fatal(err)
	}
	after, err := s.o.Store.GetAccount(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if !after.MustChangePassword || !after.PasswordChangedAt.Equal(later) || !after.UpdatedAt.Equal(later) ||
		after.PasswordHash == before.PasswordHash || !VerifyPassword(after.PasswordHash, password) {
		t.Fatalf("reset did not rotate password state: %+v", after)
	}
	if _, err := s.Authenticate(request("GET", cookies, "")); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("old session remains valid: %v", err)
	}
	r := request("GET", nil, "")
	r.Header.Set("Authorization", "Bearer "+secret)
	if _, err := s.Authenticate(r); !errors.Is(err, ErrUnauthenticated) {
		t.Fatalf("old API token remains valid: %v", err)
	}
}

func TestAccountUpdatesHaveIndependentTimestamp(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	a, err := s.GetAccount(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	originalPassword := a.PasswordChangedAt
	later := a.UpdatedAt.Add(time.Hour)
	s.now = func() time.Time { return later }
	a.Disabled = true
	updated, err := s.UpdateAccount(ctx, *a)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.UpdatedAt.Equal(later) || !updated.PasswordChangedAt.Equal(originalPassword) {
		t.Fatal("timestamp conflates account and password updates", updated)
	}
	stored, _ := s.GetAccount(ctx, "admin")
	if !stored.UpdatedAt.Equal(later) {
		t.Fatal("timestamp not persisted")
	}
}

func TestTokenValidationAppliesToCreationAndUpdate(t *testing.T) {
	s := setup(t)
	ctx := t.Context()
	now := time.Now().UTC()
	s.now = func() time.Time { return now }
	past, future := now.Add(-time.Hour), now.Add(time.Hour)
	for _, tc := range []struct {
		name          string
		expiry        *time.Time
		field, reason string
	}{
		{"", nil, "name", domain.ReasonRequired},
		{" \t\n", nil, "name", domain.ReasonRequired},
		{strings.Repeat("x", 129), nil, "name", domain.ReasonOutOfRange},
		{"automation", &past, "expiresAt", domain.ReasonOutOfRange},
		{"automation", &now, "expiresAt", domain.ReasonOutOfRange},
	} {
		_, _, err := s.CreateToken(ctx, "admin", tc.name, tc.expiry, nil)
		requireField(t, err, tc.field, tc.reason)
	}
	_, token, err := s.CreateToken(ctx, "admin", "  automation  ", &future, nil)
	if err != nil || token.Name != "automation" {
		t.Fatal(token, err)
	}
	_, duplicate, err := s.CreateToken(ctx, "admin", token.Name, nil, nil)
	if err != nil || duplicate.ID == token.ID {
		t.Fatal("duplicate names must have distinct IDs", err)
	}
	for _, tc := range []struct {
		name          string
		expiry        *time.Time
		field, reason string
	}{
		{" ", nil, "name", domain.ReasonRequired},
		{strings.Repeat("x", 129), nil, "name", domain.ReasonOutOfRange},
		{"automation", &past, "expiresAt", domain.ReasonOutOfRange},
	} {
		changed := token
		changed.Name, changed.ExpiresAt = tc.name, tc.expiry
		_, err := s.UpdateToken(ctx, changed)
		requireField(t, err, tc.field, tc.reason)
	}
	stored, _ := s.GetToken(ctx, token.ID)
	if !stored.ExpiresAt.Equal(future) {
		t.Fatal("rejected update changed expiry")
	}
	token.Name, token.ExpiresAt = " renamed ", nil
	updated, err := s.UpdateToken(ctx, token)
	if err != nil || updated.Name != "renamed" || updated.ExpiresAt != nil {
		t.Fatal(updated, err)
	}
}

func TestLoginThrottleHasSameDelayForCorrectAndIncorrectCredentials(t *testing.T) {
	s := setup(t)
	now := time.Now().UTC()
	s.now = func() time.Time { return now }
	for range 5 {
		if _, err := s.Login(t.Context(), httptest.NewRecorder(), "admin", "wrong"); !errors.Is(err, ErrUnauthenticated) {
			t.Fatal(err)
		}
	}
	for _, secret := range []string{password, "wrong"} {
		_, err := s.Login(t.Context(), httptest.NewRecorder(), "admin", secret)
		var limited *RateLimitError
		if !errors.As(err, &limited) || limited.RetryAfterSeconds != 1 || !errors.Is(err, ErrRateLimited) {
			t.Fatal("inconsistent throttle", err)
		}
	}
	now = now.Add(time.Second)
	if _, err := s.Login(t.Context(), httptest.NewRecorder(), "admin", password); err != nil {
		t.Fatal(err)
	}
}
