// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package users

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"github.com/xdg-go/stringprep"
)

func TestPasswordMinimumAndSASLprep(t *testing.T) {
	for _, tc := range []struct {
		password string
		reason   string
	}{
		{"1234567", domain.ReasonOutOfRange},
		{"中文密码七个字", domain.ReasonOutOfRange},
		{"punct-é-中文-🔐", domain.ReasonInvalid},
		{"password\n", domain.ReasonInvalid},
		{"abc\x00defgh", domain.ReasonInvalid},
		{"abcdefgh\u0627", domain.ReasonInvalid},
	} {
		var field *engine.ValidationError
		err := ValidatePassword(tc.password)
		if !errors.As(err, &field) || field.Field != "password" || field.Reason != tc.reason || !errors.Is(err, engine.ErrInvalid) {
			t.Fatalf("unexpected password validation: %v", err)
		}
		if strings.Contains(err.Error(), tc.password) {
			t.Fatal("password leaked in error")
		}
	}
	for _, password := range []string{"12345678", "punct-é-中文!", "中文密码至少八个字"} {
		if err := ValidatePassword(password); err != nil {
			t.Fatalf("valid password rejected: %v", err)
		}
	}
	// Reproduce the preparation failure with the same profile the driver uses
	// for SCRAM-SHA-256, without requiring a live MongoDB instance.
	if _, err := stringprep.SASLprep.Prepare("punct-é-中文-🔐"); err == nil {
		t.Fatal("expected emoji to be unassigned in the Unicode 3.2 profile")
	}
}

func TestPasswordMutationsRejectBeforeDatabaseAccess(t *testing.T) {
	u := domain.DatabaseUser{Name: "app", Database: "app"}
	for _, password := range []string{"1234567", "long-enough-🔐"} {
		for _, err := range []error{
			CreateUser(context.Background(), nil, u, password),
			UpdateUser(context.Background(), nil, u, password),
			SetPassword(context.Background(), nil, u.Database, u.Name, password),
		} {
			var field *engine.ValidationError
			if !errors.As(err, &field) || field.Field != "password" {
				t.Fatalf("password mutation reached database: %v", err)
			}
		}
	}
}

func TestRoleDeletionRequiresExplicitUnassignment(t *testing.T) {
	ref := domain.RoleRef{DB: "app", Role: "reader"}
	users := []domain.DatabaseUser{{Name: "alice", Database: "app", Roles: []domain.RoleRef{ref}}}
	roles := []domain.DatabaseRole{{Name: "writer", Database: "app", Roles: []domain.RoleRef{ref}}}
	for _, tc := range []struct {
		users []domain.DatabaseUser
		roles []domain.DatabaseRole
		want  string
	}{
		{users, nil, "user alice@app"},
		{nil, roles, "role writer@app"},
		{users, roles, "role writer@app, user alice@app"},
	} {
		err := validateRoleDeletion("app", "reader", tc.users, tc.roles)
		var conflict *domain.ResourceError
		if !errors.As(err, &conflict) || conflict.Reason != domain.ReasonInvalidState || !errors.Is(err, engine.ErrConflict) || !strings.Contains(err.Error(), tc.want) {
			t.Fatalf("missing assignment conflict: %v", err)
		}
	}
	if err := validateRoleDeletion("other", "reader", users, roles); err != nil {
		t.Fatal("cross-database role falsely blocked", err)
	}
	users[0].Roles, roles[0].Roles = nil, nil
	if err := validateRoleDeletion("app", "reader", users, roles); err != nil {
		t.Fatal("unassigned role blocked", err)
	}
}
