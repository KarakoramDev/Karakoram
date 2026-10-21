// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package users

import (
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/engine"
	"strings"
	"testing"
)

func TestDataAccessRoles(t *testing.T) {
	for _, level := range []engine.DataAccessLevel{engine.DataAccessRead, engine.DataAccessReadWrite} {
		role := "read"
		if level == engine.DataAccessReadWrite {
			role = "readWrite"
		}
		got, err := DataAccessRoles(level, nil)
		if err != nil || !sameRoles(got, []domain.RoleRef{{Role: role + "AnyDatabase", DB: "admin"}}) {
			t.Fatalf("global roles: %v %v", got, err)
		}
		got, err = DataAccessRoles(level, []string{"app", "other", "app"})
		if err != nil || !sameRoles(got, []domain.RoleRef{{Role: role, DB: "app"}, {Role: role, DB: "other"}}) {
			t.Fatalf("scoped roles: %v %v", got, err)
		}
		if !IsSystemUser(engine.DataAccessUser(level)) {
			t.Fatal("data identity is not reserved")
		}
		if err := validateApplicationUser("app", engine.DataAccessUser(level)); !errors.Is(err, engine.ErrInvalid) {
			t.Fatal("data identity accepted by application API", err)
		}
	}
	for _, db := range []string{"admin", "local", "config", "", " app", "bad/name", "bad.name", "$external", "bad\x00db", strings.Repeat("a", 64)} {
		if _, err := DataAccessRoles(engine.DataAccessRead, []string{db}); !errors.Is(err, engine.ErrInvalid) {
			t.Fatalf("accepted scope %q: %v", db, err)
		}
	}
	if _, err := DataAccessRoles("root", nil); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
	if err := EnsureDataAccessUser(t.Context(), nil, engine.DataAccessRead, nil, "valid-password"); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
	if err := DropDataAccessUser(t.Context(), nil, engine.DataAccessRead); !errors.Is(err, engine.ErrInvalid) {
		t.Fatal(err)
	}
}
