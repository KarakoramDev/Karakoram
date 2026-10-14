// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
)

func TestCreateFieldAndConflictErrors(t *testing.T) {
	r, _, _ := setupRegistry(t)
	for _, tc := range []struct{ name, project, token, field, reason string }{
		{"", "proj-test", "valid-token", "name", "required"},
		{"Upper", "proj-test", "valid-token", "name", "invalid"},
		{"1name", "proj-test", "valid-token", "name", "invalid"},
		{"trailing-", "proj-test", "valid-token", "name", "invalid"},
		{strings.Repeat("a", 33), "proj-test", "valid-token", "name", "invalid"},
		{"testing", "", "valid-token", "projectId", "required"},
		{"testing", "proj-test", "", "token", "required"},
	} {
		_, err := r.Create(t.Context(), tc.name, tc.project, tc.token, "")
		var field *domain.FieldError
		if !errors.Is(err, ErrInvalid) || !errors.As(err, &field) || field.Field != tc.field || field.Reason != tc.reason {
			t.Fatal(tc, err)
		}
	}
	createTestNamespace(t, r)
	for _, name := range []string{"testing", "default"} {
		_, err := r.Create(t.Context(), name, "proj-test", "valid-token", "")
		var conflict *domain.ResourceError
		if !errors.Is(err, ErrConflict) || !errors.As(err, &conflict) || conflict.Resource != "namespace" || conflict.Name != name || conflict.Reason != domain.ReasonAlreadyExists {
			t.Fatal(err)
		}
	}
}
