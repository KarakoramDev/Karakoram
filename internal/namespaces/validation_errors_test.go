// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/store"
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
		_, err := r.Create(t.Context(), tc.name, tc.project, tc.token, "", "zerops", nil)
		var field *domain.FieldError
		if !errors.Is(err, ErrInvalid) || !errors.As(err, &field) || field.Field != tc.field || field.Reason != tc.reason {
			t.Fatal(tc, err)
		}
	}
	createTestNamespace(t, r)
	for _, name := range []string{"testing", "default"} {
		_, err := r.Create(t.Context(), name, "proj-test", "valid-token", "", "zerops", nil)
		var conflict *domain.ResourceError
		if !errors.Is(err, ErrConflict) || !errors.As(err, &conflict) || conflict.Resource != "namespace" || conflict.Name != name || conflict.Reason != domain.ReasonAlreadyExists {
			t.Fatal(err)
		}
	}
}

func TestCreateAndRotatePreserveBackendRejections(t *testing.T) {
	for _, rejection := range []error{
		&infra.RejectedError{Code: "invalidTarget", Message: "backend target is invalid"},
		infra.ErrUnsupported,
	} {
		t.Run(rejection.Error(), func(t *testing.T) {
			r, _, _ := setupRegistry(t)
			n := createTestNamespace(t, r)
			registryFactory(r.o.Backends).validate = func(context.Context, infra.Target) (Backend, infra.Project, error) {
				return nil, infra.Project{}, rejection
			}
			if _, err := r.Create(t.Context(), "another", "another-project", "new-token", "", "zerops", nil); err != rejection {
				t.Fatalf("create wrapped rejection: %v", err)
			}
			if _, err := r.RotateToken(t.Context(), n.Name, "replacement"); err != rejection {
				t.Fatalf("rotation wrapped rejection: %v", err)
			}
			token, err := r.o.Store.GetNamespaceTokenForUpdate(t.Context(), n.Name)
			if err != nil || token != "valid-token" {
				t.Fatal("failed rotation replaced credentials", err)
			}
		})
	}
}

func TestManualValidationReturnsStoredTokenReadError(t *testing.T) {
	r, _, _ := setupRegistry(t)
	n := createTestNamespace(t, r)
	if err := r.o.Store.DeleteNamespaceToken(t.Context(), n); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Validate(t.Context(), n.Name); !errors.Is(err, store.ErrNotFound) {
		t.Fatal("stored token read error hidden", err)
	}
	after, err := r.o.Store.GetNamespaceForUpdate(t.Context(), n.Name)
	if err != nil || after.Token != n.Token || after.Phase != n.Phase {
		t.Fatalf("failed read changed token health: %+v %v", after, err)
	}
}
