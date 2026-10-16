// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
)

func TestTokenValidationRetainsKnownValidity(t *testing.T) {
	for _, failure := range []error{
		infra.ErrUnavailable,
		fmt.Errorf("wrapped: %w", infra.ErrUnavailable),
		&infra.RejectedError{Code: "serviceNotFound"},
		context.DeadlineExceeded,
		&net.OpError{Op: "dial", Net: "tcp", Err: errors.New("https://private-token@example.invalid")},
	} {
		for _, valid := range []bool{false, true} {
			n := &domain.Namespace{ProjectID: "project", ProjectName: "Known project", Token: domain.NamespaceTokenStatus{Valid: valid}}
			applyTokenValidation(n, infra.Project{}, failure, time.Now(), "Zerops")
			if n.Token.Valid != valid || n.ProjectName != "Known project" || !strings.HasPrefix(n.Token.LastError, "Zerops API temporarily unavailable: ") || !strings.HasSuffix(n.Token.LastError, "; retrying") {
				t.Fatalf("transient failure changed validity: %+v (%v)", n.Token, failure)
			}
			if strings.Contains(n.Token.LastError, "private-token") {
				t.Fatalf("transport credentials leaked: %s", n.Token.LastError)
			}
		}
	}
	for _, failure := range []error{
		infra.ErrUnauthorized,
		fmt.Errorf("wrapped: %w", infra.ErrUnauthorized),
	} {
		n := &domain.Namespace{Token: domain.NamespaceTokenStatus{Valid: true}}
		applyTokenValidation(n, infra.Project{}, failure, time.Now(), "Zerops")
		if n.Token.Valid || !strings.HasPrefix(n.Token.LastError, "Project token access rejected:") {
			t.Fatalf("authentication rejection not retained: %+v", n.Token)
		}
	}
}

func TestManualAndAutomaticValidationSurvivePlatformOutage(t *testing.T) {
	r, _, log := setupRegistry(t)
	ctx := context.Background()
	createTestNamespace(t, r)
	registryFactory(r.o.Backends).validate = func(context.Context, infra.Target) (Backend, infra.Project, error) {
		return nil, infra.Project{}, infra.ErrUnavailable
	}
	for _, name := range []string{"default", "testing"} {
		n, err := r.Validate(ctx, name)
		if err != nil || !n.Token.Valid || !strings.Contains(n.Token.LastError, "temporarily unavailable") {
			t.Fatalf("manual outage health: %+v %v", n, err)
		}
		n, err = r.o.Store.GetNamespaceForUpdate(ctx, name)
		if err != nil {
			t.Fatal(err)
		}
		n.Token.LastValidatedAt = time.Now().Add(-10 * time.Minute)
		if err = r.o.Store.UpdateNamespace(ctx, n); err != nil {
			t.Fatal(err)
		}
		r.step(ctx, name)
		n, err = r.Get(ctx, name)
		if err != nil || !n.Token.Valid || !strings.Contains(n.Token.LastError, "temporarily unavailable") {
			t.Fatalf("automatic outage health: %+v %v", n, err)
		}
		if _, err = r.Backend(name); err != nil {
			t.Fatalf("outage disabled known-valid backend: %v", err)
		}
	}
	for _, reason := range log.reasons {
		if reason == "NamespaceValidationFailed" {
			t.Fatal("outage emitted an invalid-token event")
		}
	}
}

type failingProjectBackend struct {
	Backend
}

func (b failingProjectBackend) ProjectInfo(context.Context) (infra.Project, error) {
	return infra.Project{}, infra.ErrUnavailable
}

func TestStartupOutageKeepsPersistedTokenValidity(t *testing.T) {
	r, _, _ := setupRegistry(t)
	o := r.o
	o.DefaultBackend = failingProjectBackend{Backend: o.DefaultBackend}
	restored, err := New(context.Background(), o)
	if err != nil {
		t.Fatal(err)
	}
	defer restored.Close()
	n, err := restored.Get(context.Background(), "default")
	if err != nil || !n.Token.Valid || !strings.Contains(n.Token.LastError, "temporarily unavailable") {
		t.Fatalf("startup outage invalidated persisted token: %+v %v", n, err)
	}
}

func TestTokenReplacementOutageDoesNotReplaceCredentials(t *testing.T) {
	r, _, _ := setupRegistry(t)
	ctx := context.Background()
	n := createTestNamespace(t, r)
	registryFactory(r.o.Backends).validate = func(context.Context, infra.Target) (Backend, infra.Project, error) {
		return nil, infra.Project{}, infra.ErrUnavailable
	}
	if _, err := r.RotateToken(ctx, n.Name, "replacement"); err == nil || errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "temporarily unavailable") {
		t.Fatalf("rotation outage misclassified: %v", err)
	}
	if _, err := r.Create(ctx, "another", "another-project", "new-token", "", "zerops", nil); err == nil || errors.Is(err, ErrInvalid) || !strings.Contains(err.Error(), "temporarily unavailable") {
		t.Fatalf("registration outage misclassified: %v", err)
	}
	token, err := r.o.Store.GetNamespaceTokenForUpdate(ctx, n.Name)
	if err != nil || token != "valid-token" {
		t.Fatal("failed rotation replaced credentials", err)
	}
}
