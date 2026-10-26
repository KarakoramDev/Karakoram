// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package namespaces

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/infra"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
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

func TestAutomaticValidationClassifiesCredentialReadFailures(t *testing.T) {
	for _, failure := range []string{"store", "missing", "version", "encoding", "truncated", "authentication"} {
		t.Run(failure, func(t *testing.T) {
			r, _, log := setupRegistry(t)
			ctx := t.Context()
			path := filepath.Join(t.TempDir(), "state.db")
			key, err := secrets.New(strings.Repeat("k", 32))
			if err != nil {
				t.Fatal(err)
			}
			st, err := store.Open(path, key)
			if err != nil {
				t.Fatal(err)
			}
			defer st.Close()
			r.o.Store = st
			n := createTestNamespace(t, r)
			n.Token.LastValidatedAt = time.Now().UTC().Add(-10 * time.Minute)
			n.Token.LastError = "Previous diagnostic"
			n.Phase, n.Relay.Phase = "Pending", "Pending"
			// Isolate periodic validation from relay provisioning.
			n.RetryAt = time.Now().Add(time.Hour)
			if err := st.UpdateNamespace(ctx, n); err != nil {
				t.Fatal(err)
			}
			before := n.Token
			db, err := sql.Open("sqlite", path)
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			switch failure {
			case "store":
				// Fail the secret SELECT after the namespace SELECT succeeds.
				_, err = db.Exec("ALTER TABLE records RENAME TO unavailable_records")
			case "missing":
				err = st.DeleteNamespaceToken(ctx, n)
			default:
				sealed := "unsupported"
				switch failure {
				case "encoding":
					sealed = "kk:v1:!"
				case "truncated":
					sealed = "kk:v1:YQ"
				case "authentication":
					otherKey, e := secrets.New(strings.Repeat("x", 32))
					if e != nil {
						t.Fatal(e)
					}
					sealed, err = otherKey.SealFor("namespace/"+n.Name+"/token", []byte("private-token"))
					if err != nil {
						t.Fatal(err)
					}
				}
				_, err = db.Exec("UPDATE records SET secret=? WHERE kind='namespace-secret' AND scope=?", sealed, n.Name)
			}
			if err != nil {
				t.Fatal(err)
			}
			_, readErr := st.GetNamespaceTokenForUpdate(ctx, n.Name)
			if readErr == nil {
				t.Fatal("credential read did not fail")
			}
			switch failure {
			case "store":
				if errors.Is(readErr, store.ErrNotFound) || errors.Is(readErr, store.ErrSecretUnreadable) {
					t.Fatalf("store failure classified as credential failure: %v", readErr)
				}
			case "missing":
				if !errors.Is(readErr, store.ErrNotFound) {
					t.Fatalf("missing credential classification: %v", readErr)
				}
			default:
				if !errors.Is(readErr, store.ErrSecretUnreadable) {
					t.Fatalf("unreadable credential classification: %v", readErr)
				}
			}
			validations := 0
			factory := registryFactory(r.o.Backends)
			validate := factory.validate
			factory.validate = func(ctx context.Context, target infra.Target) (Backend, infra.Project, error) {
				validations++
				return validate(ctx, target)
			}
			r.step(ctx, n.Name)
			after, err := st.GetNamespaceForUpdate(ctx, n.Name)
			if err != nil {
				t.Fatal(err)
			}
			if validations != 0 {
				t.Fatal("backend validation ran without readable credentials")
			}
			failedEvents := 0
			for _, reason := range log.reasons {
				if reason == "NamespaceValidationFailed" {
					failedEvents++
				}
			}
			if failure == "store" {
				if after.Token != before || after.Phase != n.Phase || failedEvents != 0 {
					t.Fatalf("store failure changed token health or phase: before=%+v after=%+v phase=%s events=%d", before, after.Token, after.Phase, failedEvents)
				}
				if _, err := db.Exec("ALTER TABLE unavailable_records RENAME TO records"); err != nil {
					t.Fatal(err)
				}
				// No five-minute wait: recovery is validated on the very next step.
				r.step(ctx, n.Name)
				after, err = st.GetNamespaceForUpdate(ctx, n.Name)
				if err != nil {
					t.Fatal(err)
				}
				if validations != 1 || !after.Token.Valid || !after.Token.LastValidatedAt.After(before.LastValidatedAt) || after.Token.LastError != "" || after.Phase != n.Phase {
					t.Fatalf("next step did not validate recovered credentials: %+v validations=%d", after, validations)
				}
			} else if after.Token.Valid || after.Phase != "Error" || !after.Token.LastValidatedAt.After(before.LastValidatedAt) || !strings.Contains(after.Token.LastError, "Stored project credentials are unavailable") || failedEvents != 1 {
				t.Fatalf("genuine credential failure did not invalidate: %+v events=%d", after, failedEvents)
			}
		})
	}
}
