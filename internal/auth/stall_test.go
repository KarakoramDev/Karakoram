// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"context"
	"database/sql"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestSettingsDoesNotWaitForIdentityMutation(t *testing.T) {
	s := setup(t)
	s.mu.Lock()
	done := make(chan error, 1)
	go func() { _, err := s.Settings(t.Context()); done <- err }()
	select {
	case err := <-done:
		s.mu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(300 * time.Millisecond):
		s.mu.Unlock()
		<-done
		t.Fatal("public version settings waited for the identity mutation mutex")
	}
}

func TestBearerAuthenticationDoesNotWaitForUsageWriter(t *testing.T) {
	sealer, err := secrets.New(strings.Repeat("w", 32))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "auth.db")
	st, err := store.Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	s, err := New(Options{Store: st, Sealer: sealer})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = s.CreateAccount(t.Context(), domain.Account{Name: "reader"}, password); err != nil {
		t.Fatal(err)
	}
	value, _, err := s.CreateToken(t.Context(), "reader", "test", nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	other, err := sql.Open("sqlite", "file:"+path)
	if err != nil {
		t.Fatal(err)
	}
	defer other.Close()
	tx, err := other.BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err = tx.ExecContext(t.Context(), "UPDATE records SET phase=phase WHERE kind='api-token'"); err != nil {
		t.Fatal(err)
	}
	r := httptest.NewRequest("GET", "https://example.test/api/v1/clusters", nil)
	r.Header.Set("Authorization", "Bearer "+value)
	done := make(chan error, 1)
	go func() { _, err := s.Authenticate(r); done <- err }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(300 * time.Millisecond):
		_ = tx.Rollback()
		<-done
		t.Fatal("bearer authentication waited for best-effort usage metadata")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()
	if _, err = s.Settings(ctx); err != nil {
		t.Fatal(err)
	}
	_ = tx.Rollback()
}
