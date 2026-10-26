// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package server

import (
	"context"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func TestStartupRetriesReadOnlyStateWithoutReplacingIntent(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root bypasses file permissions")
	}
	path := filepath.Join(t.TempDir(), "state.db")
	key, _ := secrets.New(strings.Repeat("k", 32))
	st, err := store.Open(path, key)
	if err != nil {
		t.Fatal(err)
	}
	if err := st.CreateCluster(t.Context(), &domain.Cluster{Name: "demo"}); err != nil {
		t.Fatal(err)
	}
	if err := st.EnqueueOperation(t.Context(), &domain.Operation{ID: "accepted", Cluster: "demo", Kind: domain.OperationRestart}); err != nil {
		t.Fatal(err)
	}
	st.Close()
	if err := os.Chmod(path, 0400); err != nil {
		t.Fatal(err)
	}
	defer os.Chmod(path, 0600)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	type result struct {
		st  *store.Store
		err error
	}
	done := make(chan result, 1)
	go func() {
		st, err := openState(ctx, path, key, slog.New(slog.NewTextHandler(io.Discard, nil)), 5*time.Millisecond)
		done <- result{st, err}
	}()
	select {
	case result := <-done:
		if result.st != nil {
			result.st.Close()
		}
		t.Fatal("startup did not remain unready with read-only state", result.err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := os.Chmod(path, 0600); err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-done:
		if result.err != nil {
			t.Fatal(result.err)
		}
		defer result.st.Close()
		if _, err := result.st.GetOperation(t.Context(), "accepted"); err != nil {
			t.Fatal("accepted intent lost", err)
		}
		if err := result.st.EnqueueOperation(t.Context(), &domain.Operation{ID: "recovered", Cluster: "demo", Kind: domain.OperationRestart}); err != nil {
			t.Fatal("recovered state is not writable", err)
		}
	case <-ctx.Done():
		t.Fatal("startup did not recover after write access returned")
	}
}
