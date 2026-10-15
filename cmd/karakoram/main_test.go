// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package main

import (
	"bytes"
	"context"
	"errors"
	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
	"github.com/KarakoramDev/Karakoram/internal/version"
	"net"
	"path/filepath"
	"strings"
	"testing"
)

func TestVersionAndUsage(t *testing.T) {
	var out bytes.Buffer
	if err := run(context.Background(), []string{"version"}, &out, &out); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), version.String()) {
		t.Fatal(out.String())
	}
	for _, args := range [][]string{nil, {"unknown"}, {"version", "extra"}, {"agent", "unknown"}, {"admin", "reset-password"}} {
		if err := run(context.Background(), args, &out, &out); err == nil {
			t.Fatal("invalid arguments accepted", args)
		}
	}
}

func TestAgentReadinessWithoutMemberEnvironment(t *testing.T) {
	for _, key := range []string{"KARAKORAM_URL", "KARAKORAM_INTERNAL_CA", "KARAKORAM_AGENT_TOKEN", "KARAKORAM_CLUSTER", "KARAKORAM_MEMBER", "KARAKORAM_DATA_DIR"} {
		t.Setenv(key, "")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	err := run(ctx, []string{"agent", "readiness"}, &out, &out)
	var networkError *net.OpError
	if !errors.As(err, &networkError) {
		t.Fatalf("readiness must reach the local TCP probe without member env: %v", err)
	}
}

func TestAgentReadinessArgumentPassthrough(t *testing.T) {
	t.Setenv("KARAKORAM_ENGINE", "")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	err := run(ctx, []string{"agent", "readiness", "component", "1234"}, &out, &out)
	var networkError *net.OpError
	if !errors.As(err, &networkError) {
		t.Fatalf("extra readiness arguments must reach the default probe: %v", err)
	}
	t.Setenv("KARAKORAM_ENGINE", "unknown")
	if err := run(ctx, []string{"agent", "readiness", "component"}, &out, &out); err == nil || !strings.Contains(err.Error(), "unknown KARAKORAM_ENGINE") {
		t.Fatal(err)
	}
}

func TestAgentReadinessExplicitEngineOverridesEnvironment(t *testing.T) {
	t.Setenv("KARAKORAM_ENGINE", "unknown")
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	var out bytes.Buffer
	err := run(ctx, []string{"agent", "readiness", "--engine=psmdb", "component", "1234"}, &out, &out)
	var networkError *net.OpError
	if !errors.As(err, &networkError) {
		t.Fatalf("explicit engine must override environment and reach the local TCP probe: %v", err)
	}
	t.Setenv("KARAKORAM_ENGINE", "psmdb")
	if err := run(ctx, []string{"agent", "readiness", "--engine=unknown"}, &out, &out); err == nil || !strings.Contains(err.Error(), "unknown KARAKORAM_ENGINE") {
		t.Fatal(err)
	}
}

func TestOfflinePasswordReset(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	t.Setenv("KARAKORAM_DATA_DIR", dir)
	t.Setenv("KARAKORAM_MASTER_KEY", strings.Repeat("k", 32))
	t.Setenv("KARAKORAM_RESET_PASSWORD", "new-test-password")
	sealer, err := secrets.FromEnv()
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "karakoram.db")
	st, err := store.Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	service, err := auth.New(auth.Options{Store: st, Sealer: sealer})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = service.CreateAccount(ctx, domain.Account{Name: "admin"}, "old-test-password"); err != nil {
		t.Fatal(err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	if err = run(ctx, []string{"admin", "reset-password", "admin"}, &out, &out); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out.String(), "new-test-password") {
		t.Fatal("password printed")
	}
	st, err = store.Open(path, sealer)
	if err != nil {
		t.Fatal(err)
	}
	defer st.Close()
	a, err := st.GetAccount(ctx, "admin")
	if err != nil {
		t.Fatal(err)
	}
	if !auth.VerifyPassword(a.PasswordHash, "new-test-password") || !a.MustChangePassword {
		t.Fatal("password recovery did not update identity")
	}
}
