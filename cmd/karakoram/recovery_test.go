// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package main

import (
	"bytes"
	"context"
	"errors"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/auth"
	"github.com/KarakoramDev/Karakoram/internal/rbac"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/server"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func recoveryServerEnv(t *testing.T) {
	t.Helper()
	for key, value := range map[string]string{
		"KARAKORAM_DATA_DIR":          t.TempDir(),
		"hostname":                    "karakoram",
		"KARAKORAM_LISTEN":            "127.0.0.1:0",
		"KARAKORAM_INTERNAL_LISTEN":   "127.0.0.1:0",
		"KARAKORAM_PUBLIC_URL":        "https://karakoram.example",
		"KARAKORAM_ADVERTISE_URL":     "https://karakoram:9443",
		"KARAKORAM_MASTER_KEY":        strings.Repeat("k", 32),
		"KARAKORAM_ZEROPS_TOKEN":      "test-token-not-used",
		"KARAKORAM_ZEROPS_PROJECT_ID": "test-project",
		"KARAKORAM_ZEROPS_API_URL":    "https://zerops.example/api/rest/public",
		"KARAKORAM_ADMIN_PASSWORD":    "short", // Recovery must bypass bootstrap.
		"KARAKORAM_RECOVERY_ACCOUNT":  "",
		"KARAKORAM_RECOVERY_PASSWORD": "recovery-test-password",
	} {
		t.Setenv(key, value)
	}
}

func TestServerRecoveryRunsBeforeListenersAndOnlyOnce(t *testing.T) {
	recoveryServerEnv(t)
	t.Setenv("KARAKORAM_RECOVERY_ACCOUNT", "rescuer")
	// A deliberately occupied public listener stops Run after initialization,
	// proving recovery is wired before requests can be accepted. No backend
	// calls, live service access or long-lived server are needed.
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	t.Setenv("KARAKORAM_LISTEN", listener.Addr().String())
	var logs bytes.Buffer
	start := func() {
		t.Helper()
		err := run(context.Background(), []string{"server"}, &logs, &logs)
		var networkError *net.OpError
		if !errors.As(err, &networkError) || networkError.Op != "listen" {
			t.Fatalf("startup should reach the occupied listener: %v", err)
		}
	}
	open := func() (*store.Store, *auth.Service) {
		t.Helper()
		cfg, err := server.ConfigFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		sealer, err := secrets.FromEnv()
		if err != nil {
			t.Fatal(err)
		}
		st, err := store.Open(cfg.DatabasePath(), sealer)
		if err != nil {
			t.Fatal(err)
		}
		a, err := auth.New(auth.Options{Store: st, Sealer: sealer})
		if err != nil {
			st.Close()
			t.Fatal(err)
		}
		return st, a
	}
	start()
	st, a := open()
	account, err := st.GetAccount(t.Context(), "rescuer")
	if err != nil || account.Disabled || account.OIDC || !account.MustChangePassword {
		t.Fatal("startup recovery did not create a usable local administrator", err)
	}
	policy, err := rbac.New(st)
	if err != nil || !policy.Can([]string{"user:rescuer"}, "accounts", "write", "*") {
		t.Fatal("startup recovery did not restore admin policy", err)
	}
	if err = a.ChangePassword(t.Context(), "rescuer", "recovery-test-password", "changed-test-password"); err != nil {
		t.Fatal(err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	start()
	st, _ = open()
	account, err = st.GetAccount(t.Context(), "rescuer")
	if err != nil || account.MustChangePassword || !auth.VerifyPassword(account.PasswordHash, "changed-test-password") {
		t.Fatal("same environment reset the account again", err)
	}
	if err = st.Close(); err != nil {
		t.Fatal(err)
	}
	t.Setenv("KARAKORAM_RECOVERY_PASSWORD", "different-recovery-password")
	start()
	st, _ = open()
	defer st.Close()
	account, err = st.GetAccount(t.Context(), "rescuer")
	if err != nil || !account.MustChangePassword || !auth.VerifyPassword(account.PasswordHash, "different-recovery-password") {
		t.Fatal("changed environment did not reapply recovery", err)
	}
	for _, value := range []string{"recovery-test-password", "different-recovery-password", strings.Repeat("k", 32)} {
		if strings.Contains(logs.String(), value) {
			t.Fatal("startup exposed a recovery secret")
		}
	}
}

func TestRecoveryEnvironmentValidation(t *testing.T) {
	recoveryServerEnv(t)
	cfg, err := server.ConfigFromEnv()
	if err != nil || cfg.RecoveryAccount != "admin" || cfg.RecoveryPassword != "recovery-test-password" {
		t.Fatal("recovery defaults not loaded", err)
	}
	for _, tc := range []struct{ name, account, password string }{
		{"short", "admin", "short"},
		{"long", "admin", strings.Repeat("x", 1025)},
		{"invalid-account", "bad/account", "recovery-test-password"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("KARAKORAM_RECOVERY_ACCOUNT", tc.account)
			t.Setenv("KARAKORAM_RECOVERY_PASSWORD", tc.password)
			var logs bytes.Buffer
			if err := run(t.Context(), []string{"server"}, &logs, &logs); err == nil {
				t.Fatal("invalid recovery accepted")
			}
			if _, err := os.Stat(cfg.DatabasePath()); !errors.Is(err, os.ErrNotExist) {
				t.Fatal("invalid recovery opened/mutated state", err)
			}
			if logs.Len() != 0 {
				t.Fatal("invalid recovery logged secret input")
			}
		})
	}
	t.Setenv("KARAKORAM_RECOVERY_PASSWORD", "")
	t.Setenv("KARAKORAM_RECOVERY_ACCOUNT", "bad/account")
	if cfg, err := server.ConfigFromEnv(); err != nil || cfg.RecoveryPassword != "" {
		t.Fatal("unset recovery should be ignored", err)
	}
}
