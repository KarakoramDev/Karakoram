// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rbac

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func fixture(t *testing.T) (*Enforcer, *store.Store) {
	t.Helper()
	seal, err := secrets.New(strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(":memory:", seal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	if err := st.CreateAccount(t.Context(), &domain.Account{Name: "admin"}); err != nil {
		t.Fatal(err)
	}
	e, err := New(st)
	if err != nil {
		t.Fatal(err)
	}
	return e, st
}
func TestDefaults(t *testing.T) {
	e, _ := fixture(t)
	for _, tt := range []struct {
		subject, res, act string
		want              bool
	}{{"user:admin", "settings", "update", true}, {"role:operator", "clusters", "delete", true}, {"role:operator", "cluster-credentials", "read", false}, {"role:operator", "backup-storages", "read", true}, {"role:operator", "accounts", "read", false}, {"role:viewer", "clusters", "read", true}, {"role:viewer", "clusters", "update", false}, {"role:viewer", "cluster-credentials", "read", false}} {
		if got := e.Can([]string{tt.subject}, tt.res, tt.act, "prod"); got != tt.want {
			t.Errorf("%+v: %v", tt, got)
		}
	}
	if len(e.Permissions([]string{"user:admin"})) == 0 {
		t.Fatal("missing implicit admin permission")
	}
}
func TestDenyGlobsAndPersistence(t *testing.T) {
	e, st := fixture(t)
	policy := DefaultPolicy() + "g, user:alice, role:viewer\np, group:blocked, clusters, read, prod*, deny\np, user:alice, cluster*, read, dev?, allow\n"
	if err := e.ReplacePolicy(context.Background(), policy); err != nil {
		t.Fatal(err)
	}
	if e.Can([]string{"user:alice", "group:blocked"}, "clusters", "read", "prod01") {
		t.Fatal("deny lost across subjects")
	}
	if !e.Can([]string{"user:alice"}, "cluster-credentials", "read", "dev1") {
		t.Fatal("glob did not match")
	}
	if e.Can([]string{"user:alice"}, "cluster-credentials", "read", "dev12") {
		t.Fatal("single-char glob widened")
	}
	reloaded, err := New(st)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Policy() != e.Policy() {
		t.Fatal("persistence mismatch")
	}
	perms := e.Permissions([]string{"group:blocked"})
	if len(perms) != 1 || perms[0].Values[4] != "deny" {
		t.Fatal(perms)
	}
}
func TestPolicyValidation(t *testing.T) {
	e, _ := fixture(t)
	before := e.Policy()
	for _, p := range []string{"p, role:admin, *, *, *, allow\n", DefaultPolicy() + "p, user:a, nonexistent, read, *, allow\n", DefaultPolicy() + "p, user:a, clusters, execute, *, allow\n", DefaultPolicy() + "p, user:a, clusters, read, *, unexpected\n", "g, user:admin\n", DefaultPolicy() + "p, user:admin, settings, update, *, deny\n"} {
		if err := e.ReplacePolicy(context.Background(), p); err == nil {
			t.Errorf("accepted %q", p)
		}
		if e.Policy() != before {
			t.Fatal("failed replacement mutated policy")
		}
	}
}
func TestConcurrentReload(t *testing.T) {
	e, _ := fixture(t)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 50 {
				_ = e.Can([]string{"user:admin"}, "clusters", "read", "prod")
				_ = e.Permissions([]string{"user:admin"})
				_ = e.Policy()
			}
		}()
	}
	if err := e.ReplacePolicy(context.Background(), DefaultPolicy()); err != nil {
		t.Fatal(err)
	}
	wg.Wait()
}

func TestPolicyRejectsUnavailableAdministrator(t *testing.T) {
	e, st := fixture(t)
	before := e.Policy()
	for _, subject := range []string{"user:misspelled", "group:unverified"} {
		if err := e.ReplacePolicy(t.Context(), "p, "+subject+", *, *, *, allow\n"); err == nil {
			t.Fatalf("accepted unavailable administrator %s", subject)
		}
	}
	account, err := st.GetAccount(t.Context(), "admin")
	if err != nil {
		t.Fatal(err)
	}
	account.Disabled = true
	if err := st.UpdateAccount(t.Context(), account); err != nil {
		t.Fatal(err)
	}
	if err := e.ReplacePolicy(t.Context(), DefaultPolicy()); err == nil {
		t.Fatal("accepted disabled administrator")
	}
	if e.Policy() != before {
		t.Fatal("rejected replacements changed the live policy")
	}
}
