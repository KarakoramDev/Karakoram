// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package rbac

import (
	"context"
	"database/sql"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/KarakoramDev/Karakoram/internal/domain"
	"github.com/KarakoramDev/Karakoram/internal/secrets"
	"github.com/KarakoramDev/Karakoram/internal/store"
)

func fixture(t *testing.T) (*Enforcer, *store.Store) {
	t.Helper()
	st := openStore(t, ":memory:")
	if err := st.CreateAccount(t.Context(), &domain.Account{Name: "admin"}); err != nil {
		t.Fatal(err)
	}
	e, err := New(st)
	if err != nil {
		t.Fatal(err)
	}
	return e, st
}

func openStore(t *testing.T, path string) *store.Store {
	seal, err := secrets.New(strings.Repeat("k", 32))
	if err != nil {
		t.Fatal(err)
	}
	st, err := store.Open(path, seal)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = st.Close() })
	return st
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
	if e.Can([]string{"user:alice", "group:blocked"}, "clusters", "read", "default/prod01") {
		t.Fatal("deny lost across subjects")
	}
	if !e.Can([]string{"user:alice"}, "cluster-credentials", "read", "default/dev1") {
		t.Fatal("glob did not match")
	}
	if e.Can([]string{"user:alice"}, "cluster-credentials", "read", "default/dev12") {
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
				_ = e.Can([]string{"user:admin"}, "clusters", "read", "default/prod")
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

func TestNamespaceDefaults(t *testing.T) {
	e, _ := fixture(t)
	for _, tt := range []struct {
		subject, resource, action, object string
		want                              bool
	}{
		{"user:admin", "settings", "update", "*", true},
		{"user:admin", "clusters", "delete", "prod/orders", true},
		{"user:admin", "backups", "export", "prod/orders", true},
		{"role:operator", "backups", "export", "prod/orders", true},
		{"role:viewer", "backups", "export", "prod/orders", false},
		{"role:viewer", "backups", "read", "prod/orders", true},
		{"user:admin", "namespaces", "create", "*", true},
		{"user:admin", "namespaces", "read", "prod", true},
		{"user:admin", "namespaces", "update", "prod", true},
		{"user:admin", "namespaces", "delete", "prod", true},
		{"role:operator", "namespaces", "read", "prod", true},
		{"role:viewer", "namespaces", "read", "prod", true},
		{"role:viewer", "namespaces", "create", "*", false},
		{"role:viewer", "namespaces", "update", "prod", false},
		{"role:viewer", "namespaces", "delete", "prod", false},
	} {
		if got := e.Can([]string{tt.subject}, tt.resource, tt.action, tt.object); got != tt.want {
			t.Errorf("%+v: got %v", tt, got)
		}
	}
	rules, err := parse(DefaultPolicy())
	if err != nil {
		t.Fatal(err)
	}
	for _, rule := range rules {
		if rule.PType == "p" && rule.Values[3] != "**" {
			t.Fatalf("built-in role has scoped object: %+v", rule)
		}
		if rule.PType == "p" && rule.Values[0] == "role:viewer" && rule.Values[2] != "read" {
			t.Fatalf("viewer is not read-only: %+v", rule)
		}
	}
}

func TestNormalizationIdempotent(t *testing.T) {
	for _, tt := range []struct{ object, want string }{
		{"*", "**"},
		{"**", "**"},
		{"orders*", "default/orders*"},
		{"dev", "default/dev"},
		{"default/orders*", "default/orders*"},
		{"dev/**", "dev/**"},
		{"prod/orders", "prod/orders"},
		{"**/orders", "**/orders"},
		{"**orders", "**orders"},
	} {
		t.Run(tt.object, func(t *testing.T) {
			rules, err := parse("p, user:alice, clusters, read, " + tt.object + "\n")
			if err != nil {
				t.Fatal(err)
			}
			if got := rules[0].Values[3]; got != tt.want {
				t.Fatalf("normalized %q to %q, want %q", tt.object, got, tt.want)
			}
			again, err := parse(encode(rules))
			if err != nil {
				t.Fatal(err)
			}
			if encode(again) != encode(rules) {
				t.Fatal("normalization is not idempotent")
			}
		})
	}
}

func TestNamespaceObjectNormalization(t *testing.T) {
	for _, tt := range []struct{ resource, object, want string }{
		{"namespaces", "testing", "testing"},
		{"namespaces", "test*", "test*"},
		{"namespaces", "*", "**"},
		{"namespaces", "**", "**"},
		{"namespaces", "testing/**", "testing/**"},
		{"*", "testing", "default/testing"},
		{"namespace*", "testing", "default/testing"},
	} {
		rules, err := parse("p, user:alice, " + tt.resource + ", read, " + tt.object)
		if err != nil {
			t.Fatal(err)
		}
		if got := rules[0].Values[3]; got != tt.want {
			t.Errorf("%s object %q normalized to %q, want %q", tt.resource, tt.object, got, tt.want)
		}
		again, err := parse(encode(rules))
		if err != nil {
			t.Fatal(err)
		}
		if encode(again) != encode(rules) {
			t.Fatal("namespace normalization is not idempotent")
		}
	}
	e, st := fixture(t)
	policy := DefaultPolicy() +
		"p, user:alice, namespaces, read, testing\n" +
		"p, user:bob, *, read, testing\n"
	if err := e.ReplacePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	if !e.Can([]string{"user:alice"}, "namespaces", "read", "testing") {
		t.Fatal("bare namespace grant did not match its namespace")
	}
	if e.Can([]string{"user:alice"}, "namespaces", "read", "other") ||
		e.Can([]string{"user:alice"}, "namespaces", "create", "*") {
		t.Fatal("bare namespace grant widened")
	}
	if !e.Can([]string{"user:bob"}, "clusters", "read", "default/testing") ||
		e.Can([]string{"user:bob"}, "clusters", "read", "prod/testing") ||
		e.Can([]string{"user:bob"}, "namespaces", "read", "testing") {
		t.Fatal("wildcard-resource legacy grant changed scope")
	}
	stored, err := st.ListPolicyRules(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if stored[len(stored)-2].Values[3] != "testing" ||
		stored[len(stored)-1].Values[3] != "default/testing" {
		t.Fatal("resource-aware normalization was not persisted")
	}
	reloaded, err := New(st)
	if err != nil {
		t.Fatal(err)
	}
	if reloaded.Policy() != e.Policy() ||
		!reloaded.Can([]string{"user:alice"}, "namespaces", "read", "testing") {
		t.Fatal("bare namespace grant changed on reload")
	}
}

func TestReplacementReturnsAndPersistsNormalizedPolicy(t *testing.T) {
	e, st := fixture(t)
	text := "p, user:admin, *, *, *, allow\np, user:alice, clusters, read, orders*\n"
	if err := e.ReplacePolicy(t.Context(), text); err != nil {
		t.Fatal(err)
	}
	want := "p,user:admin,*,*,**,allow\np,user:alice,clusters,read,default/orders*,allow\n"
	if got := e.Policy(); got != want {
		t.Fatalf("policy = %q, want %q", got, want)
	}
	stored, err := st.ListPolicyRules(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if encode(stored) != want {
		t.Fatal("stored policy was not normalized")
	}
	if !e.Can([]string{"user:alice"}, "clusters", "read", "default/orders1") {
		t.Fatal("legacy grant lost in default namespace")
	}
	if e.Can([]string{"user:alice"}, "clusters", "read", "prod/orders1") {
		t.Fatal("legacy grant widened to another namespace")
	}
	if err := e.ReplacePolicy(t.Context(), e.Policy()); err != nil {
		t.Fatal(err)
	}
	if e.Policy() != want {
		t.Fatal("replacement normalization is not idempotent")
	}
}

func legacyPolicyStore(t *testing.T) (*store.Store, *sql.DB, []domain.PolicyRule) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "policy.db")
	st := openStore(t, path)
	legacy := []domain.PolicyRule{
		{PType: "p", Values: []string{"user:admin", "*", "*", "*", "allow"}},
		{PType: "p", Values: []string{"user:alice", "clusters", "read", "orders*", "allow"}},
	}
	if err := st.ReplacePolicyRules(t.Context(), legacy); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return st, db, legacy
}

func TestLoadPersistsNormalizationOnce(t *testing.T) {
	st, db, legacy := legacyPolicyStore(t)
	e, err := New(st)
	if err != nil {
		t.Fatal(err)
	}
	stored, err := st.ListPolicyRules(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if encode(stored) == encode(legacy) || encode(stored) != e.Policy() {
		t.Fatal("normalized policy was not persisted on load")
	}
	if stored[0].Values[3] != "**" || stored[1].Values[3] != "default/orders*" {
		t.Fatal(stored)
	}
	if !e.Can([]string{"user:alice"}, "clusters", "read", "default/orders1") ||
		e.Can([]string{"user:alice"}, "clusters", "read", "prod/orders1") {
		t.Fatal("loaded legacy grant has incorrect namespace scope")
	}
	for _, query := range []string{
		`CREATE TRIGGER no_policy_delete BEFORE DELETE ON policies BEGIN SELECT RAISE(ABORT, 'policy rewritten'); END`,
		`CREATE TRIGGER no_policy_insert BEFORE INSERT ON policies BEGIN SELECT RAISE(ABORT, 'policy rewritten'); END`,
	} {
		if _, err := db.ExecContext(t.Context(), query); err != nil {
			t.Fatal(err)
		}
	}
	reloaded, err := New(st)
	if err != nil {
		t.Fatalf("normalized policy was persisted again: %v", err)
	}
	if reloaded.Policy() != e.Policy() {
		t.Fatal("normalized policy changed on reload")
	}
}

func TestLoadNormalizationPersistenceIsAtomic(t *testing.T) {
	st, db, legacy := legacyPolicyStore(t)
	_, err := db.ExecContext(t.Context(), `CREATE TRIGGER reject_second_policy BEFORE INSERT ON policies
WHEN NEW.position = 1 BEGIN SELECT RAISE(ABORT, 'policy write rejected'); END`)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(st); err == nil {
		t.Fatal("load succeeded despite failed normalization persistence")
	}
	stored, err := st.ListPolicyRules(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if encode(stored) != encode(legacy) {
		t.Fatal("failed normalization partially replaced stored policy")
	}
}

func TestNamespacePolicyExamples(t *testing.T) {
	e, _ := fixture(t)
	policy := DefaultPolicy() +
		"p, user:alice, *, *, dev/**\n" +
		"p, user:alice, *, read, prod/**\n" +
		"p, user:alice, cluster-credentials, read, prod/**, deny\n" +
		"p, user:bob, cluster-credentials, read, prod/orders\n"
	if err := e.ReplacePolicy(t.Context(), policy); err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		subject, resource, action, object string
		want                              bool
	}{
		{"user:alice", "namespaces", "read", "dev", true},
		{"user:alice", "namespaces", "update", "dev", true},
		{"user:alice", "clusters", "delete", "dev/orders", true},
		{"user:alice", "cluster-credentials", "read", "dev/orders", true},
		{"user:alice", "backups", "export", "dev/orders", true},
		{"user:alice", "clusters", "read", "devx/orders", false},
		{"user:alice", "settings", "read", "*", false},
		{"user:alice", "namespaces", "create", "*", false},
		{"user:alice", "namespaces", "read", "prod", true},
		{"user:alice", "namespaces", "delete", "prod", false},
		{"user:alice", "clusters", "read", "prod/orders", true},
		{"user:alice", "clusters", "update", "prod/orders", false},
		{"user:alice", "cluster-credentials", "read", "prod/orders", false},
		{"user:alice", "backups", "export", "prod/orders", false},
		{"user:bob", "cluster-credentials", "read", "prod/orders", true},
		{"user:bob", "cluster-credentials", "read", "prod/other", false},
		{"user:bob", "cluster-credentials", "read", "default/orders", false},
		{"user:bob", "clusters", "read", "prod/orders", false},
	} {
		if got := e.Can([]string{tt.subject}, tt.resource, tt.action, tt.object); got != tt.want {
			t.Errorf("%+v: got %v", tt, got)
		}
	}
}

func TestObjectGlobMatching(t *testing.T) {
	for _, tt := range []struct {
		pattern, object string
		want            bool
	}{
		{"**", "*", true},
		{"**", "ns/cluster", true},
		{"dev/**", "dev", true},
		{"dev/**", "dev/x", true},
		{"dev/**", "dev/x/y", true},
		{"dev/**", "devx/y", false},
		{"dev/**", "*", false},
		{"dev/*", "dev/x/y", false},
	} {
		rules, err := parse("p, user:alice, clusters, read, " + tt.pattern)
		if err != nil {
			t.Fatal(err)
		}
		en, err := build(rules)
		if err != nil {
			t.Fatal(err)
		}
		e := &Enforcer{en: en}
		if got := e.Can([]string{"user:alice"}, "clusters", "read", tt.object); got != tt.want {
			t.Errorf("%q matching %q: got %v, want %v", tt.pattern, tt.object, got, tt.want)
		}
	}
}

func TestObjectPatternValidation(t *testing.T) {
	for _, object := range []string{"**", "*", "dev*", "dev*/orders?", "prod/[ab]*", "dev/**", "**/orders", "dev/**/orders"} {
		if _, err := parse("p, user:alice, clusters, read, " + object); err != nil {
			t.Errorf("rejected valid object %q: %v", object, err)
		}
	}
	e, st := fixture(t)
	before := e.Policy()
	for _, object := range []string{"", "/orders", "dev/", "dev//orders", "dev/**//orders", "dev/orders/member", ".", "..", "dev/..", "dev/.", "dev/[", "dev/\\"} {
		if err := e.ReplacePolicy(t.Context(), DefaultPolicy()+"p, user:alice, clusters, read, "+object+"\n"); err == nil {
			t.Errorf("accepted malformed object %q", object)
		}
	}
	if e.Policy() != before {
		t.Fatal("invalid objects changed live policy")
	}
	stored, err := st.ListPolicyRules(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if encode(stored) != before {
		t.Fatal("invalid objects changed stored policy")
	}
}

func TestNamespaceVocabulary(t *testing.T) {
	for _, action := range []string{"read", "create", "update", "delete"} {
		if _, err := parse("p, user:alice, namespaces, " + action + ", dev/**"); err != nil {
			t.Errorf("namespace action %q: %v", action, err)
		}
	}
	if _, err := parse("p, user:alice, backups, export, prod/orders"); err != nil {
		t.Fatal(err)
	}
	for _, policy := range []string{
		"p, user:alice, namespaces, export, **",
		"p, user:alice, clusters, export, **",
	} {
		if _, err := parse(policy); err == nil {
			t.Fatalf("accepted unknown resource/action: %q", policy)
		}
	}
}

func TestUnrestrictedAdministratorObjects(t *testing.T) {
	for _, object := range []string{"*", "**"} {
		t.Run(object, func(t *testing.T) {
			e, _ := fixture(t)
			if err := e.ReplacePolicy(t.Context(), "p, user:admin, *, *, "+object); err != nil {
				t.Fatal(err)
			}
			if !e.Can([]string{"user:admin"}, "settings", "update", "*") ||
				!e.Can([]string{"user:admin"}, "clusters", "delete", "prod/orders") {
				t.Fatal("unrestricted administrator lost global or namespaced access")
			}
		})
	}
	e, _ := fixture(t)
	before := e.Policy()
	for _, object := range []string{"default/**", "prod/**", "**/*"} {
		if err := e.ReplacePolicy(t.Context(), "p, user:admin, *, *, "+object); err == nil {
			t.Errorf("scoped administrator %q passed lockout guard", object)
		}
	}
	if e.Policy() != before {
		t.Fatal("lockout changed policy")
	}
	if err := e.ReplacePolicy(t.Context(), "p, user:admin, *, *, **\np, user:admin, cluster-credentials, read, prod/**, deny"); err == nil {
		t.Fatal("namespace-specific deny passed unrestricted-admin guard")
	}
}
