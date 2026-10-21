// SPDX-License-Identifier: Apache-2.0
// Copyright 2026 The Karakoram Authors

package auth

import (
	"github.com/KarakoramDev/Karakoram/internal/domain"
	"reflect"
	"testing"
)

func TestRestrictionAllows(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		r                        *domain.TokenRestrictions
		resource, action, object string
		want                     bool
	}{
		{"legacy", nil, "cluster-credentials", "read", "*", true},
		{"credentials default", &domain.TokenRestrictions{}, "cluster-credentials", "read", "default/db", false},
		{"read only", &domain.TokenRestrictions{ReadOnly: true}, "clusters", "update", "default/db", false},
		{"namespace allowed", &domain.TokenRestrictions{Namespaces: []string{"team"}}, "clusters", "read", "team/db", true},
		{"namespace denied", &domain.TokenRestrictions{Namespaces: []string{"team"}}, "clusters", "read", "other/db", false},
		{"global denied", &domain.TokenRestrictions{Namespaces: []string{"team"}}, "settings", "read", "*", false},
		{"cluster namespace", &domain.TokenRestrictions{Clusters: []string{"team/db"}}, "namespaces", "read", "team", true},
		{"cluster denied", &domain.TokenRestrictions{Clusters: []string{"team/db"}}, "clusters", "read", "team/other", false},
		{"cluster exact", &domain.TokenRestrictions{Clusters: []string{"team/db"}}, "clusters", "read", "team/db", true},
		{"different storage name", &domain.TokenRestrictions{Clusters: []string{"team/db"}}, "backup-storages", "read", "team/archive", true},
		{"same storage name", &domain.TokenRestrictions{Clusters: []string{"team/db"}}, "backup-storages", "read", "team/db", true},
		{"storage different namespace", &domain.TokenRestrictions{Clusters: []string{"team/db"}}, "backup-storages", "read", "other/db", false},
		{"combined namespace rejects storage", &domain.TokenRestrictions{Namespaces: []string{"other"}, Clusters: []string{"team/db"}}, "backup-storages", "read", "team/archive", false},
		{"combined cluster namespace rejects storage", &domain.TokenRestrictions{Namespaces: []string{"other"}, Clusters: []string{"team/db"}}, "backup-storages", "read", "other/archive", false},
		{"events exact cluster", &domain.TokenRestrictions{Clusters: []string{"team/db"}}, "events", "read", "team/other", false},
		{"data exact cluster", &domain.TokenRestrictions{Clusters: []string{"team/db"}}, "cluster-data", "read", "team/other", false},
		{"permission wildcard", &domain.TokenRestrictions{Permissions: []string{"clusters:*"}}, "clusters", "update", "team/db", true},
		{"permission denied", &domain.TokenRestrictions{Permissions: []string{"clusters:read"}}, "events", "read", "team/db", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := RestrictionAllows(tc.r, tc.resource, tc.action, tc.object); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
	r := &domain.TokenRestrictions{Namespaces: []string{"team"}}
	if !RestrictionAllowsOperation(r, "clusters", "read", "*", true, false) || RestrictionAllowsOperation(r, "clusters", "read", "*", true, true) {
		t.Fatal("collection or secret gate")
	}
}

func TestRestrictionsSubset(t *testing.T) {
	parent := &domain.TokenRestrictions{ReadOnly: true, Namespaces: []string{"team"}, Permissions: []string{"clusters:*"}}
	for _, tc := range []struct {
		name  string
		child *domain.TokenRestrictions
		want  bool
	}{
		{"unrestricted", nil, false},
		{"same", parent, true},
		{"cluster narrowing", &domain.TokenRestrictions{ReadOnly: true, Clusters: []string{"team/db"}, Permissions: []string{"clusters:read"}}, true},
		{"other namespace", &domain.TokenRestrictions{ReadOnly: true, Clusters: []string{"other/db"}, Permissions: []string{"clusters:read"}}, false},
		{"write", &domain.TokenRestrictions{Namespaces: []string{"team"}, Permissions: []string{"clusters:read"}}, false},
		{"reveal", &domain.TokenRestrictions{ReadOnly: true, Namespaces: []string{"team"}, Permissions: []string{"clusters:read"}, AllowCredentialReveal: true}, false},
		{"permissions broadened", &domain.TokenRestrictions{ReadOnly: true, Namespaces: []string{"team"}}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := RestrictionsSubset(tc.child, parent); got != tc.want {
				t.Fatalf("got %v want %v", got, tc.want)
			}
		})
	}
	if RestrictionsSubset(&domain.TokenRestrictions{Namespaces: []string{"team"}}, &domain.TokenRestrictions{Clusters: []string{"team/db"}}) {
		t.Fatal("cluster scope widened")
	}
	if !RestrictionsSubset(nil, nil) {
		t.Fatal("unrestricted parent")
	}
}

func TestNormalizeRestrictions(t *testing.T) {
	source := &domain.TokenRestrictions{Namespaces: []string{" team ", "team", "alpha"}, Permissions: []string{"clusters:read"}}
	got, err := NormalizeRestrictions(source)
	if err != nil || !reflect.DeepEqual(got.Namespaces, []string{"alpha", "team"}) || source.Namespaces[0] != " team " {
		t.Fatal(got, err, source)
	}
	for _, r := range []*domain.TokenRestrictions{
		{Namespaces: []string{""}}, {Clusters: []string{"team/no/slash"}}, {Permissions: []string{"unknown:read"}}, {Permissions: []string{"clusters:unknown"}}, {Permissions: []string{"cluster-data:update"}},
	} {
		if _, err := NormalizeRestrictions(r); err == nil {
			t.Fatalf("accepted %+v", r)
		}
	}
	// Permissions outside OpenAPI routes come from the RBAC vocabulary.
	if _, err := NormalizeRestrictions(&domain.TokenRestrictions{Permissions: []string{"backups:export", "cluster-data:read", "cluster-data:write", "cluster-data:*"}}); err != nil {
		t.Fatal(err)
	}
	clone := CloneRestrictions(got)
	clone.Namespaces[0] = "changed"
	if got.Namespaces[0] == "changed" {
		t.Fatal("snapshot aliases")
	}
}
